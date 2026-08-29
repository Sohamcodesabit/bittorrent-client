// orchestrate.rs
//
// The layer that ties everything else together into runnable commands:
// generate a .torrent, seed a file, or leech from a manually-specified list
// of peers -- splitting pieces across them and downloading concurrently.

use crate::network::connect_and_handshake;
use crate::peer::{Handshake, PEER_ID_LEN};
use crate::seed::{run_seeder, SeedConfig};
use crate::torrent::{self, Torrent};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;

/// Generates a peer_id: the conventional Azureus-style prefix ("-XX0001-")
/// followed by bytes derived from the current time, so two instances
/// started at different moments won't collide. Not cryptographically
/// random -- doesn't need to be, peer_id just needs to be distinguishable.
pub fn generate_peer_id() -> [u8; PEER_ID_LEN] {
    let mut id = [0u8; PEER_ID_LEN];
    id[..8].copy_from_slice(b"-RS0001-");

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let bytes = nanos.to_be_bytes();
    for (i, slot) in id[8..].iter_mut().enumerate() {
        *slot = bytes[i % bytes.len()];
    }
    id
}

/// Computes the byte length of piece `index` -- every piece is
/// `piece_length` bytes except possibly the last, which absorbs whatever
/// remainder is left over from `total_length`.
fn piece_len_for(index: u32, num_pieces: u32, piece_length: u32, total_length: i64) -> u32 {
    if index == num_pieces - 1 {
        let full_pieces_bytes = piece_length as i64 * (num_pieces as i64 - 1);
        (total_length - full_pieces_bytes) as u32
    } else {
        piece_length
    }
}

/// Creates a .torrent file for `file_path` and writes it next to the source
/// file (as `<file_path>.torrent`), printing the info_hash so it's easy to
/// confirm two machines are talking about the same content.
pub fn make_torrent(
    file_path: &std::path::Path,
    announce: &str,
    piece_length: i64,
    out_path: Option<PathBuf>,
) -> std::io::Result<PathBuf> {
    let bytes = torrent::create_single_file_torrent(file_path, announce, piece_length)?;
    let out = out_path.unwrap_or_else(|| {
        let mut p = file_path.as_os_str().to_owned();
        p.push(".torrent");
        PathBuf::from(p)
    });
    std::fs::write(&out, &bytes)?;

    let parsed = torrent::parse(&bytes).expect("we just generated this, it must parse");
    println!("Created {}", out.display());
    println!("  name:       {}", parsed.name);
    println!("  size:       {} bytes", parsed.total_length());
    println!("  pieces:     {}", parsed.pieces.len());
    println!("  info_hash:  {}", parsed.info_hash_hex());

    Ok(out)
}

/// Runs as a seeder: binds `port` and serves `file_path`'s content forever,
/// according to the piece layout described by `torrent`.
pub async fn run_seed(torrent: &Torrent, file_path: PathBuf, port: u16) -> std::io::Result<()> {
    let bind_addr: SocketAddr = ([0, 0, 0, 0], port).into();
    let listener = TcpListener::bind(bind_addr).await?;
    println!("Seeding '{}' on port {}", torrent.name, port);
    println!("  info_hash: {}", torrent.info_hash_hex());
    println!("  pieces:    {}", torrent.pieces.len());

    let config = SeedConfig {
        file_path,
        piece_length: torrent.piece_length as u32,
        num_pieces: torrent.pieces.len() as u32,
        info_hash: torrent.info_hash,
    };

    run_seeder(listener, generate_peer_id(), config).await
}

/// Max times a single piece will be re-queued after a failure before we
/// give up on it entirely. Prevents one permanently-bad piece (e.g. every
/// peer keeps dropping on it) from looping forever.
const MAX_PIECE_ATTEMPTS: u32 = 4;

/// Runs as a leecher: connects to every address in `peers` and downloads
/// all of the torrent's pieces from them concurrently (one Tokio task per
/// peer), assembling the result into `output_path`.
///
/// Pieces are handed out from one shared work queue rather than being
/// statically assigned up front: each peer task repeatedly pulls the next
/// pending piece, downloads it, and loops. If a peer's connection dies
/// partway through, the piece it was working on goes back on the queue for
/// a different (still-healthy) peer to pick up -- this is what gives us
/// retry-via-a-different-peer, and as a side effect it also naturally load
/// balances: a fast peer just ends up pulling more pieces than a slow one.
pub async fn run_leech(
    torrent: &Torrent,
    output_path: PathBuf,
    peers: Vec<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    if peers.is_empty() {
        return Err("no peers given -- pass at least one with --peers".into());
    }

    let num_pieces = torrent.pieces.len() as u32;
    let piece_length = torrent.piece_length as u32;
    let total_length = torrent.total_length();
    let info_hash = torrent.info_hash;
    let output_path = Arc::new(output_path);

    // Shared work queue: (piece_index, piece_byte_length, expected_hash, attempts_so_far).
    // std::sync::Mutex is fine here (not tokio::sync::Mutex) because we
    // only ever hold the lock for a quick push/pop, never across an .await.
    let queue: Arc<std::sync::Mutex<std::collections::VecDeque<(u32, u32, [u8; 20], u32)>>> = {
        let mut q = std::collections::VecDeque::with_capacity(num_pieces as usize);
        for piece_index in 0..num_pieces {
            let len = piece_len_for(piece_index, num_pieces, piece_length, total_length);
            let hash = torrent.pieces[piece_index as usize];
            q.push_back((piece_index, len, hash, 0));
        }
        Arc::new(std::sync::Mutex::new(q))
    };

    let completed = Arc::new(AtomicUsize::new(0));
    let permanently_failed = Arc::new(AtomicUsize::new(0));
    let started_at = Instant::now();
    let our_peer_id = generate_peer_id();

    println!(
        "Leeching '{}' ({} bytes, {} pieces) from {} peer(s)",
        torrent.name,
        total_length,
        num_pieces,
        peers.len()
    );

    let mut tasks = Vec::new();
    for peer_addr in peers {
        let output_path = Arc::clone(&output_path);
        let completed = Arc::clone(&completed);
        let permanently_failed = Arc::clone(&permanently_failed);
        let queue = Arc::clone(&queue);

        tasks.push(tokio::spawn(async move {
            // A connection failure here just means this peer never
            // contributes anything -- not fatal to the overall download,
            // since every piece it would have taken is still in the queue
            // for another peer.
            let handshake = Handshake::new(info_hash, our_peer_id);
            let (mut stream, _their_hs) = match connect_and_handshake(peer_addr, &handshake).await {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("  peer {peer_addr}: connection failed ({e}), skipping");
                    return;
                }
            };
            if let Err(e) = crate::download::wait_for_unchoke(&mut stream).await {
                eprintln!("  peer {peer_addr}: never unchoked us ({e}), skipping");
                return;
            }

            loop {
                let next = { queue.lock().unwrap().pop_front() };
                let Some((piece_index, len, hash, attempts)) = next else {
                    break; // queue empty -- this peer's work is done
                };

                match crate::download::download_piece_data(&mut stream, piece_index, len, hash).await
                {
                    Ok(data) => {
                        if let Err(e) =
                            crate::download::write_piece_to_file(&output_path, piece_index, piece_length, &data)
                                .await
                        {
                            eprintln!("  piece {piece_index}: write failed ({e}), re-queueing");
                            requeue_or_drop(&queue, (piece_index, len, hash, attempts), &permanently_failed);
                            continue;
                        }
                        let done = completed.fetch_add(1, Ordering::SeqCst) + 1;
                        println!(
                            "  [{done}/{num_pieces}] piece {piece_index} from {peer_addr} ({:.1}%)",
                            100.0 * done as f64 / num_pieces as f64
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "  peer {peer_addr}: failed on piece {piece_index} ({e}), re-queueing and dropping this connection"
                        );
                        requeue_or_drop(&queue, (piece_index, len, hash, attempts), &permanently_failed);
                        // This connection is presumably in a bad state (I/O
                        // error, or the peer sent something we couldn't
                        // parse) -- stop using it rather than risk looping
                        // on a broken stream. Other peers pick up the slack.
                        break;
                    }
                }
            }
        }));
    }

    for task in tasks {
        task.await?; // propagate a panic, if any; per-peer errors are already handled above
    }

    let elapsed = started_at.elapsed();
    let done = completed.load(Ordering::SeqCst);
    let failed = permanently_failed.load(Ordering::SeqCst);

    if done as u32 == num_pieces {
        let speed_mb_s = (total_length as f64 / 1_000_000.0) / elapsed.as_secs_f64().max(0.001);
        println!(
            "Done: {} pieces in {:.2}s ({:.2} MB/s) -> {}",
            done,
            elapsed.as_secs_f64(),
            speed_mb_s,
            output_path.display()
        );
        Ok(())
    } else {
        Err(format!(
            "only {done}/{num_pieces} pieces downloaded successfully ({failed} gave up after {MAX_PIECE_ATTEMPTS} attempts, the rest had no peer left to try)"
        )
        .into())
    }
}

/// Puts a failed piece back on the queue for another peer to try, unless
/// it's already been attempted MAX_PIECE_ATTEMPTS times, in which case we
/// give up on it and count it as a permanent failure.
fn requeue_or_drop(
    queue: &std::sync::Mutex<std::collections::VecDeque<(u32, u32, [u8; 20], u32)>>,
    (piece_index, len, hash, attempts): (u32, u32, [u8; 20], u32),
    permanently_failed: &AtomicUsize,
) {
    let attempts = attempts + 1;
    if attempts >= MAX_PIECE_ATTEMPTS {
        eprintln!("  piece {piece_index}: giving up after {attempts} attempts");
        permanently_failed.fetch_add(1, Ordering::SeqCst);
    } else {
        queue.lock().unwrap().push_back((piece_index, len, hash, attempts));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{receive_handshake, receive_message, send_handshake, send_message};
    use crate::peer::PeerMessage;
    use crate::seed::run_seeder;

    /// Accepts exactly one connection, completes the handshake and
    /// unchoke ritual normally, then abandons the connection the moment
    /// it receives its first block Request -- simulating a peer that
    /// drops mid-transfer. Used to prove the shared work queue lets a
    /// healthy peer pick up the slack.
    fn spawn_unreliable_seeder(listener: TcpListener, info_hash: [u8; 20]) {
        tokio::spawn(async move {
            let (mut socket, _addr) = listener.accept().await.unwrap();
            let their_hs = receive_handshake(&mut socket).await.unwrap();
            assert_eq!(their_hs.info_hash, info_hash);

            let our_hs = Handshake::new(info_hash, generate_peer_id());
            send_handshake(&mut socket, &our_hs).await.unwrap();

            loop {
                match receive_message(&mut socket).await.unwrap() {
                    PeerMessage::Interested => break,
                    _ => continue,
                }
            }
            send_message(&mut socket, &PeerMessage::Unchoke).await.unwrap();

            // Consume exactly one Request, then just... vanish. The socket
            // closes when this task returns and `socket` is dropped.
            let _ = receive_message(&mut socket).await;
        });
    }

    /// End-to-end swarm test: three local "seeders" each holding a full
    /// copy of the same file, one leech run pulling pieces round-robin
    /// across all three concurrently, and a byte-for-byte comparison of
    /// the reassembled output against the original -- this is the same
    /// shape as the real multi-machine demo, just with all three seeders
    /// running as local tasks instead of on separate physical machines.
    #[tokio::test]
    async fn leeches_a_file_correctly_from_three_concurrent_peers() {
        let dir = std::env::temp_dir();
        let source_path = dir.join("bt_test_orchestrate_source.bin");
        let output_path = dir.join("bt_test_orchestrate_output.bin");
        let torrent_path = dir.join("bt_test_orchestrate.torrent");

        // ~1.5 MiB so we get a double-digit number of pieces to spread
        // across three peers meaningfully.
        let piece_length: i64 = 65_536;
        let mut content = Vec::new();
        for i in 0..(piece_length * 24) {
            content.push((i % 256) as u8);
        }
        std::fs::write(&source_path, &content).unwrap();

        let torrent_bytes =
            torrent::create_single_file_torrent(&source_path, "http://test.invalid", piece_length)
                .unwrap();
        std::fs::write(&torrent_path, &torrent_bytes).unwrap();
        let parsed_torrent = torrent::parse(&torrent_bytes).unwrap();

        // Spin up three seeders, all serving the identical source file.
        let mut peer_addrs = Vec::new();
        for _ in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            peer_addrs.push(addr);

            let config = SeedConfig {
                file_path: source_path.clone(),
                piece_length: parsed_torrent.piece_length as u32,
                num_pieces: parsed_torrent.pieces.len() as u32,
                info_hash: parsed_torrent.info_hash,
            };
            tokio::spawn(async move {
                run_seeder(listener, generate_peer_id(), config).await.ok();
            });
        }

        run_leech(&parsed_torrent, output_path.clone(), peer_addrs).await.unwrap();

        let downloaded = std::fs::read(&output_path).unwrap();
        assert_eq!(downloaded.len(), content.len());
        assert_eq!(downloaded, content, "reassembled file must exactly match the original");

        std::fs::remove_file(&source_path).ok();
        std::fs::remove_file(&output_path).ok();
        std::fs::remove_file(&torrent_path).ok();
    }

    #[test]
    fn piece_len_for_handles_uneven_final_piece() {
        // 1000 bytes, piece_length 300 -> pieces of 300, 300, 300, 100.
        let num_pieces = 4;
        let piece_length = 300;
        let total = 1000;
        assert_eq!(piece_len_for(0, num_pieces, piece_length, total), 300);
        assert_eq!(piece_len_for(2, num_pieces, piece_length, total), 300);
        assert_eq!(piece_len_for(3, num_pieces, piece_length, total), 100);
    }

    #[tokio::test]
    async fn retries_pieces_from_an_unreliable_peer_via_a_healthy_one() {
        let dir = std::env::temp_dir();
        let source_path = dir.join("bt_test_retry_source.bin");
        let output_path = dir.join("bt_test_retry_output.bin");

        // Enough pieces that the unreliable peer's one contribution attempt
        // (and subsequent requeue) is a small fraction of the total work.
        let piece_length: i64 = 4096;
        let num_pieces = 10;
        let mut content = Vec::new();
        for i in 0..(piece_length * num_pieces) {
            content.push((i % 256) as u8);
        }
        std::fs::write(&source_path, &content).unwrap();

        let torrent_bytes =
            torrent::create_single_file_torrent(&source_path, "http://test.invalid", piece_length)
                .unwrap();
        let parsed_torrent = torrent::parse(&torrent_bytes).unwrap();

        // One healthy seeder with the full file...
        let healthy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_addr = healthy_listener.local_addr().unwrap();
        let healthy_config = SeedConfig {
            file_path: source_path.clone(),
            piece_length: parsed_torrent.piece_length as u32,
            num_pieces: parsed_torrent.pieces.len() as u32,
            info_hash: parsed_torrent.info_hash,
        };
        tokio::spawn(async move {
            run_seeder(healthy_listener, generate_peer_id(), healthy_config).await.ok();
        });

        // ...and one unreliable peer that drops after its first request.
        let unreliable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unreliable_addr = unreliable_listener.local_addr().unwrap();
        spawn_unreliable_seeder(unreliable_listener, parsed_torrent.info_hash);

        let result = run_leech(
            &parsed_torrent,
            output_path.clone(),
            vec![unreliable_addr, healthy_addr],
        )
        .await;

        assert!(result.is_ok(), "download should still succeed despite one unreliable peer: {result:?}");

        let downloaded = std::fs::read(&output_path).unwrap();
        assert_eq!(downloaded, content, "every piece must still end up correct, whichever peer served it");

        std::fs::remove_file(&source_path).ok();
        std::fs::remove_file(&output_path).ok();
    }
}
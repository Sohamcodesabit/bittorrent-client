// orchestrate.rs
//
// The layer that ties everything else together into runnable commands:
// generate a .torrent, seed a file, or leech from a manually-specified list
// of peers -- splitting pieces across them and downloading concurrently.

use crate::download::download_pieces;
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

/// Runs as a leecher: connects to every address in `peers`, splits all of
/// the torrent's pieces round-robin across them, downloads concurrently
/// (one Tokio task per peer), and assembles the result into `output_path`.
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

    // Round-robin assignment: piece i goes to peers[i % peers.len()].
    let mut assignments_per_peer: Vec<Vec<(u32, u32, [u8; 20])>> = vec![Vec::new(); peers.len()];
    for piece_index in 0..num_pieces {
        let len = piece_len_for(piece_index, num_pieces, piece_length, total_length);
        let hash = torrent.pieces[piece_index as usize];
        let peer_slot = (piece_index as usize) % peers.len();
        assignments_per_peer[peer_slot].push((piece_index, len, hash));
    }

    let completed = Arc::new(AtomicUsize::new(0));
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
    for (peer_addr, assignments) in peers.into_iter().zip(assignments_per_peer.into_iter()) {
        if assignments.is_empty() {
            continue; // more peers than pieces -- nothing assigned to this one
        }
        let output_path = Arc::clone(&output_path);
        let completed = Arc::clone(&completed);

        tasks.push(tokio::spawn(async move {
            let handshake = Handshake::new(info_hash, our_peer_id);
            let (mut stream, _their_hs) = connect_and_handshake(peer_addr, &handshake).await?;

            let downloaded = download_pieces(&mut stream, &assignments).await?;

            for (piece_index, data) in downloaded {
                crate::download::write_piece_to_file(&output_path, piece_index, piece_length, &data)
                    .await?;
                let done = completed.fetch_add(1, Ordering::SeqCst) + 1;
                println!(
                    "  [{done}/{num_pieces}] piece {piece_index} from {peer_addr} ({:.1}%)",
                    100.0 * done as f64 / num_pieces as f64
                );
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }));
    }

    // Wait for every peer's task to finish, surfacing the first error (if
    // any) but letting all tasks run to completion rather than aborting
    // the others the moment one fails.
    let mut first_error = None;
    for task in tasks {
        if let Err(e) = task.await? {
            eprintln!("peer task failed: {e}");
            if first_error.is_none() {
                first_error = Some(e);
            }
        }
    }

    let elapsed = started_at.elapsed();
    let done = completed.load(Ordering::SeqCst);
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
        Err(format!("only {done}/{num_pieces} pieces downloaded successfully").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seed::run_seeder;

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
}
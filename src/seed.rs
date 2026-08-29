// seed.rs
//
// The seed path: listen for incoming peer connections, hand them a
// Bitfield announcing we have the whole file, then serve whatever blocks
// they Request by reading straight off disk. One Tokio task is spawned per
// incoming connection, so we can seed to many peers concurrently.

use crate::network::{receive_handshake, receive_message, send_handshake, send_message};
use crate::peer::{Handshake, PeerMessage, PEER_ID_LEN};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
pub enum SeedError {
    Io(std::io::Error),
    InfoHashMismatch,
    HandshakeFailed,
}

impl std::fmt::Display for SeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeedError::Io(e) => write!(f, "I/O error: {e}"),
            SeedError::InfoHashMismatch => {
                write!(f, "peer requested a torrent we're not seeding (info_hash mismatch)")
            }
            SeedError::HandshakeFailed => write!(f, "handshake failed"),
        }
    }
}
impl std::error::Error for SeedError {}

/// Everything a seeding session needs to know about the torrent it's
/// serving. Wrapped in Arc by run_seeder so every spawned per-connection
/// task can share one copy instead of cloning the data itself.
pub struct SeedConfig {
    pub file_path: PathBuf,
    pub piece_length: u32,
    pub num_pieces: u32,
    pub info_hash: [u8; 20],
}

/// Builds a Bitfield payload announcing every piece present: one bit per
/// piece, most-significant-bit first within each byte, per the spec.
/// Trailing padding bits (when num_pieces isn't a multiple of 8) are left
/// as 0, since they don't correspond to a real piece.
pub fn build_full_bitfield(num_pieces: u32) -> Vec<u8> {
    let num_bytes = (num_pieces as usize).div_ceil(8);
    let mut bitfield = vec![0u8; num_bytes];

    for piece_index in 0..num_pieces {
        let byte_index = (piece_index / 8) as usize;
        let bit_offset = 7 - (piece_index % 8); // MSB-first within the byte
        bitfield[byte_index] |= 1 << bit_offset;
    }

    bitfield
}

/// Reads exactly `length` bytes for piece `piece_index` starting at
/// in-piece offset `begin`, straight off disk.
async fn read_block_from_file(
    file_path: &std::path::Path,
    piece_index: u32,
    piece_length: u32,
    begin: u32,
    length: u32,
) -> std::io::Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(file_path).await?;
    let offset = piece_index as u64 * piece_length as u64 + begin as u64;
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let mut buf = vec![0u8; length as usize];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Handles one incoming peer connection end to end: handshake, bitfield,
/// then serve Request messages until the peer disconnects.
async fn handle_incoming_connection(
    mut stream: TcpStream,
    our_peer_id: [u8; PEER_ID_LEN],
    config: Arc<SeedConfig>,
) -> Result<(), SeedError> {
    let their_handshake = receive_handshake(&mut stream).await.map_err(|_| SeedError::HandshakeFailed)?;

    if their_handshake.info_hash != config.info_hash {
        return Err(SeedError::InfoHashMismatch);
    }

    let our_handshake = Handshake::new(config.info_hash, our_peer_id);
    send_handshake(&mut stream, &our_handshake).await.map_err(SeedError::Io)?;

    let bitfield = build_full_bitfield(config.num_pieces);
    send_message(&mut stream, &PeerMessage::Bitfield(bitfield)).await.map_err(SeedError::Io)?;

    let mut peer_is_interested = false;

    loop {
        let msg = match receive_message(&mut stream).await {
            Ok(m) => m,
            // The peer closing the connection (or any I/O error) just ends
            // this session cleanly -- not a crash, just "they're done."
            Err(_) => break,
        };

        match msg {
            PeerMessage::Interested => {
                peer_is_interested = true;
                send_message(&mut stream, &PeerMessage::Unchoke).await.map_err(SeedError::Io)?;
            }
            PeerMessage::NotInterested => {
                peer_is_interested = false;
            }
            PeerMessage::Request { index, begin, length } => {
                if !peer_is_interested {
                    // Well-behaved peers only request after Interested +
                    // our Unchoke, but we don't trust that blindly -- serve
                    // it anyway here since refusing costs us nothing extra
                    // and staying strict just adds complexity for a portfolio
                    // project. (A stricter client could ignore/choke here.)
                }
                match read_block_from_file(&config.file_path, index, config.piece_length, begin, length)
                    .await
                {
                    Ok(block) => {
                        let msg = PeerMessage::Piece { index, begin, block };
                        send_message(&mut stream, &msg).await.map_err(SeedError::Io)?;
                    }
                    Err(e) => return Err(SeedError::Io(e)),
                }
            }
            PeerMessage::KeepAlive | PeerMessage::Have { .. } | PeerMessage::Cancel { .. } => {
                // Nothing to do: keep-alives need no response, and we don't
                // track cancellations since we serve requests synchronously
                // and quickly -- there's no meaningful "in-flight" request
                // to cancel in this simple implementation.
            }
            PeerMessage::Choke | PeerMessage::Unchoke | PeerMessage::Piece { .. } => {
                // These are messages a leecher sends to a seeder in
                // situations we don't implement yet (e.g. them upload-choking
                // us back). Safe to ignore for a receive-then-serve seeder.
            }
            PeerMessage::Bitfield(_) => {
                // A leecher announcing what it already has. Irrelevant to a
                // pure seeder (we don't request anything from them), so we
                // don't track it -- but it's a legal message to receive here.
            }
        }
    }

    Ok(())
}

/// Seeds `config`'s file to any peer that connects to `listener`, forever,
/// spawning one task per connection so multiple peers can download from us
/// concurrently. The listener is passed in already-bound (rather than an
/// address to bind here) so callers -- including tests -- can inspect the
/// actual bound address first, e.g. when binding to port 0.
pub async fn run_seeder(
    listener: TcpListener,
    our_peer_id: [u8; PEER_ID_LEN],
    config: SeedConfig,
) -> std::io::Result<()> {
    let config = Arc::new(config);

    loop {
        let (stream, _peer_addr) = listener.accept().await?;
        let config = Arc::clone(&config);

        // Spawning means a slow or stalled peer only blocks its own task,
        // never the accept loop or any other peer's transfer.
        tokio::spawn(async move {
            if let Err(e) = handle_incoming_connection(stream, our_peer_id, config).await {
                eprintln!("seed session ended: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::download_piece;
    use crate::network::connect_and_handshake;
    use sha1::{Digest, Sha1};

    #[test]
    fn full_bitfield_sets_exactly_the_real_pieces() {
        // 10 pieces -> ceil(10/8) = 2 bytes. First byte all 1s (pieces 0-7).
        // Second byte: pieces 8-9 set (top two bits), rest padding left as 0.
        let bf = build_full_bitfield(10);
        assert_eq!(bf.len(), 2);
        assert_eq!(bf[0], 0b1111_1111);
        assert_eq!(bf[1], 0b1100_0000);
    }

    #[test]
    fn full_bitfield_handles_exact_byte_multiple() {
        // 16 pieces -> exactly 2 bytes, both fully set, no padding bits at all.
        let bf = build_full_bitfield(16);
        assert_eq!(bf, vec![0xFF, 0xFF]);
    }

    #[test]
    fn full_bitfield_handles_fewer_than_eight_pieces() {
        // 3 pieces -> 1 byte, top 3 bits set: 1110_0000.
        let bf = build_full_bitfield(3);
        assert_eq!(bf, vec![0b1110_0000]);
    }

    #[tokio::test]
    async fn seeds_a_real_file_to_our_own_leech_code() {
        // This is the interop test that matters most: our seed path and our
        // leech path, talking to each other over a real socket, downloading
        // a real piece from a real file on disk -- exactly the roles two
        // machines will play in the actual demo.
        let dir = std::env::temp_dir();
        let file_path = dir.join("bt_test_seed_source.bin");

        let piece_length: u32 = 1024;
        let num_pieces = 3;
        // Distinguishable content per piece so a wrong-offset bug would be
        // caught (all-same-byte data can't reveal an off-by-one).
        let mut file_content = Vec::new();
        for piece_idx in 0..num_pieces {
            for i in 0..piece_length {
                file_content.push(((piece_idx * 37 + i) % 256) as u8);
            }
        }
        std::fs::write(&file_path, &file_content).unwrap();

        let info_hash = [9u8; 20];
        let config = SeedConfig {
            file_path: file_path.clone(),
            piece_length,
            num_pieces,
            info_hash,
        };
        let seeder_peer_id = [0xEEu8; PEER_ID_LEN];

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            run_seeder(listener, seeder_peer_id, config).await.ok();
        });

        // Now act as a leecher: connect, handshake, download piece 1 (the
        // *middle* piece specifically, to prove offset math is right, not
        // just piece 0 which would pass even with an offset bug).
        let leecher_hs = Handshake::new(info_hash, [0xAAu8; PEER_ID_LEN]);
        let (mut stream, their_hs) = connect_and_handshake(addr, &leecher_hs).await.unwrap();
        assert_eq!(their_hs.info_hash, info_hash);

        let piece_index = 1;
        let expected_piece_data =
            &file_content[(piece_index * piece_length) as usize..((piece_index + 1) * piece_length) as usize];
        let expected_hash: [u8; 20] = Sha1::digest(expected_piece_data).into();

        let downloaded = download_piece(&mut stream, piece_index, piece_length, expected_hash)
            .await
            .expect("should download and verify piece 1 from the seeder");

        assert_eq!(downloaded, expected_piece_data);

        std::fs::remove_file(&file_path).ok();
    }

    #[tokio::test]
    async fn rejects_a_leecher_asking_for_a_different_torrent() {
        let dir = std::env::temp_dir();
        let file_path = dir.join("bt_test_seed_wrong_hash.bin");
        std::fs::write(&file_path, vec![0u8; 1024]).unwrap();

        let seeder_info_hash = [1u8; 20];
        let config = SeedConfig {
            file_path: file_path.clone(),
            piece_length: 1024,
            num_pieces: 1,
            info_hash: seeder_info_hash,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            run_seeder(listener, [0xEEu8; PEER_ID_LEN], config).await.ok();
        });

        // Leecher asks about a *different* torrent than what's being seeded.
        let wrong_info_hash = [2u8; 20];
        let leecher_hs = Handshake::new(wrong_info_hash, [0xAAu8; PEER_ID_LEN]);
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        send_handshake(&mut stream, &leecher_hs).await.unwrap();

        // The seeder should close the connection rather than reply -- our
        // next read should hit EOF/error rather than getting a handshake back.
        let result = receive_handshake(&mut stream).await;
        assert!(result.is_err(), "seeder must not hand out data for a torrent it isn't seeding");

        std::fs::remove_file(&file_path).ok();
    }
}
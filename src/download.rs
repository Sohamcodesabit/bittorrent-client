// download.rs
//
// The leech path: given an already-handshaken connection to a peer, walk
// through the interested/unchoke ritual, request a piece's blocks, verify
// the assembled piece against its expected SHA1 hash, and write it to disk
// at the correct byte offset.

use crate::network::{receive_message, send_message};
use crate::peer::PeerMessage;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Standard BitTorrent block size: pieces are downloaded in 16 KiB chunks,
/// not all at once. This value is a long-standing convention most clients
/// enforce (and many will refuse requests for anything larger).
pub const BLOCK_SIZE: u32 = 16 * 1024;

#[derive(Debug)]
pub enum DownloadError {
    Io(std::io::Error),
    UnexpectedMessage(&'static str),
    HashMismatch,
    ConnectionClosed,
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadError::Io(e) => write!(f, "I/O error: {e}"),
            DownloadError::UnexpectedMessage(ctx) => write!(f, "unexpected message: {ctx}"),
            DownloadError::HashMismatch => write!(f, "downloaded piece failed hash verification"),
            DownloadError::ConnectionClosed => write!(f, "peer closed the connection"),
        }
    }
}
impl std::error::Error for DownloadError {}

/// Splits a piece of `piece_length` bytes into (begin, length) pairs for
/// individual block requests. The final block may be shorter than
/// BLOCK_SIZE if piece_length isn't an exact multiple of it.
pub fn split_into_blocks(piece_length: u32) -> Vec<(u32, u32)> {
    let mut blocks = Vec::new();
    let mut begin = 0;
    while begin < piece_length {
        let length = BLOCK_SIZE.min(piece_length - begin);
        blocks.push((begin, length));
        begin += length;
    }
    blocks
}

/// Sends Interested and blocks (asynchronously) until the peer sends
/// Unchoke, discarding/ignoring other message types in between (a real
/// peer may send Bitfield, Have, or KeepAlive messages first).
///
/// Note: this is deliberately simple for a first pass -- it silently drops
/// any Bitfield/Have info rather than tracking peer piece availability.
/// We'll want to change that once we're choosing pieces from many peers.
async fn wait_for_unchoke(stream: &mut TcpStream) -> Result<(), DownloadError> {
    send_message(stream, &PeerMessage::Interested).await.map_err(DownloadError::Io)?;

    loop {
        let msg = receive_message(stream).await.map_err(|e| match e {
            crate::network::MessageIoError::Io(e) => DownloadError::Io(e),
            crate::network::MessageIoError::Protocol(_) => {
                DownloadError::UnexpectedMessage("malformed message while waiting for unchoke")
            }
        })?;

        match msg {
            PeerMessage::Unchoke => return Ok(()),
            // Ignore these -- expected chatter before a peer lets us download.
            PeerMessage::Bitfield(_) | PeerMessage::Have { .. } | PeerMessage::KeepAlive => continue,
            PeerMessage::Choke => continue, // already choked; keep waiting
            _ => continue, // anything else this early is unusual but non-fatal; ignore
        }
    }
}

/// Downloads a single piece's raw bytes over a connection that has ALREADY
/// been unchoked (see `download_piece` below for the common single-piece
/// case, and `download_pieces` for reusing one connection across several
/// pieces without repeating the interested/unchoke ritual each time).
async fn download_piece_data(
    stream: &mut TcpStream,
    piece_index: u32,
    piece_length: u32,
    expected_hash: [u8; 20],
) -> Result<Vec<u8>, DownloadError> {
    let blocks = split_into_blocks(piece_length);

    for &(begin, length) in &blocks {
        let request = PeerMessage::Request { index: piece_index, begin, length };
        send_message(stream, &request).await.map_err(DownloadError::Io)?;
    }

    // Buffer we'll fill in as Piece messages arrive -- they aren't
    // guaranteed to arrive in request order, so we place each block at its
    // correct `begin` offset rather than assuming sequential arrival.
    let mut piece_buf = vec![0u8; piece_length as usize];
    let mut bytes_received = 0usize;

    while bytes_received < piece_length as usize {
        let msg = receive_message(stream).await.map_err(|e| match e {
            crate::network::MessageIoError::Io(e) => DownloadError::Io(e),
            crate::network::MessageIoError::Protocol(_) => {
                DownloadError::UnexpectedMessage("malformed message while downloading blocks")
            }
        })?;

        match msg {
            PeerMessage::Piece { index, begin, block } if index == piece_index => {
                let start = begin as usize;
                let end = start + block.len();
                if end > piece_buf.len() {
                    return Err(DownloadError::UnexpectedMessage("block extends past piece boundary"));
                }
                piece_buf[start..end].copy_from_slice(&block);
                bytes_received += block.len();
            }
            // Ignore chatter that can legitimately interleave with data
            // transfer (e.g. keep-alives, Have announcements for other
            // pieces, a Choke that we're choosing to not act on mid-piece).
            PeerMessage::KeepAlive | PeerMessage::Have { .. } | PeerMessage::Choke => continue,
            PeerMessage::Piece { .. } => continue, // block for a different piece; ignore
            _ => continue,
        }
    }

    let actual_hash: [u8; 20] = Sha1::digest(&piece_buf).into();
    if actual_hash != expected_hash {
        return Err(DownloadError::HashMismatch);
    }

    Ok(piece_buf)
}

/// Downloads a single piece from an already-handshaken peer connection,
/// verifies it against `expected_hash`, and returns the raw piece bytes.
/// Waits for Unchoke first -- use `download_pieces` instead when you'll be
/// downloading more than one piece over the same connection, so you only
/// pay the interested/unchoke round trip once.
pub async fn download_piece(
    stream: &mut TcpStream,
    piece_index: u32,
    piece_length: u32,
    expected_hash: [u8; 20],
) -> Result<Vec<u8>, DownloadError> {
    wait_for_unchoke(stream).await?;
    download_piece_data(stream, piece_index, piece_length, expected_hash).await
}

/// Downloads several pieces over one connection, sending Interested and
/// waiting for Unchoke only once up front. `assignments` is a list of
/// (piece_index, piece_length, expected_hash) -- piece_length is passed
/// per-piece because the final piece of a torrent is usually shorter than
/// the rest. Stops and returns an error on the first piece that fails
/// (I/O error or hash mismatch); pieces already downloaded successfully
/// are returned alongside the error via the Err variant's data... actually,
/// for simplicity this returns only completed pieces on success and bails
/// out entirely on the first failure, leaving retry/reassignment to the
/// caller (e.g. the orchestration layer can hand a failed piece to a
/// different peer).
pub async fn download_pieces(
    stream: &mut TcpStream,
    assignments: &[(u32, u32, [u8; 20])],
) -> Result<Vec<(u32, Vec<u8>)>, DownloadError> {
    wait_for_unchoke(stream).await?;

    let mut results = Vec::with_capacity(assignments.len());
    for &(piece_index, piece_length, expected_hash) in assignments {
        let data = download_piece_data(stream, piece_index, piece_length, expected_hash).await?;
        results.push((piece_index, data));
    }
    Ok(results)
}

/// Writes a verified piece's bytes to `path` at the correct byte offset
/// (piece_index * piece_length), creating the file if it doesn't exist.
///
/// Uses `seek` + `write_all` rather than always appending, since pieces
/// can arrive and be written in any order (different pieces may come from
/// different peers in a real swarm download).
pub async fn write_piece_to_file(
    path: &std::path::Path,
    piece_index: u32,
    piece_length: u32,
    data: &[u8],
) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .await?;

    let offset = piece_index as u64 * piece_length as u64;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::connect_and_handshake;
    use crate::peer::{Handshake, PEER_ID_LEN};
    use tokio::net::TcpListener;

    fn sample_info_hash() -> [u8; 20] {
        [1u8; 20]
    }
    fn sample_peer_id(tag: u8) -> [u8; PEER_ID_LEN] {
        [tag; PEER_ID_LEN]
    }

    /// Spawns a minimal mock "seeder" on a background task: does the TCP
    /// handshake, waits for Interested, sends Unchoke, then serves whatever
    /// block requests it receives directly out of `piece_data`.
    fn spawn_mock_seeder(
        listener: TcpListener,
        piece_data: Vec<u8>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let (mut socket, _addr) = listener.accept().await.unwrap();

            let their_hs = crate::network::receive_handshake(&mut socket).await.unwrap();
            let our_hs = Handshake::new(their_hs.info_hash, sample_peer_id(0xEE));
            crate::network::send_handshake(&mut socket, &our_hs).await.unwrap();

            // Wait for Interested.
            loop {
                match receive_message(&mut socket).await.unwrap() {
                    PeerMessage::Interested => break,
                    _ => continue,
                }
            }
            send_message(&mut socket, &PeerMessage::Unchoke).await.unwrap();

            // Serve exactly as many Request messages as it takes to cover
            // the whole piece, then stop.
            let expected_requests = split_into_blocks(piece_data.len() as u32).len();
            for _ in 0..expected_requests {
                match receive_message(&mut socket).await.unwrap() {
                    PeerMessage::Request { index, begin, length } => {
                        let start = begin as usize;
                        let end = start + length as usize;
                        let block = piece_data[start..end].to_vec();
                        let msg = PeerMessage::Piece { index, begin, block };
                        send_message(&mut socket, &msg).await.unwrap();
                    }
                    other => panic!("expected Request, got {other:?}"),
                }
            }
        })
    }

    #[test]
    fn splits_piece_into_expected_block_boundaries() {
        // Exactly 2 full blocks.
        let blocks = split_into_blocks(2 * BLOCK_SIZE);
        assert_eq!(blocks, vec![(0, BLOCK_SIZE), (BLOCK_SIZE, BLOCK_SIZE)]);

        // 1 full block + 1 partial trailing block.
        let blocks = split_into_blocks(BLOCK_SIZE + 100);
        assert_eq!(blocks, vec![(0, BLOCK_SIZE), (BLOCK_SIZE, 100)]);

        // Smaller than one block.
        let blocks = split_into_blocks(500);
        assert_eq!(blocks, vec![(0, 500)]);
    }

    #[tokio::test]
    async fn downloads_and_verifies_a_piece_from_a_mock_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        // Two full 16 KiB blocks of distinguishable, non-uniform data so a
        // block-offset bug (e.g. blocks swapped) would actually be caught.
        let piece_length = 2 * BLOCK_SIZE;
        let mut piece_data = vec![0u8; piece_length as usize];
        for (i, b) in piece_data.iter_mut().enumerate() {
            *b = (i % 251) as u8; // pseudo-random-ish, deterministic pattern
        }
        let expected_hash: [u8; 20] = Sha1::digest(&piece_data).into();

        let seeder = spawn_mock_seeder(listener, piece_data.clone());

        let our_hs = Handshake::new(sample_info_hash(), sample_peer_id(0xAA));
        let (mut stream, _their_hs) = connect_and_handshake(server_addr, &our_hs).await.unwrap();

        let downloaded = download_piece(&mut stream, 0, piece_length, expected_hash)
            .await
            .expect("download should succeed and hash should verify");

        assert_eq!(downloaded, piece_data);
        seeder.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_a_piece_with_wrong_hash() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let piece_length = BLOCK_SIZE; // single block, simpler
        let real_data = vec![0x11u8; piece_length as usize];
        let wrong_expected_hash = [0u8; 20]; // deliberately does not match real_data

        let seeder = spawn_mock_seeder(listener, real_data);

        let our_hs = Handshake::new(sample_info_hash(), sample_peer_id(0xAA));
        let (mut stream, _their_hs) = connect_and_handshake(server_addr, &our_hs).await.unwrap();

        let result = download_piece(&mut stream, 0, piece_length, wrong_expected_hash).await;
        assert!(matches!(result, Err(DownloadError::HashMismatch)));

        seeder.await.unwrap();
    }

    #[tokio::test]
    async fn writes_multiple_pieces_to_correct_file_offsets() {
        let dir = std::env::temp_dir();
        let file_path = dir.join("bt_test_write_pieces.bin");
        std::fs::remove_file(&file_path).ok(); // clean slate if a prior run left it

        let piece_length: u32 = 10;
        let piece0 = vec![b'A'; 10];
        let piece1 = vec![b'B'; 10];

        // Write piece 1 first, then piece 0 -- order shouldn't matter since
        // each write seeks to its own offset.
        write_piece_to_file(&file_path, 1, piece_length, &piece1).await.unwrap();
        write_piece_to_file(&file_path, 0, piece_length, &piece0).await.unwrap();

        let contents = std::fs::read(&file_path).unwrap();
        assert_eq!(&contents[0..10], piece0.as_slice());
        assert_eq!(&contents[10..20], piece1.as_slice());

        std::fs::remove_file(&file_path).ok();
    }
}
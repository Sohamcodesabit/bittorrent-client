// network.rs
//
// Turns the pure encode/decode logic in peer.rs into actual TCP I/O using
// Tokio's async runtime. This is the layer that makes real handshakes and
// messages travel over a real socket to a real peer.

use crate::peer::{Handshake, PeerError, PeerMessage, HANDSHAKE_LEN};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Sends our handshake out over the socket.
///
/// `write_all` (as opposed to `write`) is important here: a single `write`
/// call is only guaranteed to send *some* of the bytes, especially for
/// larger payloads -- `write_all` loops internally until every byte is
/// actually on the wire, or an error occurs.
pub async fn send_handshake(stream: &mut TcpStream, handshake: &Handshake) -> std::io::Result<()> {
    stream.write_all(&handshake.serialize()).await
}

/// Reads exactly HANDSHAKE_LEN bytes from the socket and parses them.
///
/// `read_exact` blocks (asynchronously -- i.e. yields to other tasks, not
/// the OS thread) until the buffer is completely full or the connection
/// closes early. This matters because a single `read()` call over TCP can
/// return fewer bytes than you asked for even when more are coming --
/// TCP is a byte stream, not a message stream, so short reads are normal
/// and must always be handled, never assumed away.
pub async fn receive_handshake(stream: &mut TcpStream) -> Result<Handshake, HandshakeIoError> {
    let mut buf = [0u8; HANDSHAKE_LEN];
    stream.read_exact(&mut buf).await.map_err(HandshakeIoError::Io)?;
    Handshake::parse(&buf).map_err(HandshakeIoError::Protocol)
}

#[derive(Debug)]
pub enum HandshakeIoError {
    Io(std::io::Error),
    Protocol(PeerError),
}

impl std::fmt::Display for HandshakeIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeIoError::Io(e) => write!(f, "I/O error during handshake: {e}"),
            HandshakeIoError::Protocol(e) => write!(f, "protocol error during handshake: {e}"),
        }
    }
}
impl std::error::Error for HandshakeIoError {}

/// Serializes and writes a single message frame to the socket.
pub async fn send_message(stream: &mut TcpStream, message: &PeerMessage) -> std::io::Result<()> {
    stream.write_all(&message.serialize()).await
}

/// Reads exactly one complete message frame from the socket.
///
/// This is where the "consumed bytes" design of PeerMessage::parse (from
/// peer.rs) pays off conceptually, but note the approach here is simpler:
/// since TCP guarantees bytes arrive *in order* even though *read sizes*
/// aren't guaranteed, we can read the 4-byte length prefix first with
/// read_exact, then read exactly that many more bytes, and hand the whole
/// thing to PeerMessage::parse knowing it's already a complete frame.
pub async fn receive_message(stream: &mut TcpStream) -> Result<PeerMessage, MessageIoError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(MessageIoError::Io)?;
    let length = u32::from_be_bytes(len_buf) as usize;

    let mut frame = Vec::with_capacity(4 + length);
    frame.extend_from_slice(&len_buf);

    if length > 0 {
        let mut payload = vec![0u8; length];
        stream.read_exact(&mut payload).await.map_err(MessageIoError::Io)?;
        frame.extend_from_slice(&payload);
    }

    let (message, _consumed) = PeerMessage::parse(&frame).map_err(MessageIoError::Protocol)?;
    Ok(message)
}

#[derive(Debug)]
pub enum MessageIoError {
    Io(std::io::Error),
    Protocol(PeerError),
}

impl std::fmt::Display for MessageIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageIoError::Io(e) => write!(f, "I/O error reading message: {e}"),
            MessageIoError::Protocol(e) => write!(f, "protocol error reading message: {e}"),
        }
    }
}
impl std::error::Error for MessageIoError {}

/// Opens a TCP connection to a peer and performs the outbound side of the
/// handshake: send ours, then read and validate theirs. Returns the peer's
/// handshake (so we can inspect their peer_id) plus the connected stream.
pub async fn connect_and_handshake(
    addr: std::net::SocketAddr,
    our_handshake: &Handshake,
) -> Result<(TcpStream, Handshake), HandshakeIoError> {
    let mut stream = TcpStream::connect(addr).await.map_err(HandshakeIoError::Io)?;
    send_handshake(&mut stream, our_handshake).await.map_err(HandshakeIoError::Io)?;
    let their_handshake = receive_handshake(&mut stream).await?;
    Ok((stream, their_handshake))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PEER_ID_LEN;
    use crate::torrent::HASH_LEN;
    use tokio::net::TcpListener;

    fn sample_info_hash() -> [u8; HASH_LEN] {
        [7u8; HASH_LEN]
    }

    fn sample_peer_id(tag: u8) -> [u8; PEER_ID_LEN] {
        [tag; PEER_ID_LEN]
    }

    #[tokio::test]
    async fn handshake_round_trips_over_a_real_socket() {
        // Bind to port 0 -- the OS picks a free ephemeral port for us,
        // avoiding any risk of colliding with a port already in use.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_handshake = Handshake::new(sample_info_hash(), sample_peer_id(0xAA));
        let client_handshake = Handshake::new(sample_info_hash(), sample_peer_id(0xBB));

        // Spawn the "server" side as its own concurrent task -- this is
        // real Tokio concurrency: this task and the client below both run
        // on the same async runtime, taking turns whenever one is waiting
        // on I/O, without needing a manually managed OS thread each.
        let server_handshake_clone = server_handshake.clone();
        let server_task = tokio::spawn(async move {
            let (mut socket, _peer_addr) = listener.accept().await.unwrap();
            let their_hs = receive_handshake(&mut socket).await.unwrap();
            send_handshake(&mut socket, &server_handshake_clone).await.unwrap();
            their_hs
        });

        // Client side: connect and exchange handshakes.
        let (mut client_socket, server_hs_seen_by_client) =
            connect_and_handshake(server_addr, &client_handshake).await.unwrap();
        let _ = &mut client_socket; // keep the stream alive until we're done with it

        let client_hs_seen_by_server = server_task.await.unwrap();

        assert_eq!(server_hs_seen_by_client, server_handshake);
        assert_eq!(client_hs_seen_by_server, client_handshake);
    }

    #[tokio::test]
    async fn messages_round_trip_over_a_real_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut socket, _addr) = listener.accept().await.unwrap();
            // Server: receive an Interested message, then send back a
            // realistic 16 KiB Piece message, simulating a tiny download.
            let received = receive_message(&mut socket).await.unwrap();
            assert_eq!(received, PeerMessage::Interested);

            let block = vec![0x42u8; 16 * 1024];
            let piece_msg = PeerMessage::Piece { index: 0, begin: 0, block };
            send_message(&mut socket, &piece_msg).await.unwrap();
        });

        let mut client_socket = TcpStream::connect(server_addr).await.unwrap();
        send_message(&mut client_socket, &PeerMessage::Interested).await.unwrap();
        let response = receive_message(&mut client_socket).await.unwrap();

        match response {
            PeerMessage::Piece { index, begin, block } => {
                assert_eq!(index, 0);
                assert_eq!(begin, 0);
                assert_eq!(block.len(), 16 * 1024);
                assert!(block.iter().all(|&b| b == 0x42));
            }
            other => panic!("expected Piece message, got {other:?}"),
        }

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn multiple_messages_sent_back_to_back_are_each_read_correctly() {
        // Proves receive_message correctly frames consecutive messages
        // even when they arrive close together on the same connection --
        // exactly the "many small messages in a row" pattern a real
        // download will produce (choke/unchoke/have/request all interleaved).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut socket, _addr) = listener.accept().await.unwrap();
            send_message(&mut socket, &PeerMessage::Unchoke).await.unwrap();
            send_message(&mut socket, &PeerMessage::Have { piece_index: 3 }).await.unwrap();
            send_message(&mut socket, &PeerMessage::KeepAlive).await.unwrap();
        });

        let mut client_socket = TcpStream::connect(server_addr).await.unwrap();
        let first = receive_message(&mut client_socket).await.unwrap();
        let second = receive_message(&mut client_socket).await.unwrap();
        let third = receive_message(&mut client_socket).await.unwrap();

        assert_eq!(first, PeerMessage::Unchoke);
        assert_eq!(second, PeerMessage::Have { piece_index: 3 });
        assert_eq!(third, PeerMessage::KeepAlive);

        server_task.await.unwrap();
    }
}
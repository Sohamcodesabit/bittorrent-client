// peer.rs
//
// The BitTorrent peer wire protocol: the binary handshake and message
// format two clients use to talk to each other over TCP once they've
// connected. Sockets themselves come in the next step -- this module is
// pure encode/decode logic, deliberately kept separate and unit-testable
// without a live network connection.

use crate::torrent::HASH_LEN;
use std::fmt;

pub const PROTOCOL_STRING: &[u8] = b"BitTorrent protocol";
pub const PEER_ID_LEN: usize = 20;
/// 1 (pstrlen) + 19 (pstr) + 8 (reserved) + 20 (info_hash) + 20 (peer_id)
pub const HANDSHAKE_LEN: usize = 1 + PROTOCOL_STRING.len() + 8 + HASH_LEN + PEER_ID_LEN;

#[derive(Debug)]
pub enum PeerError {
    UnexpectedEof,
    WrongProtocol,
    UnknownMessageId(u8),
    TruncatedPayload { expected: usize, got: usize },
}

impl fmt::Display for PeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerError::UnexpectedEof => write!(f, "unexpected end of input"),
            PeerError::WrongProtocol => write!(f, "handshake did not identify BitTorrent protocol"),
            PeerError::UnknownMessageId(id) => write!(f, "unknown message id: {id}"),
            PeerError::TruncatedPayload { expected, got } => {
                write!(f, "payload truncated: expected {expected} bytes, got {got}")
            }
        }
    }
}

impl std::error::Error for PeerError {}

// ---------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub info_hash: [u8; HASH_LEN],
    pub peer_id: [u8; PEER_ID_LEN],
}

impl Handshake {
    pub fn new(info_hash: [u8; HASH_LEN], peer_id: [u8; PEER_ID_LEN]) -> Self {
        Handshake { info_hash, peer_id }
    }

    /// Serializes to the fixed 68-byte handshake wire format.
    pub fn serialize(&self) -> [u8; HANDSHAKE_LEN] {
        let mut buf = [0u8; HANDSHAKE_LEN];
        let mut pos = 0;

        buf[pos] = PROTOCOL_STRING.len() as u8; // pstrlen
        pos += 1;

        buf[pos..pos + PROTOCOL_STRING.len()].copy_from_slice(PROTOCOL_STRING);
        pos += PROTOCOL_STRING.len();

        // 8 reserved bytes for optional protocol extensions -- all zero,
        // since we don't implement any extensions (e.g. DHT, fast extension).
        pos += 8;

        buf[pos..pos + HASH_LEN].copy_from_slice(&self.info_hash);
        pos += HASH_LEN;

        buf[pos..pos + PEER_ID_LEN].copy_from_slice(&self.peer_id);
        pos += PEER_ID_LEN;

        debug_assert_eq!(pos, HANDSHAKE_LEN);
        buf
    }

    /// Parses a 68-byte handshake buffer received from a peer.
    pub fn parse(buf: &[u8]) -> Result<Handshake, PeerError> {
        if buf.len() < HANDSHAKE_LEN {
            return Err(PeerError::UnexpectedEof);
        }

        let pstrlen = buf[0] as usize;
        if pstrlen != PROTOCOL_STRING.len() || &buf[1..1 + pstrlen] != PROTOCOL_STRING {
            return Err(PeerError::WrongProtocol);
        }

        let mut pos = 1 + pstrlen;
        pos += 8; // skip reserved bytes -- we don't negotiate any extensions

        let mut info_hash = [0u8; HASH_LEN];
        info_hash.copy_from_slice(&buf[pos..pos + HASH_LEN]);
        pos += HASH_LEN;

        let mut peer_id = [0u8; PEER_ID_LEN];
        peer_id.copy_from_slice(&buf[pos..pos + PEER_ID_LEN]);

        Ok(Handshake { info_hash, peer_id })
    }
}

// ---------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    /// Length-0 message: no ID byte at all. Sent periodically to keep the
    /// TCP connection from being dropped as idle.
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    /// "I now have piece #index." Sent whenever a peer finishes downloading
    /// (or, for a seeder, whenever it wants to announce it has) a piece.
    Have { piece_index: u32 },
    /// A bitmap, one bit per piece, MSB-first within each byte: "here is
    /// everything I currently have." Sent once, right after the handshake.
    Bitfield(Vec<u8>),
    /// "Please send me `length` bytes of piece `index`, starting at byte
    /// offset `begin` within that piece." Pieces are downloaded in
    /// sub-piece "blocks" (commonly 16 KiB), not all at once.
    Request { index: u32, begin: u32, length: u32 },
    /// The actual data transfer message: `block` is the raw file bytes for
    /// piece `index` starting at offset `begin`.
    Piece { index: u32, begin: u32, block: Vec<u8> },
    /// "Never mind, I no longer need that request" (e.g. we already got the
    /// block from a different peer in the swarm).
    Cancel { index: u32, begin: u32, length: u32 },
}

impl PeerMessage {
    const ID_CHOKE: u8 = 0;
    const ID_UNCHOKE: u8 = 1;
    const ID_INTERESTED: u8 = 2;
    const ID_NOT_INTERESTED: u8 = 3;
    const ID_HAVE: u8 = 4;
    const ID_BITFIELD: u8 = 5;
    const ID_REQUEST: u8 = 6;
    const ID_PIECE: u8 = 7;
    const ID_CANCEL: u8 = 8;

    /// Serializes to the wire format: 4-byte big-endian length prefix,
    /// then (for everything but KeepAlive) a 1-byte message ID and payload.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();

        match self {
            PeerMessage::KeepAlive => {
                out.extend_from_slice(&0u32.to_be_bytes());
            }
            PeerMessage::Choke => write_framed(&mut out, Self::ID_CHOKE, &[]),
            PeerMessage::Unchoke => write_framed(&mut out, Self::ID_UNCHOKE, &[]),
            PeerMessage::Interested => write_framed(&mut out, Self::ID_INTERESTED, &[]),
            PeerMessage::NotInterested => write_framed(&mut out, Self::ID_NOT_INTERESTED, &[]),
            PeerMessage::Have { piece_index } => {
                write_framed(&mut out, Self::ID_HAVE, &piece_index.to_be_bytes());
            }
            PeerMessage::Bitfield(bits) => {
                write_framed(&mut out, Self::ID_BITFIELD, bits);
            }
            PeerMessage::Request { index, begin, length } => {
                let mut payload = Vec::with_capacity(12);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
                write_framed(&mut out, Self::ID_REQUEST, &payload);
            }
            PeerMessage::Piece { index, begin, block } => {
                let mut payload = Vec::with_capacity(8 + block.len());
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(block);
                write_framed(&mut out, Self::ID_PIECE, &payload);
            }
            PeerMessage::Cancel { index, begin, length } => {
                let mut payload = Vec::with_capacity(12);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
                write_framed(&mut out, Self::ID_CANCEL, &payload);
            }
        }

        out
    }

    /// Parses exactly one message from `buf`, which must contain the 4-byte
    /// length prefix followed by at least that many more bytes (i.e. this
    /// expects a *complete* frame, not a partial one from a socket read --
    /// the caller is responsible for buffering until a full frame arrives).
    ///
    /// Returns the parsed message and the total number of bytes consumed,
    /// so the caller can advance past this message to look for the next one.
    pub fn parse(buf: &[u8]) -> Result<(PeerMessage, usize), PeerError> {
        if buf.len() < 4 {
            return Err(PeerError::UnexpectedEof);
        }
        let length = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        let total_len = 4 + length;

        if length == 0 {
            return Ok((PeerMessage::KeepAlive, 4));
        }
        if buf.len() < total_len {
            return Err(PeerError::TruncatedPayload { expected: total_len, got: buf.len() });
        }

        let id = buf[4];
        let payload = &buf[5..total_len];

        let message = match id {
            Self::ID_CHOKE => PeerMessage::Choke,
            Self::ID_UNCHOKE => PeerMessage::Unchoke,
            Self::ID_INTERESTED => PeerMessage::Interested,
            Self::ID_NOT_INTERESTED => PeerMessage::NotInterested,
            Self::ID_HAVE => {
                let piece_index = read_u32(payload, 0)?;
                PeerMessage::Have { piece_index }
            }
            Self::ID_BITFIELD => PeerMessage::Bitfield(payload.to_vec()),
            Self::ID_REQUEST => PeerMessage::Request {
                index: read_u32(payload, 0)?,
                begin: read_u32(payload, 4)?,
                length: read_u32(payload, 8)?,
            },
            Self::ID_PIECE => {
                let index = read_u32(payload, 0)?;
                let begin = read_u32(payload, 4)?;
                let block = payload.get(8..).ok_or(PeerError::TruncatedPayload {
                    expected: 8,
                    got: payload.len(),
                })?;
                PeerMessage::Piece { index, begin, block: block.to_vec() }
            }
            Self::ID_CANCEL => PeerMessage::Cancel {
                index: read_u32(payload, 0)?,
                begin: read_u32(payload, 4)?,
                length: read_u32(payload, 8)?,
            },
            other => return Err(PeerError::UnknownMessageId(other)),
        };

        Ok((message, total_len))
    }
}

/// Appends a length-prefixed, ID-tagged message frame to `out`.
/// length = 1 (for the ID byte) + payload.len().
fn write_framed(out: &mut Vec<u8>, id: u8, payload: &[u8]) {
    let length = 1 + payload.len() as u32;
    out.extend_from_slice(&length.to_be_bytes());
    out.push(id);
    out.extend_from_slice(payload);
}

/// Reads a big-endian u32 out of `payload` at byte offset `offset`,
/// bounds-checked so malformed/short peer data can't panic us.
fn read_u32(payload: &[u8], offset: usize) -> Result<u32, PeerError> {
    let bytes = payload.get(offset..offset + 4).ok_or(PeerError::TruncatedPayload {
        expected: offset + 4,
        got: payload.len(),
    })?;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_info_hash() -> [u8; HASH_LEN] {
        let mut h = [0u8; HASH_LEN];
        for (i, b) in h.iter_mut().enumerate() {
            *b = i as u8;
        }
        h
    }

    fn sample_peer_id() -> [u8; PEER_ID_LEN] {
        *b"-RS0001-abcdefghijkl"
    }

    #[test]
    fn handshake_round_trips() {
        let hs = Handshake::new(sample_info_hash(), sample_peer_id());
        let bytes = hs.serialize();
        assert_eq!(bytes.len(), HANDSHAKE_LEN);
        assert_eq!(bytes.len(), 68, "BitTorrent handshake must always be exactly 68 bytes");

        let parsed = Handshake::parse(&bytes).unwrap();
        assert_eq!(parsed, hs);
    }

    #[test]
    fn handshake_rejects_wrong_protocol_string() {
        let mut bytes = Handshake::new(sample_info_hash(), sample_peer_id()).serialize();
        bytes[5] = b'X'; // corrupt a byte inside "BitTorrent protocol"
        assert!(matches!(Handshake::parse(&bytes), Err(PeerError::WrongProtocol)));
    }

    #[test]
    fn handshake_rejects_truncated_input() {
        let bytes = Handshake::new(sample_info_hash(), sample_peer_id()).serialize();
        assert!(matches!(Handshake::parse(&bytes[..30]), Err(PeerError::UnexpectedEof)));
    }

    #[test]
    fn keep_alive_round_trips() {
        let bytes = PeerMessage::KeepAlive.serialize();
        assert_eq!(bytes, 0u32.to_be_bytes());
        let (msg, consumed) = PeerMessage::parse(&bytes).unwrap();
        assert_eq!(msg, PeerMessage::KeepAlive);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn zero_length_messages_round_trip() {
        for msg in [
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
        ] {
            let bytes = msg.serialize();
            // 4-byte length prefix (value 1) + 1 id byte, no payload.
            assert_eq!(bytes.len(), 5);
            let (parsed, consumed) = PeerMessage::parse(&bytes).unwrap();
            assert_eq!(parsed, msg);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn have_round_trips() {
        let msg = PeerMessage::Have { piece_index: 42 };
        let bytes = msg.serialize();
        assert_eq!(bytes.len(), 4 + 1 + 4); // length prefix + id + u32 payload
        let (parsed, consumed) = PeerMessage::parse(&bytes).unwrap();
        assert_eq!(parsed, msg);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn bitfield_round_trips_with_arbitrary_bytes() {
        let msg = PeerMessage::Bitfield(vec![0xFF, 0x00, 0xA5]);
        let bytes = msg.serialize();
        let (parsed, consumed) = PeerMessage::parse(&bytes).unwrap();
        assert_eq!(parsed, msg);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn request_round_trips() {
        let msg = PeerMessage::Request { index: 5, begin: 16384, length: 16384 };
        let bytes = msg.serialize();
        let (parsed, _) = PeerMessage::parse(&bytes).unwrap();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn piece_round_trips_with_realistic_block_size() {
        let block = vec![0x7Au8; 16 * 1024]; // realistic 16 KiB block
        let msg = PeerMessage::Piece { index: 3, begin: 0, block: block.clone() };
        let bytes = msg.serialize();

        // length prefix (4) + id (1) + index (4) + begin (4) + block
        assert_eq!(bytes.len(), 4 + 1 + 4 + 4 + block.len());

        let (parsed, consumed) = PeerMessage::parse(&bytes).unwrap();
        assert_eq!(parsed, msg);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn cancel_round_trips() {
        let msg = PeerMessage::Cancel { index: 1, begin: 0, length: 16384 };
        let bytes = msg.serialize();
        let (parsed, _) = PeerMessage::parse(&bytes).unwrap();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn parse_reports_truncated_frame_without_panicking() {
        // Claims a 100-byte payload but only supplies a handful of bytes --
        // must error cleanly, never panic or read out of bounds.
        let mut bytes = 100u32.to_be_bytes().to_vec();
        bytes.push(PeerMessage::ID_PIECE);
        bytes.extend_from_slice(&[1, 2, 3]); // way short of the promised 100

        let err = PeerMessage::parse(&bytes).unwrap_err();
        assert!(matches!(err, PeerError::TruncatedPayload { .. }));
    }

    #[test]
    fn parse_rejects_unknown_message_id() {
        let mut bytes = 1u32.to_be_bytes().to_vec();
        bytes.push(99); // not a real message ID
        let err = PeerMessage::parse(&bytes).unwrap_err();
        assert!(matches!(err, PeerError::UnknownMessageId(99)));
    }

    #[test]
    fn parse_leaves_remaining_bytes_for_the_next_message() {
        // Simulates two messages arriving back-to-back in one socket read,
        // proving the caller can use `consumed` to walk through a buffer.
        let mut buf = PeerMessage::Unchoke.serialize();
        buf.extend_from_slice(&PeerMessage::Have { piece_index: 7 }.serialize());

        let (first, consumed1) = PeerMessage::parse(&buf).unwrap();
        assert_eq!(first, PeerMessage::Unchoke);

        let (second, _consumed2) = PeerMessage::parse(&buf[consumed1..]).unwrap();
        assert_eq!(second, PeerMessage::Have { piece_index: 7 });
    }
}
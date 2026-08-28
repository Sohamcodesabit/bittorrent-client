// bencode.rs
//
// Implements Bencode encoding/decoding: BitTorrent's serialization format.
// Spec (informal): https://wiki.theory.org/BitTorrentSpecification#Bencoding
//
// Four types only:
//   integers    i<base10>e            e.g. i42e, i-3e
//   byte strings <len>:<bytes>        e.g. 4:spam
//   lists       l<items>e             e.g. l4:spam4:eggse
//   dicts       d<key><val>...e       e.g. d3:cow3:mooe   (keys MUST be sorted)

use std::collections::BTreeMap;
use std::fmt;

/// A decoded Bencode value.
///
/// Bytes (not String) because torrent data isn't guaranteed to be valid UTF-8
/// (piece hashes, for instance, are raw binary SHA1 digests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BValue {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<BValue>),
    // BTreeMap keeps keys sorted automatically -- required by the bencode spec
    // and essential later for computing a reproducible info_hash.
    Dict(BTreeMap<Vec<u8>, BValue>),
}

#[derive(Debug)]
pub enum BError {
    UnexpectedEof,
    InvalidInteger,
    InvalidStringLength,
    UnexpectedToken(u8),
    TrailingData,
}

impl fmt::Display for BError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BError::UnexpectedEof => write!(f, "unexpected end of input"),
            BError::InvalidInteger => write!(f, "invalid integer literal"),
            BError::InvalidStringLength => write!(f, "invalid byte-string length prefix"),
            BError::UnexpectedToken(b) => write!(f, "unexpected byte: {:?}", *b as char),
            BError::TrailingData => write!(f, "trailing data after top-level value"),
        }
    }
}

impl std::error::Error for BError {}

impl BValue {
    /// Convenience accessor: treat self as a dict and look up a key by its
    /// ASCII/UTF-8 name (e.g. value.get("announce")).
    pub fn get(&self, key: &str) -> Option<&BValue> {
        match self {
            BValue::Dict(map) => map.get(key.as_bytes()),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            BValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            BValue::Int(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[BValue]> {
        match self {
            BValue::List(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, BValue>> {
        match self {
            BValue::Dict(map) => Some(map),
            _ => None,
        }
    }
}

/// Parses a single bencoded value from a byte slice, erroring if there's
/// leftover data afterward (a .torrent file is exactly one top-level dict).
pub fn decode(input: &[u8]) -> Result<BValue, BError> {
    let mut decoder = Decoder { input, pos: 0 };
    let value = decoder.decode_value()?;
    if decoder.pos != decoder.input.len() {
        return Err(BError::TrailingData);
    }
    Ok(value)
}

struct Decoder<'a> {
    input: &'a [u8],
    pos: usize, // cursor: index of the next unread byte
}

impl<'a> Decoder<'a> {
    /// Look at the next byte without consuming it, to decide which type to parse.
    fn peek(&self) -> Result<u8, BError> {
        self.input.get(self.pos).copied().ok_or(BError::UnexpectedEof)
    }

    fn decode_value(&mut self) -> Result<BValue, BError> {
        match self.peek()? {
            b'i' => self.decode_int(),
            b'l' => self.decode_list(),
            b'd' => self.decode_dict(),
            b'0'..=b'9' => self.decode_bytes().map(BValue::Bytes),
            other => Err(BError::UnexpectedToken(other)),
        }
    }

    /// i<digits>e   e.g. i42e, i-7e, i0e
    fn decode_int(&mut self) -> Result<BValue, BError> {
        self.pos += 1; // consume 'i'
        let end = self.find(b'e')?;
        let raw = std::str::from_utf8(&self.input[self.pos..end])
            .map_err(|_| BError::InvalidInteger)?;
        // Bencode disallows leading zeros (except "0" itself) and "-0";
        // we don't strictly enforce that here, just parse the number.
        let value: i64 = raw.parse().map_err(|_| BError::InvalidInteger)?;
        self.pos = end + 1; // consume 'e'
        Ok(BValue::Int(value))
    }

    /// <len>:<bytes>   e.g. 4:spam
    fn decode_bytes(&mut self) -> Result<Vec<u8>, BError> {
        let colon = self.find(b':')?;
        let len_str = std::str::from_utf8(&self.input[self.pos..colon])
            .map_err(|_| BError::InvalidStringLength)?;
        let len: usize = len_str.parse().map_err(|_| BError::InvalidStringLength)?;
        let start = colon + 1;
        let end = start.checked_add(len).ok_or(BError::InvalidStringLength)?;
        if end > self.input.len() {
            return Err(BError::UnexpectedEof);
        }
        let bytes = self.input[start..end].to_vec();
        self.pos = end;
        Ok(bytes)
    }

    /// l<value>*e
    fn decode_list(&mut self) -> Result<BValue, BError> {
        self.pos += 1; // consume 'l'
        let mut items = Vec::new();
        loop {
            if self.peek()? == b'e' {
                self.pos += 1;
                break;
            }
            items.push(self.decode_value()?);
        }
        Ok(BValue::List(items))
    }

    /// d(<key><value>)*e   -- keys are always bencoded byte strings
    fn decode_dict(&mut self) -> Result<BValue, BError> {
        self.pos += 1; // consume 'd'
        let mut map = BTreeMap::new();
        loop {
            if self.peek()? == b'e' {
                self.pos += 1;
                break;
            }
            let key = self.decode_bytes()?; // dict keys are always raw byte strings
            let value = self.decode_value()?;
            map.insert(key, value);
        }
        Ok(BValue::Dict(map))
    }

    /// Scan forward from the cursor for `target`, returning its index.
    fn find(&self, target: u8) -> Result<usize, BError> {
        self.input[self.pos..]
            .iter()
            .position(|&b| b == target)
            .map(|i| self.pos + i)
            .ok_or(BError::UnexpectedEof)
    }
}

/// Serializes a BValue back into its canonical bencoded byte representation.
/// Because BValue::Dict is a BTreeMap, keys always come out sorted, matching
/// what the original .torrent file's encoder would have produced.
pub fn encode(value: &BValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

fn encode_into(value: &BValue, out: &mut Vec<u8>) {
    match value {
        BValue::Int(n) => {
            out.push(b'i');
            out.extend_from_slice(n.to_string().as_bytes());
            out.push(b'e');
        }
        BValue::Bytes(bytes) => {
            out.extend_from_slice(bytes.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(bytes);
        }
        BValue::List(items) => {
            out.push(b'l');
            for item in items {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        BValue::Dict(map) => {
            out.push(b'd');
            // BTreeMap iterates in key-sorted order already.
            for (key, val) in map {
                encode_into(&BValue::Bytes(key.clone()), out);
                encode_into(val, out);
            }
            out.push(b'e');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(pairs: Vec<(&str, BValue)>) -> BValue {
        let mut map = BTreeMap::new();
        for (k, v) in pairs {
            map.insert(k.as_bytes().to_vec(), v);
        }
        BValue::Dict(map)
    }

    #[test]
    fn decodes_integers() {
        assert_eq!(decode(b"i42e").unwrap(), BValue::Int(42));
        assert_eq!(decode(b"i-7e").unwrap(), BValue::Int(-7));
        assert_eq!(decode(b"i0e").unwrap(), BValue::Int(0));
    }

    #[test]
    fn decodes_byte_strings() {
        assert_eq!(decode(b"4:spam").unwrap(), BValue::Bytes(b"spam".to_vec()));
        assert_eq!(decode(b"0:").unwrap(), BValue::Bytes(vec![]));
    }

    #[test]
    fn decodes_binary_safe_strings() {
        // Piece hashes are raw bytes, not valid UTF-8 -- must round-trip exactly.
        let raw: Vec<u8> = vec![0xff, 0x00, 0x10, 0xAB, 0x00];
        let mut input = format!("{}:", raw.len()).into_bytes();
        input.extend_from_slice(&raw);
        assert_eq!(decode(&input).unwrap(), BValue::Bytes(raw));
    }

    #[test]
    fn decodes_lists() {
        assert_eq!(
            decode(b"l4:spam4:eggse").unwrap(),
            BValue::List(vec![BValue::Bytes(b"spam".to_vec()), BValue::Bytes(b"eggs".to_vec())])
        );
    }

    #[test]
    fn decodes_nested_dicts() {
        let parsed = decode(b"d3:cow3:moo4:spaml1:a1:bee").unwrap();
        let expected = dict(vec![
            ("cow", BValue::Bytes(b"moo".to_vec())),
            ("spam", BValue::List(vec![BValue::Bytes(b"a".to_vec()), BValue::Bytes(b"b".to_vec())])),
        ]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn round_trips_through_encode() {
        let samples: Vec<&[u8]> = vec![
            b"i42e",
            b"4:spam",
            b"l4:spam4:eggse",
            b"d3:cow3:moo4:spam4:eggse",
        ];
        for s in samples {
            let decoded = decode(s).unwrap();
            let re_encoded = encode(&decoded);
            assert_eq!(re_encoded, s, "round-trip mismatch for {:?}", s);
        }
    }

    #[test]
    fn rejects_trailing_data() {
        assert!(matches!(decode(b"i1eGARBAGE"), Err(BError::TrailingData)));
    }

    #[test]
    fn rejects_truncated_input() {
        assert!(matches!(decode(b"i42"), Err(BError::UnexpectedEof)));
        assert!(matches!(decode(b"4:sp"), Err(BError::UnexpectedEof)));
    }
}
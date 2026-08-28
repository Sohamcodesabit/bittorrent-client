// torrent.rs
//
// Parses .torrent files (which are just bencoded dicts, see bencode.rs)
// into a typed Torrent struct, and computes the info_hash that uniquely
// identifies the torrent to trackers and peers.

use crate::bencode::{self, BValue};
use sha1::{Digest, Sha1};
use std::fmt;

pub const HASH_LEN: usize = 20; // SHA1 digest length in bytes

/// One file's metadata inside a multi-file torrent.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub length: i64,
    /// Path segments relative to the torrent's root directory, e.g.
    /// ["subdir", "file.txt"] -- NOT yet joined with OS-specific separators.
    pub path: Vec<String>,
}

/// Whether this torrent describes one file or a directory of files.
/// Bencode distinguishes these by whether `info` has a `length` key
/// (single-file) or a `files` list (multi-file) -- never both.
#[derive(Debug, Clone)]
pub enum FileLayout {
    Single { length: i64 },
    Multi { files: Vec<FileEntry> },
}

#[derive(Debug, Clone)]
pub struct Torrent {
    pub announce: String,
    pub name: String,
    pub piece_length: i64,
    /// One 20-byte SHA1 hash per piece, in piece order. We verify each
    /// downloaded piece against the matching entry here before accepting it.
    pub pieces: Vec<[u8; HASH_LEN]>,
    pub layout: FileLayout,
    /// SHA1 of the raw bencoded `info` dict. This -- not the file name --
    /// is what identifies this torrent to trackers and peers.
    pub info_hash: [u8; HASH_LEN],
}

#[derive(Debug)]
pub enum TorrentError {
    Bencode(bencode::BError),
    MissingField(&'static str),
    WrongType(&'static str),
    MalformedPieces,
    MalformedFileEntry,
}

impl fmt::Display for TorrentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TorrentError::Bencode(e) => write!(f, "bencode error: {e}"),
            TorrentError::MissingField(k) => write!(f, "missing required field: {k}"),
            TorrentError::WrongType(k) => write!(f, "field has wrong type: {k}"),
            TorrentError::MalformedPieces => {
                write!(f, "'pieces' length is not a multiple of {HASH_LEN}")
            }
            TorrentError::MalformedFileEntry => write!(f, "malformed entry in 'files' list"),
        }
    }
}

impl std::error::Error for TorrentError {}

impl From<bencode::BError> for TorrentError {
    fn from(e: bencode::BError) -> Self {
        TorrentError::Bencode(e)
    }
}

impl Torrent {
    /// Total size of everything this torrent describes, in bytes.
    pub fn total_length(&self) -> i64 {
        match &self.layout {
            FileLayout::Single { length } => *length,
            FileLayout::Multi { files } => files.iter().map(|f| f.length).sum(),
        }
    }

    /// Human-readable info_hash, e.g. for logging ("a1b2c3...").
    pub fn info_hash_hex(&self) -> String {
        self.info_hash.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Parses raw .torrent file bytes into a Torrent.
pub fn parse(bytes: &[u8]) -> Result<Torrent, TorrentError> {
    let root = bencode::decode(bytes)?;

    let announce = root
        .get("announce")
        .ok_or(TorrentError::MissingField("announce"))?
        .as_str()
        .ok_or(TorrentError::WrongType("announce"))?
        .to_string();

    let info = root.get("info").ok_or(TorrentError::MissingField("info"))?;

    // info_hash = SHA1 of the *exact* bencoded bytes of the info dict.
    // Since bencode dict keys must be sorted, and our decoder stores dict
    // entries in a BTreeMap (auto-sorted), re-encoding `info` reproduces
    // the original file's bytes for any spec-compliant torrent.
    let info_bytes = bencode::encode(info);
    let info_hash: [u8; HASH_LEN] = Sha1::digest(&info_bytes).into();

    let name = info
        .get("name")
        .ok_or(TorrentError::MissingField("info.name"))?
        .as_str()
        .ok_or(TorrentError::WrongType("info.name"))?
        .to_string();

    let piece_length = info
        .get("piece length")
        .ok_or(TorrentError::MissingField("info.piece length"))?
        .as_int()
        .ok_or(TorrentError::WrongType("info.piece length"))?;

    let pieces_raw = info
        .get("pieces")
        .ok_or(TorrentError::MissingField("info.pieces"))?
        .as_bytes()
        .ok_or(TorrentError::WrongType("info.pieces"))?;

    if pieces_raw.len() % HASH_LEN != 0 {
        return Err(TorrentError::MalformedPieces);
    }
    let pieces: Vec<[u8; HASH_LEN]> = pieces_raw
        .chunks_exact(HASH_LEN)
        .map(|chunk| chunk.try_into().expect("chunks_exact guarantees len == HASH_LEN"))
        .collect();

    let layout = if let Some(length_val) = info.get("length") {
        // Single-file torrent.
        let length = length_val.as_int().ok_or(TorrentError::WrongType("info.length"))?;
        FileLayout::Single { length }
    } else if let Some(files_val) = info.get("files") {
        // Multi-file torrent.
        let files_list = files_val.as_list().ok_or(TorrentError::WrongType("info.files"))?;
        let mut files = Vec::with_capacity(files_list.len());
        for entry in files_list {
            let length = entry
                .get("length")
                .and_then(BValue::as_int)
                .ok_or(TorrentError::MalformedFileEntry)?;
            let path_list = entry
                .get("path")
                .and_then(BValue::as_list)
                .ok_or(TorrentError::MalformedFileEntry)?;
            let path: Vec<String> = path_list
                .iter()
                .map(|seg| seg.as_str().map(str::to_string))
                .collect::<Option<_>>()
                .ok_or(TorrentError::MalformedFileEntry)?;
            files.push(FileEntry { length, path });
        }
        FileLayout::Multi { files }
    } else {
        return Err(TorrentError::MissingField("info.length or info.files"));
    };

    Ok(Torrent {
        announce,
        name,
        piece_length,
        pieces,
        layout,
        info_hash,
    })
}

/// Builds a .torrent file (as bencoded bytes) for a single local file.
/// This is what you'll use to create the torrent for your demo test file,
/// since a file you made up won't exist on any real tracker.
///
/// Splits the file into `piece_length`-byte pieces, SHA1-hashes each one,
/// and assembles the standard single-file torrent dict shape.
pub fn create_single_file_torrent(
    file_path: &std::path::Path,
    announce: &str,
    piece_length: i64,
) -> std::io::Result<Vec<u8>> {
    use std::io::Read;

    let mut file = std::fs::File::open(file_path)?;
    let total_length = file.metadata()?.len() as i64;

    let name = file_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unnamed".to_string());

    // Read the file piece_length bytes at a time, hashing each piece and
    // concatenating the hashes -- this becomes the `pieces` byte string.
    let mut pieces_concat = Vec::new();
    let mut buf = vec![0u8; piece_length.max(1) as usize];
    loop {
        // read() can return fewer bytes than the buffer even before EOF, so
        // we loop until either the buffer is full or we truly hit EOF.
        let mut filled = 0;
        while filled < buf.len() {
            let n = file.read(&mut buf[filled..])?;
            if n == 0 {
                break; // EOF
            }
            filled += n;
        }
        if filled == 0 {
            break; // nothing left to read
        }
        let hash: [u8; HASH_LEN] = Sha1::digest(&buf[..filled]).into();
        pieces_concat.extend_from_slice(&hash);
        if filled < buf.len() {
            break; // that was the final, shorter-than-piece_length piece
        }
    }

    let mut info_map = std::collections::BTreeMap::new();
    info_map.insert(b"name".to_vec(), BValue::Bytes(name.into_bytes()));
    info_map.insert(b"piece length".to_vec(), BValue::Int(piece_length));
    info_map.insert(b"pieces".to_vec(), BValue::Bytes(pieces_concat));
    info_map.insert(b"length".to_vec(), BValue::Int(total_length));
    let info = BValue::Dict(info_map);

    let mut root_map = std::collections::BTreeMap::new();
    root_map.insert(b"announce".to_vec(), BValue::Bytes(announce.as_bytes().to_vec()));
    root_map.insert(b"info".to_vec(), info);
    let root = BValue::Dict(root_map);

    Ok(bencode::encode(&root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_a_minimal_single_file_torrent() {
        // Hand-built bencode: 1 piece, single-file layout.
        let fake_piece_hash = [0xAAu8; HASH_LEN];
        let mut input = Vec::new();
        input.extend_from_slice(b"d8:announce19:http://tracker.test4:info");
        input.extend_from_slice(b"d6:lengthi123e4:name8:test.txt12:piece lengthi16384e6:pieces");
        input.extend_from_slice(format!("{}:", HASH_LEN).as_bytes());
        input.extend_from_slice(&fake_piece_hash);
        input.extend_from_slice(b"ee");

        let t = parse(&input).expect("should parse");
        assert_eq!(t.announce, "http://tracker.test");
        assert_eq!(t.name, "test.txt");
        assert_eq!(t.piece_length, 16384);
        assert_eq!(t.pieces, vec![fake_piece_hash]);
        assert_eq!(t.total_length(), 123);
        matches!(t.layout, FileLayout::Single { length: 123 });
    }

    #[test]
    fn rejects_missing_announce() {
        let input = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces0:ee";
        let err = parse(input).unwrap_err();
        assert!(matches!(err, TorrentError::MissingField("announce")));
    }

    #[test]
    fn rejects_malformed_pieces_length() {
        // 'pieces' byte-string length (5) isn't a multiple of 20.
        let input = b"d8:announce4:test4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces5:abcdeee";
        let err = parse(input).unwrap_err();
        assert!(matches!(err, TorrentError::MalformedPieces));
    }

    #[test]
    fn generated_torrent_round_trips_through_parse() {
        // Write a small temp file with known, predictable content.
        let dir = std::env::temp_dir();
        let file_path = dir.join("bt_test_roundtrip.bin");
        let content = vec![0x42u8; 50_000]; // bigger than one piece to get >1 piece
        {
            let mut f = std::fs::File::create(&file_path).unwrap();
            f.write_all(&content).unwrap();
        }

        let piece_length = 16384; // 16 KiB, a realistic real-world piece size
        let torrent_bytes =
            create_single_file_torrent(&file_path, "http://tracker.test/announce", piece_length)
                .expect("torrent creation should succeed");

        let parsed = parse(&torrent_bytes).expect("generated torrent should parse cleanly");

        assert_eq!(parsed.announce, "http://tracker.test/announce");
        assert_eq!(parsed.name, "bt_test_roundtrip.bin");
        assert_eq!(parsed.piece_length, piece_length);
        assert_eq!(parsed.total_length(), 50_000);
        // 50000 / 16384 = 3 full pieces + 1 partial piece = 4 pieces total.
        assert_eq!(parsed.pieces.len(), 4);

        // Manually verify the first piece hash matches what we'd expect.
        let expected_first_hash: [u8; HASH_LEN] =
            Sha1::digest(&content[0..piece_length as usize]).into();
        assert_eq!(parsed.pieces[0], expected_first_hash);

        std::fs::remove_file(&file_path).ok();
    }

    #[test]
    fn info_hash_is_deterministic() {
        let dir = std::env::temp_dir();
        let file_path = dir.join("bt_test_hash_determinism.bin");
        std::fs::write(&file_path, b"hello world, this is a test file").unwrap();

        let bytes1 = create_single_file_torrent(&file_path, "http://a.test", 16384).unwrap();
        let bytes2 = create_single_file_torrent(&file_path, "http://a.test", 16384).unwrap();

        let t1 = parse(&bytes1).unwrap();
        let t2 = parse(&bytes2).unwrap();
        assert_eq!(t1.info_hash, t2.info_hash, "same content must yield same info_hash");

        std::fs::remove_file(&file_path).ok();
    }
}
//! # WAL — Write-Ahead Log
//!
//! Provides crash-safe durability for the RiptideKV storage engine.
//!
//! Every mutation (`PUT` or `DELETE`) is serialized into a binary record and
//! appended to the WAL **before** the corresponding in-memory update. On
//! restart the WAL is replayed to reconstruct the memtable, guaranteeing that
//! no acknowledged write is lost.
//!
//! ## Binary Record Format
//!
//! ```text
//! [record_len: u32 LE][crc32: u32 LE][body ...]
//! ```
//!
//! Body (Put): `[seq: u64][op=0: u8][key_len: u32][key][val_len: u32][value]`
//! Body (Del): `[seq: u64][op=1: u8][key_len: u32][key]`
//!
//! `record_len` includes the 4-byte CRC but **not** itself.
//!
//! ## Example
//!
//! ```rust,no_run
//! use wal::{WalWriter, WalReader, WalRecord};
//!
//! let mut w = WalWriter::create("wal.log", true).unwrap();
//! w.append(&WalRecord::Put {
//!     seq: 1,
//!     key: b"hello".to_vec(),
//!     value: b"world".to_vec(),
//! }).unwrap();
//! drop(w);
//!
//! let mut r = WalReader::open("wal.log").unwrap();
//! r.replay(|rec| println!("{:?}", rec)).unwrap();
//! ```

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use thiserror::Error;

/// A single WAL record representing either a key-value insertion or a deletion.
///
/// Each record carries a monotonically increasing **sequence number** that the
/// engine uses for ordering, conflict resolution, and (in later phases) snapshot reads
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// A key-value insertion.
    Put {
        /// Sequence number assigned by the engine.
        seq: u64,
        /// The lookup key.
        key: Vec<u8>,
        /// The payload value.
        value: Vec<u8>,
    },
    /// A key deletion (tombstone).
    Del {
        /// Sequence number assigned by the engine.
        seq: u64,
        /// The key to delete.
        key: Vec<u8>,
    },
}

/// Errors that can occur during WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// An underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// A record failed CRC validation or contained an unknown op code.
    #[error("corrupt record")]
    Corrupt,
}

/// Append-only WAL writer.
///
/// Records are serialized into an in-memory buffer, CRC-checksummed, and then
/// written to the underlying file in a single `write_all` call. When `sync` is
/// `true`, every append is followed by `sync_all()` (fsync) to guarantee the
/// record is durable on disk before the call returns.
pub struct WalWriter {
    file: File,
    sync: bool,
}

impl WalWriter {
    /// Opens (or creates) a WAL file in append mode.
    ///
    /// # Arguments
    ///
    /// * `path` - file system path for the WAL (created if it does not exist).
    /// * `sync` - if `true`, every `append` call is followed by `fsync`.
    ///   followed by `fsync`, trading throughput for durability
    pub fn create<P: AsRef<Path>>(path: P, sync: bool) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self { file, sync })
    }

    /// Serializes `record` and appends it to the WAL file.
    ///
    /// Layout:
    /// `[record_len: u32 LE][crc32: u32 LE][body bytes...]`
    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        let mut buf = Vec::new();
        // record body: seq(u64), op(u8), key_len(u32), key, [value_len(u32), value]
        match record {
            WalRecord::Put { seq, key, value } => {
                buf.write_u64::<LittleEndian>(*seq)?;
                buf.write_u8(0)?; // op = put
                buf.write_u32::<LittleEndian>(key.len() as u32)?;
                buf.extend_from_slice(key);
                buf.write_u32::<LittleEndian>(value.len() as u32)?;
                buf.extend_from_slice(value);
            }
            WalRecord::Del { seq, key } => {
                buf.write_u64::<LittleEndian>(*seq)?;
                buf.write_u8(1)?; // op = del
                buf.write_u32::<LittleEndian>(key.len() as u32)?;
                buf.extend_from_slice(key);
            }
        }

        // compute crc
        let mut hasher = Crc32::new();
        hasher.update(&buf);
        let crc = hasher.finalize();

        // full record length = body.len() + 4 (the CRC), must fit in u32
        let record_len = (buf.len() as u64) + 4;
        if record_len > (u32::MAX as u64) {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL record too large (exceeds u32::MAX bytes)",
            )));
        }

        // write header + crc + body
        self.file.write_u32::<LittleEndian>(record_len as u32)?;
        self.file.write_u32::<LittleEndian>(crc)?;
        self.file.write_all(&buf)?;
        self.file.flush()?;

        if self.sync {
            self.file.sync_all()?;
        }

        Ok(())
    }

    /// Forces all buffered data to be written to disk via `sync_all()`.
    ///
    /// Useful when `sync` is `false` (batched mode) and the caller wants to
    /// ensure durability at a specific point (e.g., before acknowledging a batch).
    pub fn sync_to_disk(&mut self) -> Result<(), WalError> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// Sequential WAL reader that yields valid records.
///
/// The reader is generic over any `Read` implementor, allowing it to be used
/// with real files (`WalReader<File>`) or in-memory buffers for testing.
///
/// During replay, each record's CRC32 is verified. A truncated tail record
/// (e.g., from a crash mid-write) is treated as a clean EOF — all fully-written
/// records before it are still returned.
pub struct WalReader<R: Read> {
    rdr: BufReader<R>,
}

impl WalReader<File> {
    /// Opens an existing WAL file for sequential replay.
    ///
    /// Returns `WalError::Io` if the file cannot be opened.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<WalReader<File>, WalError> {
        let f = File::open(path)?;
        Ok(WalReader {
            rdr: BufReader::new(f),
        })
    }
}

impl<R: Read> WalReader<R> {
    /// Constructs a reader from any `Read` implementor.
    ///
    /// Useful for unit tests that supply an in-memory buffer (e.g., `Cursor<Vec<u8>>`).
    pub fn from_reader(reader: R) -> Self {
        WalReader {
            rdr: BufReader::new(reader),
        }
    }

    /// Replays every valid record in the WAL, calling `apply` for each one.
    ///
    /// # Termination
    ///
    /// - **Clean EOF** (no more bytes) -> returns `Ok(())`.
    /// - **Truncated tail** (partial record at end, e.g., crash mid-write) ->
    ///   returns `Ok(())` after yielding all complete records before it.
    /// - **CRC mismatch** -> returns `Err(WalError::Corrupt)`.
    /// - **Unknown op code** -> returns `Err(WalError::Corrupt)`.
    /// - **I/O error** -> returns `Err(WalError::Io(...))`.
    pub fn replay<F>(&mut self, mut apply: F) -> Result<(), WalError>
    where
        F: FnMut(WalRecord),
    {
        loop {
            // read record_len
            let record_len = match self.rdr.read_u32::<LittleEndian>() {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(WalError::Io(e)),
            };

            // record_len includes CRC (4 bytes) but not itself
            // Reject absurd sizes → corruption
            const MAX_RECORD_SIZE: u32 = 64 * 1024 * 1024; // 64MB safety cap
            if record_len <= 4 || record_len > MAX_RECORD_SIZE {
                return Err(WalError::Corrupt);
            }

            // read crc (handle truncated tail)
            let crc = match self.rdr.read_u32::<LittleEndian>() {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(WalError::Io(e)),
            };

            // read body (record_len - 4 bytes)
            let body_len = (record_len - 4) as usize;
            let mut body = vec![0u8; body_len];
            match self.rdr.read_exact(&mut body) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // truncated tail — treat as EOF
                    return Ok(());
                }
                Err(e) => return Err(WalError::Io(e)),
            }

            // verify crc (only after we've successfully read the full body)
            let mut hasher = Crc32::new();
            hasher.update(&body);
            if hasher.finalize() != crc {
                return Err(WalError::Corrupt);
            }

            // parse body (single read)
            let mut br = &body[..];
            let seq = br.read_u64::<LittleEndian>()?;
            let op = br.read_u8()?;
            let key_len = br.read_u32::<LittleEndian>()?;
            let mut key = vec![0u8; key_len as usize];
            br.read_exact(&mut key)?;

            match op {
                0 => {
                    let val_len = br.read_u32::<LittleEndian>()?;
                    let mut val = vec![0u8; val_len as usize];
                    br.read_exact(&mut val)?;
                    apply(WalRecord::Put {
                        seq,
                        key,
                        value: val,
                    });
                }
                1 => {
                    apply(WalRecord::Del { seq, key });
                }
                _ => return Err(WalError::Corrupt),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Cursor;
    use tempfile::tempdir;

    // -------------------- Helpers --------------------

    fn make_put(seq: u64, key: &[u8], value: &[u8]) -> WalRecord {
        WalRecord::Put {
            seq,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn make_del(seq: u64, key: &[u8]) -> WalRecord {
        WalRecord::Del {
            seq,
            key: key.to_vec(),
        }
    }

    fn replay_all(path: &std::path::Path) -> Result<Vec<WalRecord>, WalError> {
        let mut reader = WalReader::open(path)?;
        let mut recs = Vec::new();
        reader.replay(|r| recs.push(r))?;
        Ok(recs)
    }

    fn replay_from_bytes(data: &[u8]) -> Result<Vec<WalRecord>, WalError> {
        let cursor = Cursor::new(data.to_vec());
        let mut reader = WalReader::from_reader(cursor);
        let mut recs = Vec::new();
        reader.replay(|r| recs.push(r))?;
        Ok(recs)
    }

    // -------------------- Basic write & replay --------------------

    #[test]
    fn write_and_replay_put_and_del() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"k", b"v1")).unwrap();
            w.append(&make_put(2, b"k2", b"v2")).unwrap();
            w.append(&make_del(3, b"k")).unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(
            recs,
            vec![
                make_put(1, b"k", b"v1"),
                make_put(2, b"k2", b"v2"),
                make_del(3, b"k"),
            ]
        );
    }

    // -------------------- Stress tests --------------------

    #[test]
    fn many_records_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let n = 5_000usize;
        {
            let mut w = WalWriter::create(&path, false).unwrap();
            for i in 0..n {
                let key = format!("key{}", i).into_bytes();
                let val = format!("val{}", i).into_bytes();
                w.append(&WalRecord::Put {
                    seq: i as u64,
                    key,
                    value: val,
                })
                .unwrap();
            }
            w.sync_to_disk().unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), n);
        for (i, rec) in recs.iter().enumerate() {
            let expected_key = format!("key{}", i).into_bytes();
            let expected_val = format!("val{}", i).into_bytes();
            assert_eq!(
                rec,
                &WalRecord::Put {
                    seq: i as u64,
                    key: expected_key,
                    value: expected_val,
                }
            );
        }
    }

    #[test]
    fn interleaved_puts_and_dels() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, false).unwrap();
            for i in 0u64..1000 {
                if i % 3 == 0 {
                    w.append(&make_del(i, format!("k{}", i).as_bytes()))
                        .unwrap();
                } else {
                    w.append(&make_put(i, format!("k{}", i).as_bytes(), b"v"))
                        .unwrap();
                }
            }
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 1000);

        let del_count = recs
            .iter()
            .filter(|r| matches!(r, WalRecord::Del { .. }))
            .count();
        let put_count = recs.len() - del_count;
        // 0,3,6,...,999 -> ceil(1000/3) = 334
        assert_eq!(del_count, 334);
        assert_eq!(put_count, 666);
    }

    // -------------------- Edge tests --------------------

    #[test]
    fn seq_zero_and_max() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(0, b"min", b"v")).unwrap();
            w.append(&make_put(u64::MAX, b"max", b"v")).unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 2);
        if let WalRecord::Put { seq, .. } = &recs[0] {
            assert_eq!(*seq, 0);
        } else {
            panic!("expected Put");
        }
        if let WalRecord::Put { seq, .. } = &recs[1] {
            assert_eq!(*seq, u64::MAX);
        } else {
            panic!("expected Put");
        }
    }

    #[test]
    fn from_reader_in_memory() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"k", b"v")).unwrap();
            w.append(&make_del(2, b"k")).unwrap();
        }

        let data = fs::read(&path).unwrap();
        let recs = replay_from_bytes(&data).unwrap();
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn binary_key_and_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let key = vec![0x00u8, 0xFF, 0x80];
        let val = vec![0xDEu8, 0xAD, 0xBE, 0xEF];

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: key.clone(),
                value: val.clone(),
            })
            .unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 1);
        if let WalRecord::Put {
            seq,
            key: k,
            value: v,
        } = &recs[0]
        {
            assert_eq!(*seq, 1);
            assert_eq!(k, &key);
            assert_eq!(v, &val);
        } else {
            panic!("expected Put");
        }
    }

    #[test]
    fn large_value_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let big_val = vec![b'x'; 1_000_000]; // 1 MB

        {
            let mut w = WalWriter::create(&path, false).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: b"big".to_vec(),
                value: big_val.clone(),
            })
            .unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 1);
        if let WalRecord::Put { value, .. } = &recs[0] {
            assert_eq!(value.len(), 1_000_000);
        } else {
            panic!("expected Put");
        }
    }

    #[test]
    fn truncated_body_after_crc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"k", b"v")).unwrap();
        }

        // Append a partial record: record_len + crc but truncated body
        let mut data = fs::read(&path).unwrap();
        // record_len = 32 (0x20 0x00 0x00 0x00)
        data.extend_from_slice(&[0x20, 0x00, 0x00, 0x00]);
        // crc (4 bytes)
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
        // partial body (too short)
        data.extend_from_slice(&[0x01, 0x02]);
        fs::write(&path, &data).unwrap();

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0], make_put(1, b"k", b"v"));
    }

    #[test]
    fn append_to_existing_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"a", b"1")).unwrap();
        }
        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(2, b"b", b"2")).unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0], make_put(1, b"a", b"1"));
        assert_eq!(recs[1], make_put(2, b"b", b"2"));
    }

    #[test]
    fn sync_to_disk_does_not_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut w = WalWriter::create(&path, false).unwrap();
        w.append(&make_put(1, b"k", b"v")).unwrap();
        w.sync_to_disk().unwrap();
    }

    #[test]
    fn empty_key_and_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"", b"")).unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs, vec![make_put(1, b"", b"")]);
    }

    // -------------------- Corruption detection --------------------

    #[test]
    fn corrupt_crc_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"k", b"v")).unwrap();
        }

        // Flip a byte in the body to corrupt the CRC
        let mut data = fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = replay_all(&path);
        assert!(matches!(result, Err(WalError::Corrupt)));
    }

    #[test]
    fn crc_mismatch_is_corruption() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u64.to_le_bytes()); // seq
        body.push(0); // op = Put
        body.extend_from_slice(&1u32.to_le_bytes()); // key_len
        body.extend_from_slice(b"k");
        body.extend_from_slice(&1u32.to_le_bytes()); // val_len
        body.extend_from_slice(b"v");

        let record_len = (body.len() + 4) as u32;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&record_len.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // WRONG CRC
        bytes.extend_from_slice(&body);

        let result = replay_from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn corrupt_record_len_zero() {
        // record_len = 0 is invalid (must be > 4 for CRC)
        let data: Vec<u8> = vec![0, 0, 0, 0];
        let result = replay_from_bytes(&data);
        assert!(matches!(result, Err(WalError::Corrupt)));
    }

    #[test]
    fn corrupt_record_len_too_small() {
        // record_len = 3 is invalid (must be > 4)
        let data: Vec<u8> = vec![3, 0, 0, 0];
        let result = replay_from_bytes(&data);
        assert!(matches!(result, Err(WalError::Corrupt)));
    }

    // -------------------- Truncated tail tolerance --------------------

    #[test]
    fn truncated_tail_after_valid_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(1, b"k1", b"v1")).unwrap();
            w.append(&make_put(2, b"k2", b"v2")).unwrap();
        }

        // Append a partial record (just the record_len header, no body)
        let mut data = fs::read(&path).unwrap();
        data.extend_from_slice(&[0x20, 0x00, 0x00, 0x00]); // record_len = 32
                                                           // No CRC or body follows — simulates crash mid-write
        fs::write(&path, &data).unwrap();

        // Should recover the two valid records and ignore the truncated tail
        let recs = replay_all(&path).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0], make_put(1, b"k1", b"v1"));
        assert_eq!(recs[1], make_put(2, b"k2", b"v2"));
    }

    // -------------------- Single-roundtrip helpers --------------------

    #[test]
    fn single_put_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_put(42, b"hello", b"world")).unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs, vec![make_put(42, b"hello", b"world")]);
    }

    #[test]
    fn single_del_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&make_del(7, b"gone")).unwrap();
        }

        let recs = replay_all(&path).unwrap();
        assert_eq!(recs, vec![make_del(7, b"gone")]);
    }

    // -------------------- Empty WAL --------------------

    #[test]
    fn replay_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        fs::write(&path, b"").unwrap();

        let recs = replay_all(&path).unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn replay_empty_in_memory() {
        let recs = replay_from_bytes(b"").unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn truncated_tail_is_ok() {
        let result = replay_from_bytes(&[0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(result.is_ok());
    }

    // -------------------- File Not Found --------------------
    #[test]
    fn open_non_existent_file_return_error() {
        let result = WalReader::open("/tmp/non_existent_wal.log");
        assert!(matches!(result, Err(WalError::Io(_))));
    }
}

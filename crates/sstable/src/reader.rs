use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use memtable::ValueEntry;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::format::{read_footer, SSTABLE_MAGIC};

/// Reads an SSTable file for point lookups.
///
/// On [`open`](SSTableReader::open) the entire **index** is loaded into memory
/// as a `BTreeMap<Vec<u8>, u64>` (key → data-section byte offset). Point
/// lookups then require only a single disk seek + read per call.
///
/// The data file itself is **not** kept open between lookups — each
/// [`get`](SSTableReader::get) call opens the file, seeks, reads the record,
/// and closes the handle. This keeps ownership simple and avoids holding
/// long-lived file descriptors.
pub struct SSTableReader {
    /// Path to the `.sst` file on disk.
    path: PathBuf,
    /// In-memory index mapping each key to its byte offset in the data section.
    index: BTreeMap<Vec<u8>, u64>,
}

impl SSTableReader {
    /// Opens an SSTable file and loads its index into memory.
    ///
    /// # Validation
    ///
    /// - The file must be at least 12 bytes (footer size).
    /// - The footer magic must equal `0x5353_5431` ("SST1").
    /// - The `index_offset` must point inside the file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is too small, the magic is wrong, or any
    /// I/O operation fails.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut f = File::open(&path_buf)?;
        let metadata = f.metadata()?;
        let filesize = metadata.len();

        if filesize < crate::format::FOOTER_BYTES {
            bail!("sstable file too small");
        }

        // read footer via helper
        let (index_offset, magic) = read_footer(&mut f)?;
        if magic != SSTABLE_MAGIC {
            bail!("invalid sstable magic: {:x}", magic);
        }
        if index_offset >= filesize {
            bail!("invalid index_offset");
        }

        // Read index entries from index_offset up to footer start
        f.seek(SeekFrom::Start(index_offset))?;
        let mut index = BTreeMap::new();

        // Read until we reach footer (filesize - FOOTER_BYTES)
        while f.stream_position()? < (filesize - crate::format::FOOTER_BYTES) {
            // key_len (u32) + key bytes + data_offset (u64)
            let key_len = f.read_u32::<LittleEndian>()? as usize;
            let mut key = vec![0u8; key_len];
            f.read_exact(&mut key)?;
            let data_offset = f.read_u64::<LittleEndian>()?;
            index.insert(key, data_offset);
        }

        Ok(Self {
            path: path_buf,
            index,
        })
    }

    /// Point lookup for a single key.
    ///
    /// Returns `Ok(Some(entry))` if the key exists in this SSTable (the entry
    /// may be a tombstone with `value: None`). Returns `Ok(None)` if the key
    /// is not present in the index.
    ///
    /// Each call opens the file, seeks to the record offset, reads and parses
    /// the record, then closes the file handle.
    ///
    /// # Errors
    ///
    /// Returns an error on I/O failure or if the on-disk key does not match
    /// the requested key (index corruption).
    pub fn get(&self, key: &[u8]) -> Result<Option<ValueEntry>> {
        let maybe_offset = self.index.get(key);
        if maybe_offset.is_none() {
            return Ok(None);
        }
        let offset = *maybe_offset.unwrap();

        // Open file each time to keep API & ownership simple and avoid mutable File in struct.
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;

        // Parse record at offset:
        // u32 key_len
        // key bytes
        // u64 seq
        // u8 value_present
        // if present == 1:
        //   u32 value_len
        //   value bytes
        let key_len = f.read_u32::<LittleEndian>()? as usize;
        let mut key_buf = vec![0u8; key_len];
        f.read_exact(&mut key_buf)?;

        // Sanity: the key read should match the requested key
        if key_buf.as_slice() != key {
            bail!("index pointed to mismatching key at offset");
        }

        let seq = f.read_u64::<LittleEndian>()?;
        let present = f.read_u8()?;
        if present == 1 {
            let val_len = f.read_u32::<LittleEndian>()? as usize;
            let mut val = vec![0u8; val_len];
            f.read_exact(&mut val)?;
            Ok(Some(ValueEntry {
                seq,
                value: Some(val),
            }))
        } else {
            Ok(Some(ValueEntry { seq, value: None }))
        }
    }

    /// Returns the number of entries in the in-memory index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns `true` if the SSTable contains zero entries.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Returns an iterator over all keys in the in-memory index.
    ///
    /// Keys are yielded in ascending sorted order (guaranteed by `BTreeMap`).
    ///
    /// Useful for debugging, testing, and future range-scan support.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.index.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SSTableWriter;
    use memtable::Memtable;
    use tempfile::tempdir;

    fn make_sample_memtable() -> Memtable {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"apple".to_vec(), 1);
        m.put(b"b".to_vec(), b"banana".to_vec(), 2);
        m.put(b"c".to_vec(), b"".to_vec(), 3);
        m.delete(b"d".to_vec(), 4);
        m
    }

    // -------------------- Basic open & get --------------------

    #[test]
    fn open_and_get_entries() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("sample.sst");

        let mem = make_sample_memtable();
        SSTableWriter::write_from_memtable(&path, &mem)?;
        let reader = SSTableReader::open(&path)?;

        // Check keys exist in index
        let keys: Vec<_> = reader.keys().cloned().collect();
        assert!(keys.contains(&b"a".to_vec()));
        assert!(keys.contains(&b"b".to_vec()));
        assert!(keys.contains(&b"c".to_vec()));
        assert!(keys.contains(&b"d".to_vec()));

        // Get 'a'
        let a = reader.get(b"a")?.expect("a must exist");
        assert_eq!(a.seq, 1);
        assert_eq!(a.value, Some(b"apple".to_vec()));

        // Get 'b'
        let b = reader.get(b"b")?.expect("b must exist");
        assert_eq!(b.seq, 2);
        assert_eq!(b.value, Some(b"banana".to_vec()));

        // Get 'c' (empty but present)
        let c = reader.get(b"c")?.expect("c must exist");
        assert_eq!(c.seq, 3);
        assert_eq!(c.value, Some(b"".to_vec()));

        // Get 'd' (tombstone)
        let d = reader.get(b"d")?.expect("d must exist");
        assert_eq!(d.seq, 4);
        assert_eq!(d.value, None);

        // Non-existent key
        assert!(reader.get(b"nope")?.is_none());

        Ok(())
    }

    // -------------------- len / is_empty --------------------

    #[test]
    fn len_and_is_empty() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("len.sst");

        let mem = make_sample_memtable();
        SSTableWriter::write_from_memtable(&path, &mem)?;

        let reader = SSTableReader::open(&path)?;
        assert_eq!(reader.len(), 4);
        assert!(!reader.is_empty());
        Ok(())
    }

    // -------------------- Validation errors --------------------

    #[test]
    fn open_file_too_small() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tiny.sst");
        std::fs::write(&path, b"short").unwrap();

        let result = SSTableReader::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn open_bad_magic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("badmagic.sst");

        // 12 bytes: 8 for index_offset + 4 for wrong magic
        let mut data = vec![0u8; 8]; // index_offset = 0
        data.extend_from_slice(&[0xBA, 0xAD, 0xF0, 0x0D]); // wrong magic
        std::fs::write(&path, &data).unwrap();

        let result = SSTableReader::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn open_nonexistent_file() {
        let result = SSTableReader::open("/tmp/no_such_file_riptide.sst");
        assert!(result.is_err());
    }

    // -------------------- Keys iterator ordering --------------------

    #[test]
    fn keys_are_sorted() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("sorted.sst");

        let mut mem = Memtable::new();
        mem.put(b"z".to_vec(), b"1".to_vec(), 1);
        mem.put(b"a".to_vec(), b"2".to_vec(), 2);
        mem.put(b"m".to_vec(), b"3".to_vec(), 3);
        SSTableWriter::write_from_memtable(&path, &mem)?;

        let reader = SSTableReader::open(&path)?;
        let keys: Vec<_> = reader.keys().cloned().collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]);
        Ok(())
    }

    // -------------------- Multiple gets on same reader --------------------

    #[test]
    fn multiple_gets_same_reader() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("multi.sst");

        let mut mem = Memtable::new();
        for i in 0..100u64 {
            mem.put(format!("k{:03}", i).into_bytes(), b"v".to_vec(), i);
        }
        SSTableWriter::write_from_memtable(&path, &mem)?;

        let reader = SSTableReader::open(&path)?;
        // Read all keys twice to ensure re-opening the file works
        for _ in 0..2 {
            for i in 0..100u64 {
                let key = format!("k{:03}", i).into_bytes();
                let entry = reader.get(&key)?.unwrap();
                assert_eq!(entry.seq, i);
            }
        }

        Ok(())
    }

    // -------------------- Large values --------------------

    #[test]
    fn large_value_roundtrip() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("bigval.sst");

        let mut mem = Memtable::new();
        let big = vec![b'x'; 500_000];
        mem.put(b"big".to_vec(), big.clone(), 1);
        SSTableWriter::write_from_memtable(&path, &mem)?;

        let reader = SSTableReader::open(&path)?;
        let entry = reader.get(b"big")?.unwrap();
        assert_eq!(entry.value.unwrap().len(), 500_000);
        Ok(())
    }
}

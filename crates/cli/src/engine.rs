/// Storage engine that ties together the Memtable, WAL, and SSTable layers.
use anyhow::Result;
use memtable::Memtable;
use sstable::SSTableReader;
use sstable::SSTableWriter;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use wal::{WalReader, WalRecord, WalWriter};

/// Replays a WAL file into the given memtable, returning the highest sequence
/// number encountered.
///
/// If the WAL file does not exist, returns `Ok(0)` (fresh start).
///
/// # Errors
///
/// Propagates any I/O or corruption error from [`WalReader::replay`].
pub fn replay_wal_and_build(path: &str, mem: &mut Memtable) -> Result<u64> {
    if let Ok(mut reader) = WalReader::open(path) {
        let mut max_seq = 0u64;

        reader.replay(|r| match r {
            WalRecord::Put { seq, key, value } => {
                mem.put(key, value, seq);
                max_seq = max_seq.max(seq);
            }
            WalRecord::Del { seq, key } => {
                mem.delete(key, seq);
                max_seq = max_seq.max(seq);
            }
        })?;

        Ok(max_seq)
    } else {
        Ok(0)
    }
}

/// The central storage engine orchestrating Memtable, WAL, and SSTables.
///
/// # Write Path
///
/// 1. Increment the monotonic sequence number.
/// 2. Append the record to the WAL (crash-safe durability).
/// 3. Apply the mutation to the in-memory Memtable.
/// 4. If `approx_size >= flush_threshold`, flush the Memtable to a new SSTable,
///    truncate the WAL, and reset the Memtable.
///
/// # Read Path
///
/// 1. Check the Memtable (freshest data, includes tombstones).
/// 2. Check SSTables from newest to oldest.
/// 3. First match wins; tombstones shadow older values.
///
/// # Recovery
///
/// On construction ([`Engine::new`]), the WAL is replayed into a fresh Memtable
/// and existing `.sst` files are loaded from the SST directory.
pub struct Engine {
    mem: Memtable,
    sstables: Vec<SSTableReader>,
    wal_path: PathBuf,
    sst_dir: PathBuf,
    wal_writer: WalWriter,

    /// Current monotonic sequence number.
    pub seq: u64,

    /// Memtable byte-size threshold that triggers a flush to SSTable.
    pub flush_threshold: usize,

    /// If `true`, every WAL append is followed by `fsync` for durability.
    pub wal_sync: bool,
}

impl Engine {
    /// Creates a new engine, performing full recovery from the WAL and existing
    /// SSTable files.
    ///
    /// # Arguments
    ///
    /// * `wal_path` — path to the write-ahead log file.
    /// * `sst_dir` — directory where SSTable files are stored.
    /// * `flush_threshold` — memtable byte-size threshold that triggers flush.
    /// * `wal_sync` — if `true`, every WAL append calls `fsync`.
    ///
    /// # Recovery Steps
    ///
    /// 1. Create the SST directory if it does not exist.
    /// 2. Open the WAL writer in append mode.
    /// 3. Replay the WAL into a fresh Memtable.
    /// 4. Scan the SST directory for `.sst` files, sorted newest-first.
    /// 5. Open each SSTable and load its index into memory.
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        sst_dir: P,
        flush_threshold: usize,
        wal_sync: bool,
    ) -> Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let sst_dir = sst_dir.as_ref().to_path_buf();

        // ensure sst dir exists
        std::fs::create_dir_all(&sst_dir)?;

        // create/open wal writer (we will use create to append)
        let wal_writer = WalWriter::create(&wal_path, wal_sync)?;

        // replay wal into memtable and obtain last seq
        let mut mem = Memtable::new();
        let seq = replay_wal_and_build(wal_path.to_str().unwrap(), &mut mem)?;

        let mut sstables = Vec::new();

        let mut paths: Vec<_> = std::fs::read_dir(&sst_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "sst").unwrap_or(false))
            .collect();

        // newest first (filename contains seq + timestamp)
        paths.sort();
        paths.reverse();

        for path in paths {
            sstables.push(SSTableReader::open(&path)?);
        }

        Ok(Self {
            mem,
            sstables,
            wal_path,
            sst_dir,
            wal_writer,
            seq,
            flush_threshold,
            wal_sync,
        })
    }

    /// Inserts a key-value pair (the `SET` command).
    ///
    /// The operation is first appended to the WAL, then applied to the
    /// Memtable. If the Memtable exceeds the flush threshold, it is
    /// automatically flushed to a new SSTable.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.seq = self.seq.saturating_add(1);
        let seq = self.seq;

        // Append to WAL first
        self.wal_writer.append(&WalRecord::Put {
            seq,
            key: key.clone(),
            value: value.clone(),
        })?;

        // Apply to memtable
        self.mem.put(key, value, seq);

        // Maybe flush memtable to SSTable
        if self.mem.approx_size() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Looks up a key, returning `Some((seq, value))` if found and live.
    ///
    /// The read path checks the Memtable first, then SSTables from newest to
    /// oldest. Tombstones in any layer shadow older values, causing `None` to
    /// be returned.
    pub fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        // 1. Check memtable FIRST (and respect tombstones)
        if let Some(entry) = self.mem.get_entry(key) {
            return entry.value.as_ref().map(|v| (entry.seq, v.clone()));
        }

        // 2. Check SSTables (newest -> oldest)
        for sst in &self.sstables {
            match sst.get(key) {
                Ok(Some(entry)) => {
                    return match entry.value {
                        Some(v) => Some((entry.seq, v)),
                        None => None, // tombstone hides older values
                    };
                }
                Ok(None) => continue,
                Err(_) => continue,
            }
        }

        // 3. Not found anywhere
        None
    }

    /// Deletes a key by writing a tombstone (the `DEL` command).
    ///
    /// A tombstone record is appended to the WAL and inserted into the
    /// Memtable. The tombstone shadows any older value in SSTables.
    pub fn del(&mut self, key: Vec<u8>) -> Result<()> {
        self.seq = self.seq.saturating_add(1);
        let seq = self.seq;

        self.wal_writer.append(&WalRecord::Del {
            seq,
            key: key.clone(),
        })?;

        self.mem.delete(key, seq);

        if self.mem.approx_size() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(())
    }

    /// Flushes the current Memtable to a new SSTable, truncates the WAL, and
    /// resets the Memtable.
    ///
    /// # Steps
    ///
    /// 1. Generate a unique filename: `sst-{seq}-{timestamp_ms}.sst`.
    /// 2. Write the SSTable via [`SsTableWriter::write_from_memtable`]
    ///    (atomic temp + rename).
    /// 3. Truncate the WAL to zero bytes.
    /// 4. Create a fresh [`WalWriter`] in append mode.
    /// 5. Replace the Memtable with an empty one.
    /// 6. Open the new SSTable and insert it at position 0 (newest).
    fn flush(&mut self) -> Result<()> {
        // choose filename using current seq and timestamp so it's monotonic
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        let sst_name = format!("sst-{}-{}.sst", self.seq, ts);
        let sst_path = self.sst_dir.join(&sst_name);

        // write sstable (this writes to temp and rename inside)
        SSTableWriter::write_from_memtable(&sst_path, &self.mem)?;

        // Successfully wrote SSTable; now safely truncate the WAL
        // We ensure the SSTable is fsynced by the writer; now we can truncate wal.log
        // Truncate by opening with truncate(true)
        let _f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.wal_path)?;
        // (dropping _f closes it)

        // create a fresh WalWriter (append mode)
        self.wal_writer = WalWriter::create(&self.wal_path, self.wal_sync)?;

        // reset memtable
        self.mem = Memtable::new();

        let reader = SSTableReader::open(&sst_path)?;
        self.sstables.insert(0, reader);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;
    use std::path::Path;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    // ---------------------- Helpers ----------------------
    fn count_sst_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| ext == "sst")
                    .unwrap_or(false)
            })
            .count()
    }

    // ---------------------- Basic set / get / del ----------------------

    #[test]
    fn set_and_get() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1024 * 1024,
            false,
        )?;

        engine.set(b"name".to_vec(), b"alice".to_vec())?;
        let (seq, val) = engine.get(b"name").unwrap();
        assert_eq!(seq, 1);
        assert_eq!(val, b"alice");
        Ok(())
    }

    #[test]
    fn get_missing_key() -> Result<()> {
        let dir = tempdir()?;
        let engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1024 * 1024,
            false,
        )?;
        assert!(engine.get(b"nope").is_none());
        Ok(())
    }

    #[test]
    fn del_removes_key() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1024 * 1024,
            false,
        )?;

        engine.set(b"k".to_vec(), b"v".to_vec())?;
        assert!(engine.get(b"k").is_some());

        engine.del(b"k".to_vec())?;
        assert!(engine.get(b"k").is_none());
        Ok(())
    }

    #[test]
    fn overwrite_key() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1024 * 1024,
            false,
        )?;

        engine.set(b"k".to_vec(), b"v1".to_vec())?;
        engine.set(b"k".to_vec(), b"v2".to_vec())?;
        assert_eq!(engine.get(b"k").unwrap().1, b"v2".to_vec());
        Ok(())
    }

    #[test]
    fn set_after_del_resurrects() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1024 * 1024,
            false,
        )?;

        engine.set(b"k".to_vec(), b"v1".to_vec())?;
        engine.del(b"k".to_vec())?;
        engine.set(b"k".to_vec(), b"v2".to_vec())?;
        assert_eq!(engine.get(b"k").unwrap().1, b"v2".to_vec());
        Ok(())
    }

    // ---------------------- Flush mechanics ----------------------

    #[test]
    fn flush_writes_sstable_and_truncates_wal() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;
        engine.set(b"key1".to_vec(), b"value1".to_vec())?;

        assert!(
            count_sst_files(&sst_dir) >= 1,
            "expected at least one .sst file"
        );

        let wal_meta = fs::metadata(&wal_path)?;
        assert_eq!(wal_meta.len(), 0, "expected wal to be truncated to 0 bytes");
        Ok(())
    }

    #[test]
    fn flush_triggers_at_threshold() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");
        let threshold = 4 * 1024; // 4 KB for fast test

        let mut engine = Engine::new(&wal_path, &sst_dir, threshold, false)?;

        let value = vec![b'x'; 512];
        let writes = (threshold / value.len()) + 5;
        for i in 0..writes {
            engine.set(format!("key{}", i).into_bytes(), value.clone())?;
        }

        assert!(
            count_sst_files(&sst_dir) >= 1,
            "expected at least one SSTable after crossing threshold"
        );
        Ok(())
    }

    // ---------------------- Read from SSTables after flush ----------------------

    #[test]
    fn get_reads_from_sstable_after_flush() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1, // tiny threshold - every set triggers flush
            false,
        )?;

        engine.set(b"k1".to_vec(), b"v1".to_vec())?;
        // After flush, memtable is empty; k1 is only in SSTable
        assert_eq!(engine.get(b"k1").unwrap().1, b"v1".to_vec());
        Ok(())
    }

    #[test]
    fn tombstone_in_sstable_shadows_older_value() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        // Large threshold so we control flushes manually
        let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, false)?;

        // Write k=v, then force flush by lowering threshold temporarily
        engine.set(b"k".to_vec(), b"old_value".to_vec())?;
        engine.flush_threshold = 1;
        // Trigger flush with a dummy write
        engine.set(b"dummy".to_vec(), b"x".to_vec())?;

        // Now SSTable #1 has {k: old_value, dummy: x}
        // Reset threshold high
        engine.flush_threshold = 1024 * 1024;

        // Delete k (goes into memtable as tombstone)
        engine.del(b"k".to_vec())?;

        // Memtable tombstone should shadow SSTable value
        assert!(engine.get(b"k").is_none());
        Ok(())
    }

    // ---------------------- Recovery ----------------------

    #[test]
    fn recovery_from_wal() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        // Write some data, then drop engine (simulates crash)
        {
            let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
            engine.set(b"a".to_vec(), b"1".to_vec())?;
            engine.set(b"b".to_vec(), b"2".to_vec())?;
            engine.del(b"a".to_vec())?;
        }

        // Reopen engine - should replay WAL
        let engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
        assert!(engine.get(b"a").is_none()); // deleted
        assert_eq!(engine.get(b"b").unwrap().1, b"2".to_vec());
        assert_eq!(engine.seq, 3); // 3 operations
        Ok(())
    }

    #[test]
    fn recovery_from_sstables() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        // Write data and force flush
        {
            let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;
            engine.set(b"k".to_vec(), b"v".to_vec())?;
            // Flush happened due to threshold=1
        }

        // Reopen - WAL is empty but SSTable has the data
        let engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
        assert_eq!(engine.get(b"k").unwrap().1, b"v".to_vec());
        Ok(())
    }

    #[test]
    fn recovery_combines_wal_and_sstables() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        // Create an engine that flushes immediately
        {
            let mut engine = Engine::new(&wal_path, &sst_dir, 1, true)?;
            // This triggers flush (threshold=1)
            engine.set(b"flushed".to_vec(), b"in_sst".to_vec())?;
        }

        {
            // Reopen with high threshold so next writes stay in WAL
            let mut engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
            engine.set(b"in_wal".to_vec(), b"pending".to_vec())?;
        }

        // Final reopen - should have both
        let engine = Engine::new(&wal_path, &sst_dir, 1024 * 1024, true)?;
        assert_eq!(engine.get(b"flushed").unwrap().1, b"in_sst".to_vec());
        assert_eq!(engine.get(b"in_wal").unwrap().1, b"pending".to_vec());
        Ok(())
    }

    // ---------------------- Multiple flushes ----------------------

    #[test]
    fn multiple_flushes_create_multiple_sstables() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        let mut engine = Engine::new(&wal_path, &sst_dir, 1, false)?;

        for i in 0..5u64 {
            engine.set(format!("k{}", i).into_bytes(), b"v".to_vec())?;
            // Each set triggers a flush due to threshold=1
            // Small sleep to ensure unique timestamps in filenames
            thread::sleep(Duration::from_millis(2));
        }

        let sst_count = count_sst_files(&sst_dir);
        assert!(
            sst_count >= 5,
            "expected multiple SSTable files, got {}",
            sst_count
        );

        // All keys should be readable
        for i in 0..5u64 {
            let key = format!("k{}", i).into_bytes();
            assert!(engine.get(&key).is_some(), "key {} should be readable", i);
        }
        Ok(())
    }

    #[test]
    fn newest_sstable_wins_on_read() -> Result<()> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("wal.log");
        let sst_dir = dir.path().join("sst");

        let mut engine = Engine::new(&wal_path, &sst_dir, 1, false)?;

        // Write k=v1, flush
        engine.set(b"k".to_vec(), b"v1".to_vec())?;
        thread::sleep(Duration::from_millis(2));

        // Write k=v2, flush (newer SSTable)
        engine.set(b"k".to_vec(), b"v2".to_vec())?;

        // Should read v2 from the newest SSTable
        assert_eq!(engine.get(b"k").unwrap().1, b"v2".to_vec());
        Ok(())
    }

    // ---------------------- Sequence number ----------------------

    #[test]
    fn seq_increments_on_every_operation() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            1024 * 1024,
            false,
        )?;

        assert_eq!(engine.seq, 0);
        engine.set(b"a".to_vec(), b"1".to_vec())?;
        assert_eq!(engine.seq, 1);
        engine.set(b"b".to_vec(), b"2".to_vec())?;
        assert_eq!(engine.seq, 2);
        engine.del(b"a".to_vec())?;
        assert_eq!(engine.seq, 3);
        Ok(())
    }

    // ---------------------- Stress ----------------------

    #[test]
    fn many_keys_with_flushes() -> Result<()> {
        let dir = tempdir()?;
        let mut engine = Engine::new(
            dir.path().join("wal.log"),
            dir.path().join("sst"),
            4096, // 4 KB threshold
            false,
        )?;

        for i in 0..500u64 {
            let key = format!("key{:04}", i).into_bytes();
            let val = vec![b'v'; 64];
            engine.set(key, val)?;
        }

        // Verify all keys readable
        for i in 0..500u64 {
            let key = format!("key{:04}", i).into_bytes();
            assert!(
                engine.get(&key).is_some(),
                "key:{:04} should be readable",
                i
            );
        }

        // Delete half
        for i in (0..500u64).step_by(2) {
            let key = format!("key{:04}", i).into_bytes();
            engine.del(key)?;
        }

        // Verify deletes
        for i in 0..500u64 {
            let key = format!("key{:04}", i).into_bytes();
            if i % 2 == 0 {
                assert!(engine.get(&key).is_none(), "key:{:04} should be deleted", i);
            } else {
                assert!(
                    engine.get(&key).is_some(),
                    "key:{:04} should still exist",
                    i
                );
            }
        }

        Ok(())
    }
}

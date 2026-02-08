use anyhow::Result;
use memtable::Memtable;
use wal::{WalReader, WalRecord};

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

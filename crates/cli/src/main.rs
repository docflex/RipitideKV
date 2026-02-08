use anyhow::Result;
use memtable::Memtable;
use std::io::{self, BufRead, Write};
use wal::{WalRecord, WalWriter};

mod engine;

use engine::replay_wal_and_build;

fn main() -> Result<()> {
    let wal_path = "wal.log";
    let mut mem = Memtable::new();
    let mut seq: u64 = replay_wal_and_build(wal_path, &mut mem)?;

    println!("kv-cli started. Replayed WAL, current seq={}", seq);
    println!("Commands: SET key value | GET key | DEL key | EXIT");

    let stdin = io::stdin();
    let mut wal_writer = WalWriter::create(wal_path, true)?;

    for line in stdin.lock().lines() {
        let line = line?;
        let mut parts = line.split_whitespace();
        if let Some(cmd) = parts.next() {
            match cmd.to_uppercase().as_str() {
                "SET" => {
                    if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                        seq += 1;
                        let key = k.as_bytes().to_vec();
                        let val = v.as_bytes().to_vec();
                        wal_writer.append(&WalRecord::Put {
                            seq,
                            key: key.clone(),
                            value: val.clone(),
                        })?;
                        mem.put(key, val, seq);
                        println!("OK");
                    } else {
                        println!("ERR usage: SET key value");
                    }
                }
                "GET" => {
                    if let Some(k) = parts.next() {
                        if let Some((_s, v)) = mem.get(k.as_bytes()) {
                            println!("{}", String::from_utf8_lossy(&v));
                        } else {
                            println!("(nil)");
                        }
                    } else {
                        println!("ERR usage: GET key");
                    }
                }
                "DEL" => {
                    if let Some(k) = parts.next() {
                        seq += 1;
                        let key = k.as_bytes().to_vec();
                        wal_writer.append(&WalRecord::Del {
                            seq,
                            key: key.clone(),
                        })?;
                        mem.delete(key, seq);
                        println!("OK");
                    } else {
                        println!("ERR usage: DEL key");
                    }
                }
                "EXIT" => {
                    println!("bye");
                    break;
                }
                other => {
                    println!("unknown command: {}", other);
                }
            }
        }
        print!("> ");
        io::stdout().flush().ok();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::engine::replay_wal_and_build;
    use memtable::Memtable;
    use wal::{WalRecord, WalWriter};

    #[test]
    fn wal_replay_rebuilds_memtable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Del {
                seq: 2,
                key: b"a".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Put {
                seq: 3,
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            })
            .unwrap();
        }

        let mut mem = Memtable::new();
        let max_seq = replay_wal_and_build(path.to_str().unwrap(), &mut mem).unwrap();

        assert_eq!(max_seq, 3);
        assert!(mem.get(b"a").is_none());
        assert_eq!(mem.get(b"b").unwrap().1, b"2");
    }

    #[test]
    fn wal_durability_without_memtable_update() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap();
            // crash here: memtable never updated
        }

        let mut mem = Memtable::new();
        replay_wal_and_build(path.to_str().unwrap(), &mut mem).unwrap();

        assert_eq!(mem.get(b"k").unwrap().1, b"v");
    }

    #[test]
    fn wal_crc_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        std::fs::write(&path, vec![0, 1, 2, 3, 4]).unwrap();

        let mut mem = Memtable::new();
        let res = replay_wal_and_build(path.to_str().unwrap(), &mut mem);

        assert!(res.is_err());
    }
}

#[cfg(test)]
mod load_test {
    use memtable::Memtable;

    #[test]
    fn write_load_test() {
        let mut mem = Memtable::new();
        let mut seq = 0;

        for i in 0..1_000_000 {
            seq += 1;
            let key = format!("key{}", i % 10_000).into_bytes();
            let val = vec![b'x'; 100];
            mem.put(key, val, seq);
        }

        assert!(mem.len() <= 10_000);
    }

    #[test]
    fn delete_heavy_workload() {
        let mut mem = Memtable::new();
        let mut seq = 0;

        for _i in 0..100_000 {
            seq += 1;
            mem.put(b"k".to_vec(), b"v".to_vec(), seq);
            seq += 1;
            mem.delete(b"k".to_vec(), seq);
        }

        assert!(mem.get(b"k").is_none());
        assert_eq!(mem.len(), 1);
    }
}

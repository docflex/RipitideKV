use anyhow::Result;
use std::io::{self, BufRead, Write};
use memtable::Memtable;
use wal::{WalRecord, WalWriter};

fn replay_wal_and_build(path: &str, mem: &mut Memtable) -> Result<u64> {
    if let Ok(mut reader) = wal::WalReader::open(path) {
        let mut max_seq = 0u64;
        reader.replay(|r| match r {
            WalRecord::Put { seq, key, value } => {
                mem.put(key, value, seq);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
            WalRecord::Del { seq, key } => {
                mem.delete(key, seq);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        })?;
        Ok(max_seq)
    } else {
        Ok(0)
    }
}

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

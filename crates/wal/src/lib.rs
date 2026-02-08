use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    Put {
        seq: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Del {
        seq: u64,
        key: Vec<u8>,
    },
}

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt record")]
    Corrupt,
}

/// Simple WAL writer that appends records and optionally fsyncs.
pub struct WalWriter {
    file: File,
    sync: bool,
}

impl WalWriter {
    pub fn create<P: AsRef<Path>>(path: P, sync: bool) -> Result<Self, WalError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Self { file, sync })
    }

    /// Append a record. This writes the full record and (optionally) calls sync_all.
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

        // full record: record_len(u32) [not counting this header], crc(u32), body...
        let record_len = buf.len() as u32 + 4 /*crc*/;
        self.file.write_u32::<LittleEndian>(record_len)?;
        self.file.write_u32::<LittleEndian>(crc)?;
        self.file.write_all(&buf)?;
        self.file.flush()?;

        if self.sync {
            self.file.sync_all()?;
        }
        Ok(())
    }
}

/// WAL reader that yields valid records in sequence. Stops on EOF.
pub struct WalReader<R: Read> {
    rdr: BufReader<R>,
}

impl WalReader<File> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<WalReader<File>, WalError> {
        let f = File::open(path)?;
        Ok(WalReader {
            rdr: BufReader::new(f),
        })
    }
}

// use std::io::BufReader;

impl<R: Read> WalReader<R> {
    pub fn from_reader(reader: R) -> Self {
        WalReader {
            rdr: BufReader::new(reader),
        }
    }

    /// Iterate over records (valid records only). Returns Err on IO errors.
    pub fn replay<F>(&mut self, mut apply: F) -> Result<(), WalError>
    where
        F: FnMut(WalRecord),
    {
        loop {
            // read record_len
            let record_len = match self.rdr.read_u32::<LittleEndian>() {
                Ok(v) => v,
                Err(e) => {
                    return if e.kind() == io::ErrorKind::UnexpectedEof {
                        Ok(())
                    } else {
                        Err(WalError::Io(e))
                    };
                }
            };
            // read crc
            let crc = self.rdr.read_u32::<LittleEndian>()?;
            let mut body = vec![0u8; (record_len - 4) as usize]; // minus crc field
            self.rdr.read_exact(&mut body)?;

            // verify crc
            let mut hasher = Crc32::new();
            hasher.update(&body);
            if hasher.finalize() != crc {
                return Err(WalError::Corrupt);
            }

            // parse body
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
                _ => {
                    return Err(WalError::Corrupt);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn wal_write_and_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let mut w = WalWriter::create(&path, true).unwrap();
            w.append(&WalRecord::Put {
                seq: 1,
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Put {
                seq: 2,
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord::Del {
                seq: 3,
                key: b"k".to_vec(),
            })
            .unwrap();
        }

        // replay
        let mut reader = WalReader::open(&path).unwrap();
        let mut recs = Vec::new();
        reader.replay(|r| recs.push(r)).unwrap();

        assert_eq!(
            recs,
            vec![
                WalRecord::Put {
                    seq: 1,
                    key: b"k".to_vec(),
                    value: b"v1".to_vec()
                },
                WalRecord::Put {
                    seq: 2,
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec()
                },
                WalRecord::Del {
                    seq: 3,
                    key: b"k".to_vec()
                }
            ]
        );

        fs::remove_file(path).unwrap();
    }
}

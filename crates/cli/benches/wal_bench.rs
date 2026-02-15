use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tempfile::tempdir;
use wal::{WalReader, WalRecord, WalWriter};

const N: usize = 5_000;
const VAL_SIZE: usize = 100;

fn wal_append_sync(c: &mut Criterion) {
    c.bench_function("wal_append_sync_1k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("wal.log");
                let w = WalWriter::create(&path, true).unwrap();
                (dir, w)
            },
            |(_dir, mut w)| {
                for i in 0..1_000u64 {
                    w.append(&WalRecord::Put {
                        seq: i,
                        key: format!("k{}", i).into_bytes(),
                        value: vec![b'x'; VAL_SIZE],
                    })
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn wal_append_nosync(c: &mut Criterion) {
    c.bench_function("wal_append_nosync_5k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("wal.log");
                let w = WalWriter::create(&path, false).unwrap();
                (dir, w)
            },
            |(_dir, mut w)| {
                for i in 0..N as u64 {
                    w.append(&WalRecord::Put {
                        seq: i,
                        key: format!("k{}", i).into_bytes(),
                        value: vec![b'x'; VAL_SIZE],
                    })
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn wal_append_del_records(c: &mut Criterion) {
    c.bench_function("wal_append_del_nosync_5k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("wal.log");
                let w = WalWriter::create(&path, false).unwrap();
                (dir, w)
            },
            |(_dir, mut w)| {
                for i in 0..N as u64 {
                    w.append(&WalRecord::Del {
                        seq: i,
                        key: format!("k{}", i).into_bytes(),
                    })
                    .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn wal_replay(c: &mut Criterion) {
    c.bench_function("wal_replay_5k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("wal.log");

                {
                    let mut w = WalWriter::create(&path, false).unwrap();
                    for i in 0..N as u64 {
                        w.append(&WalRecord::Put {
                            seq: i,
                            key: format!("k{}", i).into_bytes(),
                            value: vec![b'x'; VAL_SIZE],
                        })
                        .unwrap();
                    }
                }

                (dir, path)
            },
            |(_dir, path)| {
                let mut reader = WalReader::open(&path).unwrap();
                let mut count = 0usize;

                reader
                    .replay(|_r| {
                        count += 1;
                    })
                    .unwrap();

                assert_eq!(count, N);
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    benches,
    wal_append_sync,
    wal_append_nosync,
    wal_append_del_records,
    wal_replay,
);

criterion_main!(benches);

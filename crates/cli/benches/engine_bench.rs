use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tempfile::tempdir;

// We need to reference the engine module from the cli crate.
// Since engine is a private module in the cli binary crate, we replicate
// a minimal engine setup here using the public crate APIs directly.
use memtable::Memtable;
use sstable::{SSTableReader, SSTableWriter};
use wal::{WalRecord, WalWriter};

const N: usize = 1_000;
const VAL_SIZE: usize = 100;

fn engine_set_no_flush(c: &mut Criterion) {
    c.bench_function("engine_set_no_flush_1k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let wal_path = dir.path().join("wal.log");
                let w = WalWriter::create(&wal_path, false).unwrap();
                let m = Memtable::new();
                (dir, w, m)
            },
            |(_dir, mut w, mut m)| {
                for i in 0..N as u64 {
                    let key = format!("k{}", i).into_bytes();
                    let val = vec![b'x'; VAL_SIZE];
                    w.append(&WalRecord::Put {
                        seq: i + 1,
                        key: key.clone(),
                        value: val.clone(),
                    })
                    .unwrap();
                    m.put(key, val, i + 1);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn engine_set_with_flush(c: &mut Criterion) {
    c.bench_function("engine_set_with_flush_1k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let wal_path = dir.path().join("wal.log");
                let sst_dir = dir.path().join("sst");
                std::fs::create_dir_all(&sst_dir).unwrap();
                let w = WalWriter::create(&wal_path, false).unwrap();
                let m = Memtable::new();
                (dir, wal_path, sst_dir, w, m)
            },
            |(_dir, _wal_path, sst_dir, mut w, mut m)| {
                let threshold = 4096usize;
                let mut seq = 0u64;
                let mut flush_count = 0u32;

                for i in 0..N {
                    seq += 1;
                    let key = format!("k{}", i).into_bytes();
                    let val = vec![b'x'; VAL_SIZE];

                    w.append(&WalRecord::Put {
                        seq,
                        key: key.clone(),
                        value: val.clone(),
                    })
                    .unwrap();

                    m.put(key, val, seq);

                    if m.approx_size() >= threshold {
                        let sst_path = sst_dir.join(format!("sst-{}-{}.sst", seq, flush_count));
                        SSTableWriter::write_from_memtable(&sst_path, &m).unwrap();
                        m.clear();
                        flush_count += 1;
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn engine_get_memtable_hit(c: &mut Criterion) {
    let mut m = Memtable::new();
    for i in 0..N as u64 {
        m.put(
            format!("k{:06}", i).into_bytes(),
            vec![b'x'; VAL_SIZE],
            i + 1,
        );
    }

    c.bench_function("engine_get_memtable_hit_1k", |b| {
        b.iter(|| {
            for i in 0..N as u64 {
                let key = format!("k{:06}", i).into_bytes();
                criterion::black_box(m.get(&key));
            }
        });
    });
}

fn engine_get_sstable_hit(c: &mut Criterion) {
    c.bench_function("engine_get_sstable_hit_1k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.sst");

                let mut m = Memtable::new();
                for i in 0..N as u64 {
                    m.put(
                        format!("k{:06}", i).into_bytes(),
                        vec![b'x'; VAL_SIZE],
                        i + 1,
                    );
                }

                SSTableWriter::write_from_memtable(&path, &m).unwrap();
                let reader = SSTableReader::open(&path).unwrap();
                (dir, reader)
            },
            |(_dir, reader)| {
                for i in 0..N as u64 {
                    let key = format!("k{:06}", i).into_bytes();
                    criterion::black_box(reader.get(&key).unwrap());
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn engine_mixed_workload(c: &mut Criterion) {
    c.bench_function("engine_mixed_set_get_del_1k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let wal_path = dir.path().join("wal.log");
                let w = WalWriter::create(&wal_path, false).unwrap();
                let m = Memtable::new();
                (dir, w, m)
            },
            |(_dir, mut w, mut m)| {
                let mut seq = 0u64;

                for i in 0..N as u64 {
                    seq += 1;
                    let key = format!("k{:06}", i).into_bytes();
                    let val = vec![b'x'; VAL_SIZE];

                    w.append(&WalRecord::Put {
                        seq,
                        key: key.clone(),
                        value: val.clone(),
                    })
                    .unwrap();

                    m.put(key.clone(), val, seq);

                    criterion::black_box(m.get(&key));

                    if i % 5 == 0 {
                        seq += 1;
                        w.append(&WalRecord::Del {
                            seq,
                            key: key.clone(),
                        })
                        .unwrap();
                        m.delete(key, seq);
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    engine_set_no_flush,
    engine_set_with_flush,
    engine_get_memtable_hit,
    engine_get_sstable_hit,
    engine_mixed_workload,
);

criterion_main!(benches);

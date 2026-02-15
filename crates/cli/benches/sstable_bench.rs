use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use memtable::Memtable;
use sstable::{SSTableReader, SSTableWriter};
use tempfile::tempdir;

const N_KEYS: usize = 10_000;
const VALUE_SIZE: usize = 100;

fn build_memtable() -> Memtable {
    let mut mem = Memtable::new();
    for i in 0..N_KEYS {
        mem.put(
            format!("key{}", i).into_bytes(),
            vec![b'x'; VALUE_SIZE],
            i as u64,
        );
    }
    mem
}

fn sstable_write_benchmark(c: &mut Criterion) {
    c.bench_function("sstable_write_from_memtable_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.sst");
                let mem = build_memtable();
                (dir, path, mem)
            },
            |(_dir, path, mem)| {
                SSTableWriter::write_from_memtable(&path, &mem).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn sstable_get_hit_benchmark(c: &mut Criterion) {
    c.bench_function("sstable_get_hit_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.sst");

                let mem = build_memtable();
                SSTableWriter::write_from_memtable(&path, &mem).unwrap();

                let reader = SSTableReader::open(&path).unwrap();
                (dir, reader)
            },
            |(_dir, reader)| {
                for i in 0..N_KEYS {
                    let key = format!("key{}", i).into_bytes();
                    let v = reader.get(&key).unwrap();
                    assert!(v.is_some());
                }
            },
            BatchSize::LargeInput,
        );
    });
}

fn sstable_get_miss_benchmark(c: &mut Criterion) {
    c.bench_function("sstable_get_miss_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let path = dir.path().join("bench.sst");

                let mem = build_memtable();
                SSTableWriter::write_from_memtable(&path, &mem).unwrap();

                let reader = SSTableReader::open(&path).unwrap();
                (dir, reader)
            },
            |(_dir, reader)| {
                for i in 0..N_KEYS {
                    let key = format!("missing{}", i).into_bytes();
                    let v = reader.get(&key).unwrap();
                    assert!(v.is_none());
                }
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(
    benches,
    sstable_write_benchmark,
    sstable_get_hit_benchmark,
    sstable_get_miss_benchmark
);

criterion_main!(benches);

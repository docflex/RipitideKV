use criterion::{criterion_group, criterion_main, Criterion};
use memtable::Memtable;

fn memtable_put_benchmark(c: &mut Criterion) {
    c.bench_function("memtable_put_10k", |b| {
        b.iter(|| {
            let mut m = Memtable::new();
            for i in 0..10_000 {
                m.put(format!("k{}", i).into_bytes(), vec![b'x'; 100], i as u64);
            }
        });
    });
}

criterion_group!(benches, memtable_put_benchmark);
criterion_main!(benches);

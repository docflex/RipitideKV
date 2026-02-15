use criterion::{criterion_group, criterion_main, Criterion};
use memtable::Memtable;

const N: u64 = 10_000;
const VAL_SIZE: usize = 100;

fn memtable_put_sequential(c: &mut Criterion) {
    c.bench_function("memtable_put_10k_sequential", |b| {
        b.iter(|| {
            let mut m = Memtable::new();
            for i in 0..N {
                m.put(format!("k{:06}", i).into_bytes(), vec![b'x'; VAL_SIZE], i);
            }
        });
    });
}

fn memtable_get_hit(c: &mut Criterion) {
    let mut m = Memtable::new();
    for i in 0..N {
        m.put(format!("k{:06}", i).into_bytes(), vec![b'x'; VAL_SIZE], i);
    }

    c.bench_function("memtable_get_hit_10k", |b| {
        b.iter(|| {
            for i in 0..N {
                let key = format!("k{:06}", i).into_bytes();
                criterion::black_box(m.get(&key));
            }
        });
    });
}

fn memtable_get_miss(c: &mut Criterion) {
    let mut m = Memtable::new();
    for i in 0..N {
        m.put(format!("k{:06}", i).into_bytes(), vec![b'x'; VAL_SIZE], i);
    }

    c.bench_function("memtable_get_miss_10k", |b| {
        b.iter(|| {
            for i in 0..N {
                let key = format!("miss{:06}", i).into_bytes();
                criterion::black_box(m.get(&key));
            }
        });
    });
}

fn memtable_overwrite_same_key(c: &mut Criterion) {
    c.bench_function("memtable_overwrite_same_key_10k", |b| {
        b.iter(|| {
            let mut m = Memtable::new();
            for i in 0..N {
                m.put(b"k".to_vec(), vec![b'x'; VAL_SIZE], i);
            }
        });
    });
}

fn memtable_delete(c: &mut Criterion) {
    c.bench_function("memtable_delete_10k", |b| {
        b.iter(|| {
            let mut m = Memtable::new();

            for i in 0..N {
                m.put(format!("k{:06}", i).into_bytes(), vec![b'x'; VAL_SIZE], i);
            }

            for i in 0..N {
                m.delete(format!("k{:06}", i).into_bytes(), N + i);
            }
        });
    });
}

fn memtable_mixed_workload(c: &mut Criterion) {
    c.bench_function("memtable_mixed_put_get_del_10k", |b| {
        b.iter(|| {
            let mut m = Memtable::new();
            let mut seq = 0u64;

            for i in 0..N {
                seq += 1;
                let key = format!("k{:06}", i).into_bytes();

                m.put(key.clone(), vec![b'x'; VAL_SIZE], seq);
                criterion::black_box(m.get(&key));

                if i % 3 == 0 {
                    seq += 1;
                    m.delete(key, seq);
                }
            }
        });
    });
}

fn memtable_iter(c: &mut Criterion) {
    let mut m = Memtable::new();
    for i in 0..N {
        m.put(format!("k{:06}", i).into_bytes(), vec![b'x'; VAL_SIZE], i);
    }

    c.bench_function("memtable_iter_10k", |b| {
        b.iter(|| {
            let count = m.iter().count();
            criterion::black_box(count);
        });
    });
}

criterion_group!(
    benches,
    memtable_put_sequential,
    memtable_get_hit,
    memtable_get_miss,
    memtable_overwrite_same_key,
    memtable_delete,
    memtable_mixed_workload,
    memtable_iter,
);

criterion_main!(benches);

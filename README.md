# RiptideKV

A learning project to build a simple Log-Structured Merge (LSM) key–value store in Rust.

## Goals

- Learn Rust fundamentals in a systems context
- Incrementally build an LSM-style storage engine
- Practice testing, CI, and clean architecture

## Non-Goals (for now)

- Production performance
- Distributed consensus
- Persistence guarantees beyond learning needs

## Glossary

- **LSM**: Log-Structured Merge tree, a write-optimized storage structure
- **Memtable**: In-memory, mutable structure holding recent writes
- **SSTable**: Immutable on-disk sorted string table
- **Compaction**: Merging SSTables to remove duplicates and reclaim space
- **WAL**: Write-Ahead Log for crash recovery

### Phase 0 — Rust fundamentals & repo setup (deliverables)

- Deliverables:
    - Repo skeleton with Cargo workspace, CI (GitHub Actions), linting (clippy), formatting (rustfmt).
    - A README with project goals + glossary (LSM, memtable, sstable, compaction, WAL).
- Learning tasks (self-study + tiny exercises):
    - Rust basics: ownership, borrowing, lifetimes, traits, `Result`/`Option`, pattern matching.
    - Modules, crates, cargo, unit testing.
    - Concurrency primitives and `async`/`await`.
    - Exercises: implement a command-line toy that stores key→value in-memory (HashMap), tests, and cargo fmt/clippy.
- Tools: use `rustup`, `cargo`, and add `clippy`, `rustfmt`.

### Phase 1 — Core LSM (in-memory + on-disk basic) (deliverables)

- Deliverables:
    - Memtable (ordered) with unit tests.
    - WAL append with safe fsync and recovery test (crash simulation).
    - SSTable writer and reader (simple block layout, uncompressed).
    - A small CLI that can SET/GET locally (no networking).
- Design choices (locked):
    - Memtable implementation: start with `BTreeMap<Vec<u8>, ValueEntry>` (easy for Rust beginners); later swap to a skiplist if you want lock-free behavior.
    - WAL: append binary records: `[len][crc32][key_len][key][value_len][value]`. Use CRC per record for corruption detection.
    - SSTable layout: write data blocks where each block contains contiguous entries; build a small in-memory sparse index (key → block offset) when opening file.
    - Recovery: on startup list SSTables, read manifest (or derive from filenames), then apply WALs (newest to oldest) to rebuild memtable.

### Phase 2 — Reads, bloom filters, and compaction basics (deliverables)

- Deliverables:
    - Read path: search memtable → recent SSTables (using bloom filter & index) → older SSTables (merge results).
    - Bloom filter implementation per SSTable.
    - Simple compaction: merge two or more SSTables into a new one; remove deleted keys / tombstones.
    - Tests for correctness (reads across levels, deletion semantics).
- Design choices:
    - Use a per-SSTable bloom filter to avoid disk reads when key absent.
    - Tombstones for deletes: store tombstone markers during deletes and remove during compaction.

### Phase 3 — Robustness, concurrency & configuration knobs (deliverables)

- Deliverables:
    - Background compaction worker; max concurrent compactions configurable.
    - Config knobs for memtable size, compaction thresholds, WAL durability (`fsync` on every write vs batched).
    - Snapshot support (point-in-time view) possibly via sequence numbers.
    - More extensive tests: crash (kill while compaction), recovery, read/write consistency.
- Design choices:
    - Compaction worker scheduling: simple priority rule (smallest level first).
    - Sequence numbers per write to support consistent snapshots.

### Phase 4 — RESP server & Java compatibility (deliverables)

- Deliverables:
    - RESP server supporting GET/SET/DEL and PING/INFO. Existing Redis clients should work unchanged.
    - Integration tests using a Java Redis client (e.g., Jedis) to exercise GET/SET/DEL.
    - Basic telemetry (metrics endpoint or INFO command).
- Design choices:
    - Implement RESP parser (RESP2 subset first) and dispatcher in Tokio.
    - Command execution path: parse → map to engine call → write response.
    - Keep networking async; ensure storage engine calls that block are done in `spawn_blocking` or are non-blocking.

### Phase 5 — Performance, features, and polish (deliverables)

- Deliverables:
    - Benchmarks and tuning (use `criterion` or custom harness).
    - Optional features: TTL (expire), persistence compaction strategies (leveled), compression (snappy), LRU cache for data blocks, memory limits.
    - Documentation: design doc per component, API reference, "How it works" diagrams.
    - Example Java app that uses a Redis client (Jedis/Lettuce) to replace Redis with your store.
- Engineering tasks:
    - Add metrics (prometheus), logging (tracing), CI for tests + benchmarks.
    - Add fuzzing (`cargo-fuzz`) and property tests (quickcheck / proptest) for invariants.

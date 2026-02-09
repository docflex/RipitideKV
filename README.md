# RiptideKV

**RiptideKV** is a learning project to build a simple **Log-Structured Merge (LSM) key–value store** in Rust.
The goal is to understand storage engine internals by implementing them incrementally and correctly, not to ship a production database.

## Goals

- Learn Rust fundamentals in a systems programming context
- Incrementally build an LSM-style storage engine
- Practice testing, CI, and clean architecture

## Non-Goals (for now)

- Production-grade performance
- Distributed systems or consensus
- Strong persistence guarantees beyond learning needs

## Glossary

- **LSM** — Log-Structured Merge tree; a write-optimized storage structure
- **Memtable** — In-memory, mutable structure holding recent writes
- **SSTable** — Immutable, on-disk sorted string table
- **Compaction** — Merging SSTables to remove duplicates and reclaim space
- **WAL** — Write-Ahead Log used for crash recovery

---

## Phase 0 — Rust fundamentals & repository setup

### Deliverables

- Repository skeleton with:
    - Cargo workspace
    - CI (GitHub Actions)
    - Linting (`clippy`)
    - Formatting (`rustfmt`)

- README describing project goals and glossary

### Learning Tasks

- Rust fundamentals:
    - Ownership, borrowing, lifetimes
    - Traits
    - `Result` / `Option`
    - Pattern matching

- Modules, crates, Cargo, and unit testing
- Concurrency primitives and `async` / `await`
- Exercises:
    - Implement a simple CLI storing key → value in memory (`HashMap`)
    - Add tests
    - Enforce `cargo fmt` and `cargo clippy`

### Tooling

- `rustup`
- `cargo`
- `clippy`
- `rustfmt`

---

## Phase 1 — Core LSM (in-memory + basic on-disk) [DELIVERED]

### Deliverables

- Ordered memtable with unit tests
- Write-Ahead Log (WAL):
    - Append-only writes
    - Safe `fsync`
    - Recovery test via crash simulation

- SSTable writer and reader:
    - Simple block layout
    - No compression

- Small CLI supporting `SET` and `GET` (local only, no networking)

### Design Decisions

- **Memtable**:
  Start with `BTreeMap<Vec<u8>, ValueEntry>` for simplicity; optionally replace with a skip list later.
- **WAL format**:
  Binary records:

    ```
    [len][crc32][key_len][key][value_len][value]
    ```

    CRC per record for corruption detection.

- **SSTable layout**:
    - Immutable data blocks containing contiguous entries
    - Build a sparse in-memory index (key → block offset) on open

- **Recovery**:
    - Discover SSTables via manifest or filenames
    - Replay WALs (newest to oldest) to rebuild the memtable

### Write Path Demonstrations

#### 1. Writing to Memtable, then WAL

<video autoplay muted playsinline controls>
  <source src="public/assets/memtable_wal.mp4" type="video/mp4">
</video>

#### 2. Memtable Threshold Exceeded → SSTable Created → WAL Flushed

<video autoplay muted playsinline controls>
  <source src="public/assets/flush_to_sstable.mp4" type="video/mp4">
</video>

#### 3. New Memtables Writing to WAL After SST Creation

<video autoplay muted playsinline controls>
  <source src="public/assets/new_memtable_after_flush.mp4" type="video/mp4">
</video>

#### 4. Deletions Propagated via Writes

<video autoplay muted playsinline controls>
  <source src="public/assets/delete_propagation.mp4" type="video/mp4">
</video>

---

## Phase 2 — Reads, bloom filters, and compaction basics

### Deliverables

- Read path:
    - Memtable
    - Newest SSTables (via bloom filter + index)
    - Older SSTables (merged results)

- Bloom filter per SSTable
- Basic compaction:
    - Merge multiple SSTables
    - Drop obsolete keys and tombstones

- Tests covering:
    - Reads across levels
    - Delete semantics

### Design Decisions

- Use per-SSTable bloom filters to avoid unnecessary disk reads
- Represent deletes using tombstones; remove them during compaction

---

## Phase 3 — Robustness, concurrency, and configuration

### Deliverables

- Background compaction worker
- Configurable:
    - Memtable size
    - Compaction thresholds
    - WAL durability (`fsync` per write vs batched)

- Snapshot support (point-in-time reads)
- Extended test coverage:
    - Crash during compaction
    - Recovery correctness
    - Read/write consistency

### Design Decisions

- Compaction scheduling: prioritize smallest levels first
- Use sequence numbers per write to support snapshots

---

## Phase 4 — RESP server & Java compatibility

### Deliverables

- RESP server supporting:
    - `GET`, `SET`, `DEL`
    - `PING`, `INFO`

- Compatibility with existing Redis clients
- Integration tests using a Java Redis client (e.g. Jedis)
- Basic telemetry (metrics endpoint or `INFO` output)

### Design Decisions

- Implement RESP2 subset first
- Async networking using Tokio
- Ensure blocking storage operations run in `spawn_blocking` or are non-blocking

---

## Phase 5 — Performance, features, and polish

### Deliverables

- Benchmarks and tuning (`criterion` or custom harness)
- Optional features:
    - TTL / expiration
    - Leveled compaction
    - Compression (e.g. Snappy)
    - LRU cache for data blocks
    - Memory limits

- Documentation:
    - Per-component design docs
    - API reference
    - Architecture diagrams

- Example Java application replacing Redis with RiptideKV

### Engineering Tasks

- Metrics (Prometheus)
- Structured logging (`tracing`)
- CI for tests and benchmarks
- Fuzzing (`cargo-fuzz`)
- Property-based testing (`quickcheck` / `proptest`)

---

If you want, I can also:

- tighten this further into a **one-page README**
- split it into **README + DESIGN.md**
- or rewrite it in a more **“database internals blog series”** tone

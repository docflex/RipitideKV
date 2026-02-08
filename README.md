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

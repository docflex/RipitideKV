//! # SSTable — Sorted String Table
//!
//! Immutable, on-disk storage files for the RiptideKV storage engine.
//!
//! When the in-memory [`memtable::Memtable`] exceeds its size threshold the
//! engine flushes it to disk as an SSTable. SSTables are **write-once,
//! read-many** — once created they are never modified (only replaced during
//! compaction in Phase 2+).
//!
//! ## File layout (v1)
//!
//! ```text
//! ┌───────────────────────────────────────────────┐
//! │ DATA SECTION (sorted key/value records)       │
//! │                                               │
//! │ key_len (u32) | key | seq (u64) | present (u8)│
//! │ val_len (u32) | val                           │
//! │                                               │
//! │ ... repeated for each entry ...               │
//! ├───────────────────────────────────────────────┤
//! │ INDEX SECTION (key → data_offset mapping)     │
//! │                                               │
//! │ key_len (u32) | key | data_offset (u64)       │
//! │                                               │
//! │ ... repeated for each entry ...               │
//! ├───────────────────────────────────────────────┤
//! │ FOOTER (always last 12 bytes)                 │
//! │                                               │
//! │ index_offset (u64 LE) | magic (u32 LE) "SST1" │
//! └───────────────────────────────────────────────┘
//! ```
//!
//! All integers are little-endian. The magic value `0x5353_5431` ("SST1")
//! identifies the file format version.

mod format;
mod reader;
mod writer;

pub use format::{FOOTER_BYTES, SSTABLE_MAGIC};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;

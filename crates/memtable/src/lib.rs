//! # Memtable
//!
//! An in-memory, sorted, mutable write buffer for the RiptideKV storage engine.
//!
//! The memtable is the first point of contact for every write operation. It buffers
//! recent `PUT` and `DELETE` operations in a sorted structure (`BTreeMap`) before
//! they are flushed to immutable on-disk SSTables.
//!
//! ## Key properties
//! - **Sorted order**: entries are always in ascending key order (required for SSTable flush).
//! - **Sequence-number gated**: stale writes (lower sequence number) are silently rejected.
//! - **Tombstone support**: deletes are recorded as `ValueEntry { value: None }` markers.
//! - **Approximate size tracking**: tracks the byte size of keys + values for flush threshold decisions.
//!
//! ## Example
//! ```rust
//! use memtable::Memtable;
//!
//! let mut m = Memtable::new();
//! m.put(b"hello".to_vec(), b"world".to_vec(), 1);
//! assert_eq!(m.get(b"hello").unwrap().1, b"world".to_vec());
//!
//! m.delete(b"hello".to_vec(), 2);
//! assert!(m.get(b"hello").is_none());
//! ```

use std::collections::BTreeMap;

/// A single entry in the memtable, pairing a sequence number with an optional value.
///
/// - `value == Some(bytes)` — the key holds a live value.
/// - `value == None` — the key has been deleted (tombstone).
///
/// Tombstones are retained in the memtable and flushed to SSTables so that
/// older values in lower levels are correctly shadowed during reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    /// Monotonically increasing sequence number assigned at write time.
    pub seq: u64,
    /// `Some(bytes)` for live values, `None` for tombstones (deletes).
    pub value: Option<Vec<u8>>,
}

/// An ordered, in-memory write buffer backed by a `BTreeMap`.
///
/// The memtable tracks an approximate byte size (keys + values) so the engine
/// can decide when to flush to an SSTable. Sequence numbers gate every mutation:
/// a write with a sequence number <= the existing entry's sequence is silently
/// dropped, ensuring consistency during WAL replay and concurrent recovery.
#[derive(Debug)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, ValueEntry>,
    approx_size: usize,
}

impl Memtable {
    /// Creates a new, empty memtable.
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    /// Inserts a key-value pair with the given sequence number.
    ///
    /// If the key already exists with a **newer or equal** sequence number, the
    /// write is silently ignored (stale-write protection). Otherwise the old
    /// entry is replaced and `approx_size` is adjusted accordingly.
    ///
    /// # Arguments
    ///
    /// * `key` - the lookup key (ownership transferred to the memtable).
    /// * `value` - the payload bytes (ownership transferred).
    /// * `seq` - monotonically increasing sequence number.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, seq: u64) {
        match self.map.get(&key) {
            Some(old) if old.seq >= seq => {
                // stale or equal write, ignore
                return;
            }
            Some(old) => {
                // Replace existing entry: remove old value bytes from approx_size if present.
                if let Some(ref ov) = old.value {
                    self.approx_size = self.approx_size.saturating_sub(ov.len());
                }
                // Key bytes already counted; do not subtract key length here.
            }
            None => {
                // New key: count key bytes
                self.approx_size = self.approx_size.saturating_add(key.len());
            }
        }

        // Add new value bytes
        self.approx_size = self.approx_size.saturating_add(value.len());

        self.map.insert(
            key,
            ValueEntry {
                seq,
                value: Some(value),
            },
        );
    }

    /// Records a tombstone (delete marker) for the given key.
    ///
    /// A tombstone is stored as `ValueEntry { seq, value: None }`. It shadows
    /// any older value both in the memtable and in SSTables during reads.
    ///
    /// Stale-write protection applies: if the key already has a newer or equal
    /// sequence number, the delete is silently ignored.
    pub fn delete(&mut self, key: Vec<u8>, seq: u64) {
        match self.map.get(&key) {
            Some(old) if old.seq >= seq => {
                // existing newer or equal entry; ignore
                return;
            }
            Some(old) => {
                // If there was a live value, subtract its size (key stays counted)
                if let Some(ref ov) = old.value {
                    self.approx_size = self.approx_size.saturating_sub(ov.len());
                }
                // Leave key bytes counted (they were already counted when the key first appeared)
            }
            None => {
                // New tombstone for a key we haven't seen — count the key bytes
                self.approx_size = self.approx_size.saturating_add(key.len());
            }
        }

        self.map.insert(key, ValueEntry { seq, value: None });
    }

    /// Returns the value for the given key if it exists and is **not** a tombstone.
    ///
    /// Returns `Some((seq, value_bytes))` for live entries, `None` for missing
    /// keys or tombstones.
    pub fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        self.map
            .get(key)
            .and_then(|e| e.value.as_ref().map(|v| (e.seq, v.clone())))
    }

    /// Returns an iterator over all entries in **ascending key order**.
    ///
    /// This includes tombstones. The ordering guarantee is provided by the
    /// underlying `BTreeMap` and is required for correct SSTable flush.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &ValueEntry)> {
        self.map.iter()
    }

    /// Returns the number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns the approximate byte size of all keys and values stored.
    ///
    /// This is used by the engine to decide when to flush the memtable to an
    /// SSTable. The size tracks key bytes + value bytes but does **not** include
    /// `BTreeMap` node overhead.
    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    /// Returns `true` if the memtable contains zero entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the raw [`ValueEntry`] for the given key, if present.
    ///
    /// Unlike [`get`], this does **not** filter out tombstones. The engine uses
    /// this to distinguish between "key not found" (returns `None`) and
    /// "key was deleted" (returns `Some(ValueEntry { value: None })`).
    pub fn get_entry(&self, key: &[u8]) -> Option<&ValueEntry> {
        self.map.get(key)
    }

    /// Returns `true` if the memtable contains the given key (including tombstones).
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    /// Removes all entries and resets `approx_size` to zero.
    ///
    /// This is semantically equivalent to replacing the memtable with
    /// `Memtable::new()`, but reuses the existing allocations.
    pub fn clear(&mut self) {
        self.map.clear();
        self.approx_size = 0;
    }

    /// Returns `true` if the memtable contains the given key (including tombstones).
    pub fn is_present(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------- Basic CRUD --------------------

    #[test]
    fn put_and_get_single_key() {
        let mut m = Memtable::new();
        m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
        assert_eq!(m.len(), 1);
        let (seq, val) = m.get(b"k1").unwrap();
        assert_eq!(seq, 1);
        assert_eq!(val, b"v1");
    }

    #[test]
    fn put_overwrites_with_newer_seq() {
        let mut m = Memtable::new();
        m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
        m.put(b"k1".to_vec(), b"v2".to_vec(), 2);
        assert_eq!(m.get(b"k1").unwrap().1, b"v2");
    }

    #[test]
    fn put_ignores_stale_seq() {
        let mut m = Memtable::new();
        m.put(b"k1".to_vec(), b"v2".to_vec(), 5);
        m.put(b"k1".to_vec(), b"v-old".to_vec(), 3);
        assert_eq!(m.get(b"k1").unwrap().1, b"v2");
    }

    #[test]
    fn put_ignores_equal_seq() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"first".to_vec(), 1);
        m.put(b"k".to_vec(), b"second".to_vec(), 1);
        // Equal seq is treated as stale — first write wins
        assert_eq!(m.get(b"k").unwrap().1, b"first");
    }

    #[test]
    fn get_missing_key_returns_none() {
        let m = Memtable::new();
        assert!(m.get(b"nonexistent").is_none());
    }

    #[test]
    fn delete_creates_tombstone() {
        let mut m = Memtable::new();
        m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
        m.delete(b"k1".to_vec(), 2);
        assert!(m.get(b"k1").is_none());
        assert_eq!(m.len(), 1); // tombstone still present
    }

    // -------------------- Load / write tests --------------------

    #[test]
    fn write_load_10k_unique_keys() {
        let mut m = Memtable::new();
        for i in 0..10_000u64 {
            let key = format!("key{}", i).into_bytes();
            let val = vec![b'x'; 100];
            m.put(key, val, i);
        }
        assert_eq!(m.len(), 10_000);
    }

    #[test]
    fn write_load_with_key_reuse() {
        let mut m = Memtable::new();
        let mut seq = 0u64;
        for i in 0..100_000u64 {
            seq += 1;
            let key = format!("key{}", i % 1_000).into_bytes();
            m.put(key, vec![b'x'; 50], seq);
        }
        assert_eq!(m.len(), 1_000);
    }

    // -------------------- Many / stress tests --------------------

    #[test]
    fn many_distinct_keys() {
        let mut m = Memtable::new();
        for i in 0u64..1000 {
            m.put(format!("key{:04}", i).into_bytes(), b"v".to_vec(), i);
        }
        assert_eq!(m.len(), 1000);
        // Verify sorted order
        let keys: Vec<_> = m.iter().map(|(k, _)| k.clone()).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn overwrite_same_key_many_times() {
        let mut m = Memtable::new();
        for seq in 1..=10_000u64 {
            m.put(b"k".to_vec(), format!("v{}", seq).into_bytes(), seq);
        }
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(b"k").unwrap().0, 10_000);
    }

    #[test]
    fn alternating_put_delete() {
        let mut m = Memtable::new();
        for i in 0..1_000u64 {
            let seq = i * 2 + 1;
            m.put(b"k".to_vec(), b"v".to_vec(), seq);
            m.delete(b"k".to_vec(), seq + 1);
        }
        assert!(m.get(b"k").is_none());
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn delete_heavy_workload() {
        let mut m = Memtable::new();
        let mut seq = 0u64;
        for _ in 0..10_000 {
            seq += 1;
            m.put(b"k".to_vec(), b"v".to_vec(), seq);
            seq += 1;
            m.delete(b"k".to_vec(), seq);
        }
        assert!(m.get(b"k").is_none());
        assert_eq!(m.len(), 1);
    }

    // -------------------- Edge cases --------------------

    #[test]
    fn empty_key() {
        let mut m = Memtable::new();
        m.put(b"".to_vec(), b"val".to_vec(), 1);
        assert_eq!(m.get(b"").unwrap().1, b"val");
    }

    #[test]
    fn empty_value() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"".to_vec(), 1);
        let (_s, v) = m.get(b"k").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn binary_key_and_value() {
        let mut m = Memtable::new();
        let key = vec![0x00, 0xFF, 0x80, 0x01];
        let val = vec![0xDE, 0xAD, 0xBE, 0xEF];
        m.put(key.clone(), val.clone(), 1);
        assert_eq!(m.get(&key).unwrap().1, val);
    }

    #[test]
    fn large_value() {
        let mut m = Memtable::new();
        let val = vec![b'x'; 1_000_000]; // 1 MB
        m.put(b"big".to_vec(), val.clone(), 1);
        assert_eq!(m.get(b"big").unwrap().1.len(), 1_000_000);
        assert_eq!(m.approx_size(), 3 + 1_000_000); // key len (3) + value len
    }

    #[test]
    fn seq_zero_is_valid() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v".to_vec(), 0);
        assert_eq!(m.get(b"k").unwrap().0, 0);
    }

    #[test]
    fn seq_max_u64() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v".to_vec(), u64::MAX);
        assert_eq!(m.get(b"k").unwrap().0, u64::MAX);
    }

    // -------------------- Clear --------------------

    #[test]
    fn clear_resets_everything() {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"1".to_vec(), 1);
        m.put(b"b".to_vec(), b"2".to_vec(), 2);
        assert!(!m.is_empty());
        assert!(m.approx_size() > 0);

        m.clear();
        assert_eq!(m.len(), 0);
        assert_eq!(m.approx_size(), 0);
        assert!(m.is_empty());
        assert!(m.get(b"a").is_none());
    }

    #[test]
    fn clear_then_reuse() {
        let mut m = Memtable::new();
        m.put(b"old".to_vec(), b"data".to_vec(), 1);
        m.clear();
        // Seq 1 should work fine after clear (no stale-write issue)
        m.put(b"new".to_vec(), b"data".to_vec(), 1);
        assert_eq!(m.get(b"new").unwrap().0, 1);
        assert!(m.get(b"old").is_none());
    }

    // -------------------- Len / is_empty --------------------

    #[test]
    fn len_counts_tombstones() {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"1".to_vec(), 1);
        m.delete(b"b".to_vec(), 2);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn is_empty_on_new() {
        let m = Memtable::new();
        assert!(m.is_empty());
    }

    #[test]
    fn is_empty_after_insert() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v".to_vec(), 1);
        assert!(!m.is_empty());
    }

    #[test]
    fn default_creates_empty() {
        let m = Memtable::default();
        assert!(m.is_empty());
        assert_eq!(m.approx_size(), 0);
    }

    // -------------------- Iterator ordering --------------------

    #[test]
    fn iter_yields_sorted_keys() {
        let mut m = Memtable::new();
        m.put(b"c".to_vec(), b"3".to_vec(), 3);
        m.put(b"a".to_vec(), b"1".to_vec(), 1);
        m.put(b"b".to_vec(), b"2".to_vec(), 2);

        let keys: Vec<&[u8]> = m.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(
            keys,
            vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
    }

    #[test]
    fn iter_includes_tombstones() {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"1".to_vec(), 1);
        m.delete(b"b".to_vec(), 2);
        m.put(b"c".to_vec(), b"3".to_vec(), 3);

        let entries: Vec<_> = m.iter().collect();
        assert_eq!(entries.len(), 3);
        assert!(entries[1].1.value.is_none()); // "b" is tombstone
    }

    #[test]
    fn iter_empty_memtable() {
        let m = Memtable::new();
        assert_eq!(m.iter().count(), 0);
    }

    // -------------------- contains_key --------------------

    #[test]
    fn contains_key_live_value() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v".to_vec(), 1);
        assert!(m.contains_key(b"k"));
    }

    #[test]
    fn contains_key_tombstone() {
        let mut m = Memtable::new();
        m.delete(b"k".to_vec(), 1);
        assert!(m.contains_key(b"k"));
    }

    #[test]
    fn contains_key_missing() {
        let m = Memtable::new();
        assert!(!m.contains_key(b"k"));
    }

    // -------------------- approx_size tracking --------------------

    #[test]
    fn approx_size_includes_key_and_value() {
        let mut m = Memtable::new();
        assert_eq!(m.approx_size(), 0);
        // key="ab" (2) + value="ccc" (3) = 5
        m.put(b"ab".to_vec(), b"ccc".to_vec(), 1);
        assert_eq!(m.approx_size(), 5);
    }

    #[test]
    fn approx_size_adjusts_on_overwrite() {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"aaa".to_vec(), 1); // key=1 + val=3 = 4
        assert_eq!(m.approx_size(), 4);
        m.put(b"a".to_vec(), b"bb".to_vec(), 2); // key=1 + val=2 = 3
        assert_eq!(m.approx_size(), 3);
    }

    #[test]
    fn approx_size_adjusts_on_delete() {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"aaa".to_vec(), 1); // 1+3=4
        m.delete(b"a".to_vec(), 2); // value removed, key stays -> 1
        assert_eq!(m.approx_size(), 1);
    }

    #[test]
    fn approx_size_for_new_tombstone() {
        let mut m = Memtable::new();
        m.delete(b"key".to_vec(), 1); // key=3, no value -> 3
        assert_eq!(m.approx_size(), 3);
    }

    #[test]
    fn approx_size_stale_write_no_change() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v".to_vec(), 5); // 1+1=2
        let before = m.approx_size();
        m.put(b"k".to_vec(), b"vvvvv".to_vec(), 3); // stale, ignored
        assert_eq!(m.approx_size(), before);
    }

    #[test]
    fn approx_size_multiple_keys() {
        let mut m = Memtable::new();
        m.put(b"a".to_vec(), b"1".to_vec(), 1); // 1+1=2
        m.put(b"bb".to_vec(), b"22".to_vec(), 2); // 2+2=4
        m.put(b"ccc".to_vec(), b"333".to_vec(), 3); // 3+3=6
        assert_eq!(m.approx_size(), 12);
    }

    #[test]
    fn delete_with_stale_seq_ignored() {
        let mut m = Memtable::new();
        m.put(b"k1".to_vec(), b"v1".to_vec(), 5);
        m.delete(b"k1".to_vec(), 3);
        assert_eq!(m.get(b"k1").unwrap().1, b"v1");
    }

    #[test]
    fn delete_nonexistent_key_creates_tombstone() {
        let mut m = Memtable::new();
        m.delete(b"k".to_vec(), 1);
        assert_eq!(m.len(), 1);
        assert!(m.get(b"k").is_none());
        assert!(m.contains_key(b"k"));
    }

    #[test]
    fn put_after_delete_with_higher_seq_resurrects_key() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v1".to_vec(), 1);
        m.delete(b"k".to_vec(), 2);
        assert!(m.get(b"k").is_none());

        m.put(b"k".to_vec(), b"v2".to_vec(), 3);
        assert_eq!(m.get(b"k").unwrap().1, b"v2");
    }

    #[test]
    fn put_after_delete_with_lower_seq_ignored() {
        let mut m = Memtable::new();
        m.delete(b"k".to_vec(), 5);
        m.put(b"k".to_vec(), b"v".to_vec(), 3);
        assert!(m.get(b"k").is_none());
    }

    // -------------------- get_entry & tombstones --------------------

    #[test]
    fn get_entry_returns_tombstone() {
        let mut m = Memtable::new();
        m.delete(b"k".to_vec(), 1);
        let entry = m.get_entry(b"k").unwrap();
        assert_eq!(entry.seq, 1);
        assert!(entry.value.is_none());
    }

    #[test]
    fn get_entry_returns_none_for_missing_key() {
        let m = Memtable::new();
        assert!(m.get_entry(b"nope").is_none());
    }

    #[test]
    fn get_entry_returns_live_value() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v".to_vec(), 1);
        let entry = m.get_entry(b"k").unwrap();
        assert_eq!(entry.value.as_deref(), Some(b"v".as_slice()));
    }
}

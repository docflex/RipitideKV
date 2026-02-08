use std::collections::BTreeMap;

/// ValueEntry stores the sequence number and the optional value.
/// `value == None` signifies a tombstone (delete).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    pub seq: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, ValueEntry>,
    approx_size: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    /// Put a key with a seq number. Overwrites existing entry if seq is newer.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, seq: u64) {
        match self.map.get(&key) {
            Some(old) if old.seq >= seq => return,
            Some(old) => {
                if let Some(ref ov) = old.value {
                    self.approx_size = self.approx_size.saturating_sub(ov.len());
                }
            }
            None => {}
        }

        self.approx_size += value.len();
        self.map.insert(
            key,
            ValueEntry {
                seq,
                value: Some(value),
            },
        );
    }

    /// Delete: add a tombstone with seq
    pub fn delete(&mut self, key: Vec<u8>, seq: u64) {
        match self.map.get(&key) {
            Some(old) if old.seq >= seq => {
                return;
            }
            Some(old) => {
                if let Some(ref ov) = old.value {
                    self.approx_size = self.approx_size.saturating_sub(ov.len());
                }
            }
            None => {}
        }
        self.map.insert(key, ValueEntry { seq, value: None });
    }

    /// Get the latest value if present and not a tombstone
    pub fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        self.map
            .get(key)
            .and_then(|e| e.value.as_ref().map(|v| (e.seq, v.clone())))
    }

    /// Ordered iterator over entries (key, ValueEntry)
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &ValueEntry)> {
        self.map.iter()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
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

    #[test]
    fn memtable_put_get_delete() {
        let mut m = Memtable::new();
        m.put(b"k1".to_vec(), b"v1".to_vec(), 1);
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(b"k1").unwrap().1, b"v1".to_vec());

        // newer put replaces
        m.put(b"k1".to_vec(), b"v2".to_vec(), 2);
        assert_eq!(m.get(b"k1").unwrap().1, b"v2".to_vec());

        // older put ignored
        m.put(b"k1".to_vec(), b"v-old".to_vec(), 1);
        assert_eq!(m.get(b"k1").unwrap().1, b"v2".to_vec());

        // delete with newer seq creates tombstone
        m.delete(b"k1".to_vec(), 3);
        assert!(m.get(b"k1").is_none());
        assert_eq!(m.len(), 1); // tombstone still present

        // delete with older seq ignored
        m.delete(b"k1".to_vec(), 2);
        assert!(m.get(b"k1").is_none());
    }

    #[test]
    fn approx_size_counts_values() {
        let mut m = Memtable::new();
        assert_eq!(m.approx_size(), 0);
        m.put(b"a".to_vec(), b"aaa".to_vec(), 1);
        assert_eq!(m.approx_size(), 3);
        m.put(b"a".to_vec(), b"bb".to_vec(), 2);
        assert_eq!(m.approx_size(), 2);
        m.delete(b"a".to_vec(), 3);
        assert_eq!(m.approx_size(), 0);
    }
}

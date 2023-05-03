use crate::kv;
use anyhow::{bail, Result};

/// A ValueEntry or contain a [kv::Value] or a Tombstone. A tombstone value
/// means that a value for a given key was deleted.
enum ValueEntry {
    Some(kv::Value),
    Tombstone,
}

/// A entry key that may contain multiple values for different point of time.
/// The last value of the list of values is the most recent value.
struct KeyEntry {
    key: kv::Key,
    values: Vec<ValueEntry>,
}

impl From<kv::Key> for KeyEntry {
    fn from(key: kv::Key) -> Self {
        Self {
            key,
            values: Vec::new(),
        }
    }
}

/// The in-memory representation of the database.
pub struct Memtable {
    entries: Vec<KeyEntry>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Insert the given value on the given key. If the key already exist on
    /// memtable a new version of the value is added (the previous value is not
    /// replaced).
    pub fn set(&mut self, key: kv::Key, value: kv::Value) -> Result<()> {
        match self.get_index(&key) {
            Ok(idx) => {
                let entry = &mut self.entries[idx];
                entry.values.push(ValueEntry::Some(value));
                Ok(())
            }
            Err(idx) => {
                let mut entry = KeyEntry::from(key);
                entry.values.push(ValueEntry::Some(value));
                self.entries.insert(idx, entry);
                Ok(())
            }
        }
    }

    /// Fetch the given key from memtable. It returns [Option::None] if the key don't
    /// exists.
    pub fn get(&self, key: &kv::Key) -> Option<&kv::Value> {
        match self.get_index(key) {
            Ok(idx) => {
                let entry = &self.entries[idx];
                let value = entry.values.last()?;
                match value {
                    ValueEntry::Some(value) => Some(value),
                    ValueEntry::Tombstone => None,
                }
            }
            Err(_) => None,
        }
    }

    /// Deletes a Key-Value pair in the MemTable.
    ///
    /// This is achieved using tombstones.
    pub fn delete(&mut self, key: &kv::Key) -> Result<()> {
        match self.get_index(key) {
            Ok(idx) => {
                let key = &mut self.entries[idx];
                key.values.push(ValueEntry::Tombstone);
                Ok(())
            }
            Err(_) => {
                bail!("not found")
            }
        }
    }

    /// Performs Binary Search to find a record in the MemTable.
    ///
    /// If the record is found [Result::Ok] is returned, with the index of
    /// record. If the record is not found then [Result::Err] is returned,
    /// with the index to insert the record at.
    fn get_index(&self, key: &kv::Key) -> Result<usize, usize> {
        self.entries.binary_search_by_key(&key, |e| &e.key)
    }
}

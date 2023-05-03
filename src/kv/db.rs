use anyhow::{bail, Result};

use crate::kv::{batch::Batch, Key, Value};

use super::memtable::Memtable;

pub struct DB {
    memtable: Memtable,
}

impl DB {
    pub fn new() -> Self {
        Self {
            memtable: Memtable::new(),
        }
    }

    /// Sets the value for the given key. It overwrites any previous value
    /// for that key.
    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        let mut batch = Batch::new();
        batch.set(&key, &value)?;
        self.apply(&batch)
    }

    pub fn apply(&mut self, batch: &Batch) -> Result<()> {
        for (op, key, value) in batch.iter() {
            match op {
                super::Op::Set => {
                    self.memtable.set(key, value)?;
                }
                super::Op::Delete => {
                    self.memtable.delete(&key)?;
                }
            }
        }
        Ok(())
    }

    pub fn get(&self, key: &Key) -> Result<&Value> {
        match self.memtable.get(key) {
            Some(value) => Ok(value),
            None => bail!("not found"),
        }
    }

    pub fn delete(&mut self, key: &Key) -> Result<()> {
        let mut batch = Batch::new();
        batch.delete(&key)?;
        self.apply(&batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_set_get() -> Result<()> {
        let mut db = DB::new();

        let key = Key::try_from(10)?;
        let value = Value::try_from(20)?;

        db.set(key.clone(), value.clone())?;

        let value2 = db.get(&key)?;

        assert_eq!(&value, value2);

        Ok(())
    }

    #[test]
    pub fn test_set_overwrite_key() -> Result<()> {
        let mut db = DB::new();

        let key = Key::try_from(10)?;
        let value = Value::try_from(20)?;
        db.set(key.clone(), value.clone())?;

        let value = Value::try_from(50)?;
        db.set(key.clone(), value.clone())?;

        let value2 = db.get(&key)?;

        assert_eq!(&value, value2);

        Ok(())
    }

    #[test]
    pub fn test_delete() -> Result<()> {
        let mut db = DB::new();

        let key = Key::try_from(10)?;
        let value = Value::try_from(20)?;
        db.set(key.clone(), value.clone())?;

        db.delete(&key)?;

        let result = db.get(&key);
        assert!(
            result.is_err(),
            "Expected error to get deleted key: {:?}",
            result.err()
        );

        Ok(())
    }

    #[test]
    pub fn test_get_not_found() -> Result<()> {
        let db = DB::new();

        let key = Key::try_from(10)?;

        let result = db.get(&key);
        assert!(
            result.is_err(),
            "Expected error to get invalid key: {:?}",
            result.err()
        );

        Ok(())
    }

    #[test]
    pub fn test_delete_not_found() -> Result<()> {
        let mut db = DB::new();

        let key = Key::try_from(10)?;

        let result = db.delete(&key);
        assert!(
            result.is_err(),
            "Expected error to get invalid key: {:?}",
            result.err()
        );

        Ok(())
    }
}

use std::ops::Deref;

mod batch;
pub mod db;
mod memtable;

#[derive(Clone, Debug, PartialEq, Ord, Eq, PartialOrd)]
pub struct Key(Vec<u8>);

impl TryFrom<i32> for Key {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        let value = bincode::serialize(&value)?;
        Ok(Self(value))
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl Deref for Key {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Key {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Value(Vec<u8>);

impl TryFrom<i32> for Value {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        let value = bincode::serialize(&value)?;
        Ok(Self(value))
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Value {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Enumerates the kinds of operations on a kv: a deletion tombstone, a set
/// value, a update value, etc.
#[repr(u8)]
pub enum Op {
    Set = 1,
    Delete = 2,
}

impl From<u8> for Op {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Set,
            2 => Self::Delete,
            _ => panic!("invalid op code {}", value),
        }
    }
}

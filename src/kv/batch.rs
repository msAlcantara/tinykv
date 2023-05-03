use crate::kv::{Key, Op, Value};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct Batch {
    data: BytesMut,
}

impl Batch {
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
        }
    }

    pub fn set(&mut self, key: &Key, value: &Value) -> Result<()> {
        self.data.put_u8(Op::Set as u8);

        self.data.put_u32(key.len() as u32);
        self.data.extend_from_slice(key);

        self.data.put_u32(value.len() as u32);
        self.data.extend_from_slice(value);

        Ok(())
    }

    pub fn delete(&mut self, key: &Key) -> Result<()> {
        self.data.put_u8(Op::Delete as u8);

        self.data.put_u32(key.len() as u32);
        self.data.extend_from_slice(key);

        // Value len.
        self.data.put_u32(0);

        Ok(())
    }

    pub fn iter(&self) -> BatchIter {
        BatchIter {
            // TODO: Avoid clone the batch internal data.
            data: Bytes::from(self.data.clone()),
        }
    }
}

pub struct BatchIter {
    data: Bytes,
}

impl Iterator for BatchIter {
    type Item = (Op, Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.remaining() == 0 {
            // We consume the entire batch.
            return None;
        }

        let op = Op::from(self.data.get_u8());

        let key_len = self.data.get_u32();
        let mut key = vec![0; key_len as usize];
        self.data.copy_to_slice(&mut key);

        let value_len = self.data.get_u32();
        let mut value = vec![0; value_len as usize];
        self.data.copy_to_slice(&mut value);

        Some((op, Key::from(key), Value::from(value)))
    }
}

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::RwLock;

use crate::cache::cache::{value_type, Entry};
use crate::cache::encoding::Value;

pub struct Partition {
    store: RwLock<HashMap<Vec<u8>, Entry>>,
}

impl Partition {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        let inner = self.store.read().unwrap();
        inner.len()
    }

    pub fn each<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &Entry),
    {
        let inner = self.store.read().unwrap();
        for (k, v) in inner.deref() {
            f(k.as_slice(), v)
        }
    }

    pub fn entry(&self, key: &[u8]) -> Option<Entry> {
        let inner = self.store.read().unwrap();
        inner.get(key).map(|e| e.clone())
    }

    pub fn write(&self, key: &[u8], values: Vec<Value>) -> anyhow::Result<bool> {
        {
            let inner = self.store.read().unwrap();
            if let Some(e) = inner.get(key) {
                e.add(values.as_slice())?;
                return Ok(true);
            }
        }

        if values.len() == 0 {
            return Ok(true);
        }

        let vtype = value_type(&values[0]);

        let mut inner = self.store.write().unwrap();
        let e = inner
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(vtype));
        e.add(values.as_slice())?;

        return Ok(true);
    }

    pub fn add(&self, key: Vec<u8>, entry: Entry) {
        let mut inner = self.store.write().unwrap();
        inner.insert(key, entry);
    }

    pub fn remove(&self, key: &[u8]) {
        let mut inner = self.store.write().unwrap();
        inner.remove(key);
    }

    pub fn keys<F>(&self, mut cb: F)
    where
        F: FnMut(&[u8]),
    {
        let inner = self.store.read().unwrap();

        for (k, v) in inner.iter() {
            if v.count() == 0 {
                continue;
            }
            cb(k.as_slice());
        }
    }

    pub fn reset(&self) {
        let mut inner = self.store.write().unwrap();
        inner.clear();
    }

    pub fn count(&self) -> usize {
        let inner = self.store.write().unwrap();
        inner.iter().map(|e| e.1.count()).sum()
    }
}

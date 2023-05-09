use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::cache::cache::Entry;
use crate::cache::encoding::Value;
use crate::cache::partition::Partition;

const PARTITIONS: usize = 16;

pub struct Ring {
    pub keys_hint: AtomicU64,
    pub partitions: Vec<Arc<Partition>>,
}

impl Ring {
    pub fn new(n: usize) -> anyhow::Result<Self> {
        if n <= 0 || n > PARTITIONS {
            return Err(anyhow!(""));
        }

        let mut r = Self {
            keys_hint: AtomicU64::new(0),
            partitions: Vec::with_capacity(n),
        };

        for i in 0..n {
            r.partitions[i] = Arc::new(Partition::new());
        }

        return Ok(r);
    }

    pub fn reset(&self) {
        self.partitions.iter().for_each(|p| p.reset());
        self.keys_hint.store(0, Ordering::SeqCst);
    }

    pub fn get_partition(&self, key: &[u8]) -> &Partition {
        let n = murmur3::murmur3_x64_128(&mut Cursor::new(key), 0).unwrap();
        &self.partitions[(n % self.partitions.len() as u128) as usize]
    }

    pub fn entry(&self, key: &[u8]) -> Option<Entry> {
        self.get_partition(key).entry(key)
    }

    pub fn write(&self, key: &[u8], values: Vec<Value>) -> anyhow::Result<bool> {
        self.get_partition(key).write(key, values)
    }

    pub fn add(&self, key: Vec<u8>, entry: Entry) {
        self.get_partition(key.as_slice()).add(key, entry)
    }

    pub fn remove(&self, key: &[u8]) {
        self.get_partition(key).remove(key);
        if self.keys_hint.load(Ordering::SeqCst) > 0 {
            self.keys_hint.fetch_sub(1, Ordering::SeqCst);
        }
    }

    pub fn keys(&self, sorted: bool) -> Vec<Vec<u8>> {
        let mut keys = Vec::with_capacity(self.keys_hint.load(Ordering::SeqCst) as usize);
        for p in &self.partitions {
            p.keys(|key| {
                keys.push(key.to_vec());
            })
        }

        if sorted {
            keys.sort()
        }

        keys
    }

    pub fn count(&self) -> usize {
        self.partitions.iter().map(|p| p.count()).sum()
    }

    pub fn apply<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &Entry),
    {
        for p in &self.partitions {
            p.each(&mut f);
        }
    }

    pub fn split(&self, n: usize) -> Vec<Ring> {
        // let mut keys = 0;
        let mut stores = Vec::with_capacity(n);
        for i in 0..n {
            stores[i] = Ring::new(self.partitions.len()).unwrap();
        }

        for i in 0..self.partitions.len() {
            let p = &self.partitions[i];
            let r = &mut stores[i % n];
            r.partitions[i] = p.clone();
            // keys += p.len();
        }

        stores
    }
}

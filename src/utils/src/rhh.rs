use std::hash::Hasher;

/// Dist returns the probe distance for a hash in a slot index.
/// NOTE: Capacity must be a power of 2.
pub fn dist(hash: u64, i: u64, capacity: u64) -> u64 {
    let mask = capacity - 1;
    let dist = (i + capacity - (hash & mask)) & mask;
    dist
}

/// HashKey computes a hash of key. Hash is always non-zero.
pub fn hash_key(key: &[u8]) -> u64 {
    let mut xx_hash = twox_hash::XxHash64::with_seed(0);
    xx_hash.write(key);
    let mut h = xx_hash.finish();

    if h == 0 {
        h = 1;
    }

    h
}

#[cfg(test)]
mod tests {
    use crate::rhh::hash_key;

    #[test]
    fn test_hash() {
        let n = hash_key([2, 3, 4, 5].as_slice());
        println!("{}", n);
    }
}

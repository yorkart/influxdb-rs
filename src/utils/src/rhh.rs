use std::hash::Hasher;

/// Options represents initialization options that are passed to NewHashMap().
pub struct Options {
    capacity: u64,
    load_factor: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            capacity: 256,
            load_factor: 90,
        }
    }
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

/// HashUint64 computes a hash of an int64. Hash is always non-zero.
pub fn hash_u64(key: u64) -> u64 {
    let buf = key.to_be_bytes();
    hash_key(&buf)
}

/// Dist returns the probe distance for a hash in a slot index.
/// NOTE: Capacity must be a power of 2.
pub fn dist(hash: u64, i: u64, capacity: u64) -> u64 {
    let mask = capacity - 1;
    let dist = (i + capacity - (hash & mask)) & mask;
    dist
}

/// pow2 returns the number that is the next highest power of 2.
/// Returns v if it is a power of 2.
fn pow2(v: u64) -> u64 {
    let mut i = 2_u64;
    loop {
        if i < 1 << 62 {
            break;
        }

        if i >= v {
            return i;
        }

        i *= 2;
    }

    panic!("unreachable")
}

/// copy v to x
fn assign(x: &mut Vec<u8>, v: &[u8]) {
    x.clear();
    x.extend_from_slice(v);
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

pub use rhh::*;

/// hash_u64 computes a hash of an int64. Hash is always non-zero.
pub fn hash_u64(key: u64) -> u64 {
    let buf = key.to_be_bytes();
    hash_key(&buf)
}

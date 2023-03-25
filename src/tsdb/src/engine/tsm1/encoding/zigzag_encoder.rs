/// zig_zag_encode converts a int64 to a uint64 by zig zagging negative and positive values
/// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
#[inline]
pub fn zig_zag_encode(x: i64) -> u64 {
    (x << 1) as u64 ^ (x >> 63) as u64
}

/// zig_zag_decode converts a previously zigzag encoded uint64 back to a int64.
/// see: http://stackoverflow.com/a/2211086/56332
/// casting required because operations like unary negation
/// cannot be performed on unsigned integers
#[inline]
pub fn zig_zag_decode(v: u64) -> i64 {
    // ((v >> 1) ^ (-((v & 1) as i64)) as u64) as i64
    ((v >> 1) ^ ((((v & 1) as i64) << 63) >> 63) as u64) as i64
}

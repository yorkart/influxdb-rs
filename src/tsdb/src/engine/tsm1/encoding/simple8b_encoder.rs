use anyhow::anyhow;

/// maximum value that can be encoded.
pub const MAX_VALUE: u64 = (1 << 60) - 1;

/// Encoder converts a stream of unsigned 64bit integers to a compressed byte slice.
pub struct Encoder {
    /// most recently written integers that have not been flushed
    buf: [u64; 240],

    /// index in buf of the head of the buf
    h: usize,

    /// index in buf of the tail of the buf
    t: usize,

    /// index into bytes of written bytes
    bp: usize,

    /// current bytes written and flushed
    bytes: Vec<u8>,
    b: [u8; 8],
}

impl Encoder {
    pub fn new() -> Self {
        Self {
            buf: [0; 240],
            h: 0,
            t: 0,
            bp: 0,
            bytes: Vec::with_capacity(128),
            b: [0; 8],
        }
    }
}

impl Encoder {
    pub fn reset(&mut self) {
        self.t = 0;
        self.h = 0;
        self.bp = 0;

        self.buf.fill(0);
        self.b.fill(0);
        self.bytes.clear();
    }

    pub fn write(&mut self, v: u64) -> anyhow::Result<()> {
        if self.t >= self.buf.len() {
            self.flush()?;
        }

        // The buf is full but there is space at the front, just shift
        // the values down for now. TODO: use ring buffer
        if self.t >= self.buf.len() {
            let shift = self.buf[self.h..].to_vec();
            self.buf[..shift.len()].copy_from_slice(shift.as_slice());
            // self.buf.copy_from_slice(shift.as_slice());

            self.t -= self.h;
            self.h = 0;
        }
        self.buf[self.t] = v;
        self.t += 1;

        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        if self.t == 0 {
            return Ok(());
        }

        // encode as many values into one as we can
        let (encoded, n) = encode(&self.buf[self.h..self.t])?;

        let b: [u8; 8] = encoded.to_be_bytes();
        if self.bp + 8 > self.bytes.len() {
            self.bytes.extend_from_slice(b.as_slice());
            self.bp = self.bytes.len();
        } else {
            self.bytes.extend_from_slice(b.as_slice());
            self.bp += 8;
        }

        // Move the head forward since we encoded those values
        self.h += n;

        // If we encoded them all, reset the head/tail pointers to the beginning
        if self.h == self.t {
            self.h = 0;
            self.t = 0;
        }

        Ok(())
    }

    pub fn bytes(&mut self) -> anyhow::Result<&[u8]> {
        while self.t > 0 {
            self.flush()?;
        }

        Ok(&self.bytes[..self.bp])
    }
}

// Decoder converts a compressed byte slice to a stream of unsigned 64bit integers.
pub struct Decoder<'a> {
    bytes: &'a [u8],
    buf: [u64; 240],
    i: usize,
    n: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            buf: [0; 240],
            i: 0,
            n: 0,
        }
    }

    pub fn next(&mut self) -> bool {
        self.i += 1;

        if self.i >= self.n {
            self.read0();
        }

        self.bytes.len() >= 8 || self.i < self.n
    }

    // pub fn set_bytes(&mut self, b: &mut [u8]) {
    //     self.bytes = b;
    //     self.i = 0;
    //     self.n = 0;
    // }

    pub fn read(&self) -> u64 {
        let v = self.buf[self.i];
        v
    }

    fn read0(&mut self) {
        if self.bytes.len() < 8 {
            return;
        }

        let s = &self.bytes[..8];
        let v = u64::from_be_bytes(s.try_into().unwrap());

        self.bytes = self.bytes[8..].as_ref();
        self.n = decode(&mut self.buf, v).unwrap();
        self.i = 0;
    }
}

struct Packing {
    pub n: usize,
    pub bit: usize,
    pub unpack: fn(u64, &mut [u64]),
    pub pack: fn(&[u64]) -> u64,
}

impl Packing {
    pub fn new(n: usize, bit: usize, unpack: fn(u64, &mut [u64]), pack: fn(&[u64]) -> u64) -> Self {
        Self {
            n,
            bit,
            unpack,
            pack,
        }
    }
}

static SELECTOR: [Packing; 16] = [
    Packing {
        n: 240,
        bit: 0,
        unpack: unpack240,
        pack: pack240,
    },
    Packing {
        n: 120,
        bit: 0,
        unpack: unpack120,
        pack: pack120,
    },
    Packing {
        n: 60,
        bit: 1,
        unpack: unpack60,
        pack: pack60,
    },
    Packing {
        n: 30,
        bit: 2,
        unpack: unpack30,
        pack: pack30,
    },
    Packing {
        n: 20,
        bit: 3,
        unpack: unpack20,
        pack: pack20,
    },
    Packing {
        n: 15,
        bit: 4,
        unpack: unpack15,
        pack: pack15,
    },
    Packing {
        n: 12,
        bit: 5,
        unpack: unpack12,
        pack: pack12,
    },
    Packing {
        n: 10,
        bit: 6,
        unpack: unpack10,
        pack: pack10,
    },
    Packing {
        n: 8,
        bit: 7,
        unpack: unpack8,
        pack: pack8,
    },
    Packing {
        n: 7,
        bit: 8,
        unpack: unpack7,
        pack: pack7,
    },
    Packing {
        n: 6,
        bit: 10,
        unpack: unpack6,
        pack: pack6,
    },
    Packing {
        n: 5,
        bit: 12,
        unpack: unpack5,
        pack: pack5,
    },
    Packing {
        n: 4,
        bit: 15,
        unpack: unpack4,
        pack: pack4,
    },
    Packing {
        n: 3,
        bit: 20,
        unpack: unpack3,
        pack: pack3,
    },
    Packing {
        n: 2,
        bit: 30,
        unpack: unpack2,
        pack: pack2,
    },
    Packing {
        n: 1,
        bit: 60,
        unpack: unpack1,
        pack: pack1,
    },
];

fn count_bytes(b: &[u8]) -> anyhow::Result<usize> {
    let mut count = 0usize;
    let mut step = 0;
    while b.len() - step >= 8 {
        let s = &b[step..step + 8];
        let v = u64::from_be_bytes(s.try_into().unwrap());
        step += 8;

        let sel = (v >> 60) as usize;
        if sel >= 16 {
            return Err(anyhow!("invalid selector value: {}", sel));
        }
        count += SELECTOR[sel].n;
    }

    if b.len() - step > 0 {
        return Err(anyhow!("invalid slice len remaining: {}", b.len() - step));
    }
    Ok(count)
}

pub fn count_bytes_between(mut b: &[u8], min: u64, max: u64) -> anyhow::Result<usize> {
    let mut count = 0usize;
    while b.len() >= 8 {
        let mut v = u64::from_be_bytes(b[..8].try_into().unwrap());
        b = &b[8..];

        let sel = (v >> 60) as usize;
        if sel >= 16 {
            return Err(anyhow!("invalid selector value: {}", sel));
        }

        // If the max value that could be encoded by the uint64 is less than the min
        // skip the whole thing.
        let max_value = ((1u64 << SELECTOR[sel].bit) - 1) as u64;
        if max_value < min {
            continue;
        }

        // mask := uint64(^(int64(^0) << uint(selector[sel].bit)))
        let mask = (-1 ^ (-1_i64 << SELECTOR[sel].bit)) as u64;

        for _ in 0..SELECTOR[sel].n {
            let val = v & mask;
            if val >= min && val < max {
                count += 1;
            } else if val > max {
                break;
            }

            v = v >> SELECTOR[sel].bit;
        }
    }

    if b.len() > 0 {
        return Err(anyhow!("invalid slice len remaining: {}", b.len()));
    }

    Ok(count)
}

/// Encode packs as many values into a single uint64.  It returns the packed
/// uint64, how many values from src were packed, or an error if the values exceed
/// the maximum value range.
pub fn encode(src: &[u64]) -> anyhow::Result<(u64, usize)> {
    if can_pack(src, 240, 0) {
        Ok((0, 240))
    } else if can_pack(src, 120, 0) {
        Ok((1 << 60, 120))
    } else if can_pack(src, 60, 1) {
        Ok((pack60(&src[..60]), 60))
    } else if can_pack(src, 30, 2) {
        Ok((pack30(&src[..30]), 30))
    } else if can_pack(src, 20, 3) {
        Ok((pack20(&src[..20]), 20))
    } else if can_pack(src, 15, 4) {
        Ok((pack15(&src[..15]), 15))
    } else if can_pack(src, 12, 5) {
        Ok((pack12(&src[..12]), 12))
    } else if can_pack(src, 10, 6) {
        Ok((pack10(&src[..10]), 10))
    } else if can_pack(src, 8, 7) {
        Ok((pack8(&src[..8]), 8))
    } else if can_pack(src, 7, 8) {
        Ok((pack7(&src[..7]), 7))
    } else if can_pack(src, 6, 10) {
        Ok((pack6(&src[..6]), 6))
    } else if can_pack(src, 5, 12) {
        Ok((pack5(&src[..5]), 5))
    } else if can_pack(src, 4, 15) {
        Ok((pack4(&src[..4]), 4))
    } else if can_pack(src, 3, 20) {
        Ok((pack3(&src[..3]), 3))
    } else if can_pack(src, 2, 30) {
        Ok((pack2(&src[..2]), 2))
    } else if can_pack(src, 1, 60) {
        Ok((pack1(&src[..1]), 1))
    } else {
        if src.len() > 0 {
            Err(anyhow!("value out of bounds: {:?}", src))
        } else {
            Ok((0, 0))
        }
    }
}

/// Encode returns a packed slice of the values from src.  If a value is over
/// 1 << 60, an error is returned.  The input src is modified to avoid extra
/// allocations.  If you need to re-use, use a copy.
pub fn encode_all(src: &mut [u64]) -> anyhow::Result<usize> {
    let src_len = src.len();
    let mut i = 0;

    // Re-use the input slice and write encoded values back in place
    // let dst = src;
    let mut j = 0;

    loop {
        if i >= src_len {
            break;
        }
        let remaining = &src[i..];

        if can_pack(remaining, 240, 0) {
            src[j] = 0;
            i += 240;
        } else if can_pack(remaining, 120, 0) {
            src[j] = 1 << 60;
            i += 120;
        } else if can_pack(remaining, 60, 1) {
            src[j] = pack60(&src[i..i + 60]);
            i += 60;
        } else if can_pack(remaining, 30, 2) {
            src[j] = pack30(&src[i..i + 30]);
            i += 30;
        } else if can_pack(remaining, 20, 3) {
            src[j] = pack20(&src[i..i + 20]);
            i += 20;
        } else if can_pack(remaining, 15, 4) {
            src[j] = pack15(&src[i..i + 15]);
            i += 15;
        } else if can_pack(remaining, 12, 5) {
            src[j] = pack12(&src[i..i + 12]);
            i += 12;
        } else if can_pack(remaining, 10, 6) {
            src[j] = pack10(&src[i..i + 10]);
            i += 10;
        } else if can_pack(remaining, 8, 7) {
            src[j] = pack8(&src[i..i + 8]);
            i += 8;
        } else if can_pack(remaining, 7, 8) {
            src[j] = pack7(&src[i..i + 7]);
            i += 7;
        } else if can_pack(remaining, 6, 10) {
            src[j] = pack6(&src[i..i + 6]);
            i += 6;
        } else if can_pack(remaining, 5, 12) {
            src[j] = pack5(&src[i..i + 5]);
            i += 5;
        } else if can_pack(remaining, 4, 15) {
            src[j] = pack4(&src[i..i + 4]);
            i += 4;
        } else if can_pack(remaining, 3, 20) {
            src[j] = pack3(&src[i..i + 3]);
            i += 3;
        } else if can_pack(remaining, 2, 30) {
            src[j] = pack2(&src[i..i + 2]);
            i += 2;
        } else if can_pack(remaining, 1, 60) {
            src[j] = pack1(&src[i..i + 1]);
            i += 1;
        } else {
            return Err(anyhow!("value out of bounds"));
        }
        j += 1;
    }
    return Ok(j);
}

pub fn decode(dst: &mut [u64], v: u64) -> anyhow::Result<usize> {
    let sel = (v >> 60) as usize;
    if sel >= 16 {
        return Err(anyhow!("invalid selector value: {}", sel));
    }
    (SELECTOR[sel].unpack)(v, dst);
    return Ok(SELECTOR[sel].n);
}

/// Decode writes the uncompressed values from src to dst.  It returns the number
/// of values written or an error.
pub fn decode_all(dst: &mut [u64], src: &[u64]) -> anyhow::Result<usize> {
    let mut j = 0;
    for v in src {
        let sel = (v >> 60) as usize;
        if sel >= 16 {
            return Err(anyhow!("invalid selector value: {}", sel));
        }
        (SELECTOR[sel].unpack)(*v, dst);
        j += SELECTOR[sel].n;
    }
    return Ok(j);
}

fn can_pack(src: &[u64], n: usize, bits: usize) -> bool {
    if src.len() < n {
        return false;
    }

    let end = {
        let mut end = src.len();
        if n < end {
            end = n;
        }
        end
    };

    // Selector 0,1 are special and use 0 bits to encode runs of 1's
    if bits == 0 {
        for v in src {
            if *v != 1 {
                return false;
            }
        }
        return true;
    }

    let max = ((1u64 << bits as u64) - 1) as u64;

    for i in 0..end {
        if src[i] > max {
            return false;
        }
    }

    true
}

/// pack240 packs 240 ones from in using 1 bit each
fn pack240(_src: &[u64]) -> u64 {
    0
}

/// pack120 packs 120 ones from in using 1 bit each
fn pack120(_src: &[u64]) -> u64 {
    0
}

/// pack60 packs 60 values from in using 1 bit each
fn pack60(src: &[u64]) -> u64 {
    return 2 << 60
        | src[0]
        | src[1] << 1
        | src[2] << 2
        | src[3] << 3
        | src[4] << 4
        | src[5] << 5
        | src[6] << 6
        | src[7] << 7
        | src[8] << 8
        | src[9] << 9
        | src[10] << 10
        | src[11] << 11
        | src[12] << 12
        | src[13] << 13
        | src[14] << 14
        | src[15] << 15
        | src[16] << 16
        | src[17] << 17
        | src[18] << 18
        | src[19] << 19
        | src[20] << 20
        | src[21] << 21
        | src[22] << 22
        | src[23] << 23
        | src[24] << 24
        | src[25] << 25
        | src[26] << 26
        | src[27] << 27
        | src[28] << 28
        | src[29] << 29
        | src[30] << 30
        | src[31] << 31
        | src[32] << 32
        | src[33] << 33
        | src[34] << 34
        | src[35] << 35
        | src[36] << 36
        | src[37] << 37
        | src[38] << 38
        | src[39] << 39
        | src[40] << 40
        | src[41] << 41
        | src[42] << 42
        | src[43] << 43
        | src[44] << 44
        | src[45] << 45
        | src[46] << 46
        | src[47] << 47
        | src[48] << 48
        | src[49] << 49
        | src[50] << 50
        | src[51] << 51
        | src[52] << 52
        | src[53] << 53
        | src[54] << 54
        | src[55] << 55
        | src[56] << 56
        | src[57] << 57
        | src[58] << 58
        | src[59] << 59;
}

/// pack30 packs 30 values from in using 2 bits each
fn pack30(src: &[u64]) -> u64 {
    return 3 << 60
        | src[0]
        | src[1] << 2
        | src[2] << 4
        | src[3] << 6
        | src[4] << 8
        | src[5] << 10
        | src[6] << 12
        | src[7] << 14
        | src[8] << 16
        | src[9] << 18
        | src[10] << 20
        | src[11] << 22
        | src[12] << 24
        | src[13] << 26
        | src[14] << 28
        | src[15] << 30
        | src[16] << 32
        | src[17] << 34
        | src[18] << 36
        | src[19] << 38
        | src[20] << 40
        | src[21] << 42
        | src[22] << 44
        | src[23] << 46
        | src[24] << 48
        | src[25] << 50
        | src[26] << 52
        | src[27] << 54
        | src[28] << 56
        | src[29] << 58;
}

/// pack20 packs 20 values from in using 3 bits each
fn pack20(src: &[u64]) -> u64 {
    return 4 << 60
        | src[0]
        | src[1] << 3
        | src[2] << 6
        | src[3] << 9
        | src[4] << 12
        | src[5] << 15
        | src[6] << 18
        | src[7] << 21
        | src[8] << 24
        | src[9] << 27
        | src[10] << 30
        | src[11] << 33
        | src[12] << 36
        | src[13] << 39
        | src[14] << 42
        | src[15] << 45
        | src[16] << 48
        | src[17] << 51
        | src[18] << 54
        | src[19] << 57;
}

/// pack15 packs 15 values from in using 3 bits each
fn pack15(src: &[u64]) -> u64 {
    return 5 << 60
        | src[0]
        | src[1] << 4
        | src[2] << 8
        | src[3] << 12
        | src[4] << 16
        | src[5] << 20
        | src[6] << 24
        | src[7] << 28
        | src[8] << 32
        | src[9] << 36
        | src[10] << 40
        | src[11] << 44
        | src[12] << 48
        | src[13] << 52
        | src[14] << 56;
}

/// pack12 packs 12 values from in using 5 bits each
fn pack12(src: &[u64]) -> u64 {
    return 6 << 60
        | src[0]
        | src[1] << 5
        | src[2] << 10
        | src[3] << 15
        | src[4] << 20
        | src[5] << 25
        | src[6] << 30
        | src[7] << 35
        | src[8] << 40
        | src[9] << 45
        | src[10] << 50
        | src[11] << 55;
}

/// pack10 packs 10 values from in using 6 bits each
fn pack10(src: &[u64]) -> u64 {
    return 7 << 60
        | src[0]
        | src[1] << 6
        | src[2] << 12
        | src[3] << 18
        | src[4] << 24
        | src[5] << 30
        | src[6] << 36
        | src[7] << 42
        | src[8] << 48
        | src[9] << 54;
}

/// pack8 packs 8 values from in using 7 bits each
fn pack8(src: &[u64]) -> u64 {
    return 8 << 60
        | src[0]
        | src[1] << 7
        | src[2] << 14
        | src[3] << 21
        | src[4] << 28
        | src[5] << 35
        | src[6] << 42
        | src[7] << 49;
}

/// pack7 packs 7 values from in using 8 bits each
fn pack7(src: &[u64]) -> u64 {
    return 9 << 60
        | src[0]
        | src[1] << 8
        | src[2] << 16
        | src[3] << 24
        | src[4] << 32
        | src[5] << 40
        | src[6] << 48;
}

/// pack6 packs 6 values from in using 10 bits each
fn pack6(src: &[u64]) -> u64 {
    return 10 << 60
        | src[0]
        | src[1] << 10
        | src[2] << 20
        | src[3] << 30
        | src[4] << 40
        | src[5] << 50;
}

/// pack5 packs 5 values from in using 12 bits each
fn pack5(src: &[u64]) -> u64 {
    return 11 << 60 | src[0] | src[1] << 12 | src[2] << 24 | src[3] << 36 | src[4] << 48;
}

/// pack4 packs 4 values from in using 15 bits each
fn pack4(src: &[u64]) -> u64 {
    return 12 << 60 | src[0] | src[1] << 15 | src[2] << 30 | src[3] << 45;
}

/// pack3 packs 3 values from in using 20 bits each
fn pack3(src: &[u64]) -> u64 {
    return 13 << 60 | src[0] | src[1] << 20 | src[2] << 40;
}

/// pack2 packs 2 values from in using 30 bits each
fn pack2(src: &[u64]) -> u64 {
    return 14 << 60 | src[0] | src[1] << 30;
}

/// pack1 packs 1 values from in using 60 bits each
fn pack1(src: &[u64]) -> u64 {
    return 15 << 60 | src[0];
}

fn unpack240(_v: u64, dst: &mut [u64]) {
    for i in 0..dst.len() {
        dst[i] = 1;
    }
}

fn unpack120(_v: u64, dst: &mut [u64]) {
    for i in 0..dst.len() {
        dst[i] = 1;
    }
}

fn unpack60(v: u64, dst: &mut [u64]) {
    dst[0] = v & 1;
    dst[1] = (v >> 1) & 1;
    dst[2] = (v >> 2) & 1;
    dst[3] = (v >> 3) & 1;
    dst[4] = (v >> 4) & 1;
    dst[5] = (v >> 5) & 1;
    dst[6] = (v >> 6) & 1;
    dst[7] = (v >> 7) & 1;
    dst[8] = (v >> 8) & 1;
    dst[9] = (v >> 9) & 1;
    dst[10] = (v >> 10) & 1;
    dst[11] = (v >> 11) & 1;
    dst[12] = (v >> 12) & 1;
    dst[13] = (v >> 13) & 1;
    dst[14] = (v >> 14) & 1;
    dst[15] = (v >> 15) & 1;
    dst[16] = (v >> 16) & 1;
    dst[17] = (v >> 17) & 1;
    dst[18] = (v >> 18) & 1;
    dst[19] = (v >> 19) & 1;
    dst[20] = (v >> 20) & 1;
    dst[21] = (v >> 21) & 1;
    dst[22] = (v >> 22) & 1;
    dst[23] = (v >> 23) & 1;
    dst[24] = (v >> 24) & 1;
    dst[25] = (v >> 25) & 1;
    dst[26] = (v >> 26) & 1;
    dst[27] = (v >> 27) & 1;
    dst[28] = (v >> 28) & 1;
    dst[29] = (v >> 29) & 1;
    dst[30] = (v >> 30) & 1;
    dst[31] = (v >> 31) & 1;
    dst[32] = (v >> 32) & 1;
    dst[33] = (v >> 33) & 1;
    dst[34] = (v >> 34) & 1;
    dst[35] = (v >> 35) & 1;
    dst[36] = (v >> 36) & 1;
    dst[37] = (v >> 37) & 1;
    dst[38] = (v >> 38) & 1;
    dst[39] = (v >> 39) & 1;
    dst[40] = (v >> 40) & 1;
    dst[41] = (v >> 41) & 1;
    dst[42] = (v >> 42) & 1;
    dst[43] = (v >> 43) & 1;
    dst[44] = (v >> 44) & 1;
    dst[45] = (v >> 45) & 1;
    dst[46] = (v >> 46) & 1;
    dst[47] = (v >> 47) & 1;
    dst[48] = (v >> 48) & 1;
    dst[49] = (v >> 49) & 1;
    dst[50] = (v >> 50) & 1;
    dst[51] = (v >> 51) & 1;
    dst[52] = (v >> 52) & 1;
    dst[53] = (v >> 53) & 1;
    dst[54] = (v >> 54) & 1;
    dst[55] = (v >> 55) & 1;
    dst[56] = (v >> 56) & 1;
    dst[57] = (v >> 57) & 1;
    dst[58] = (v >> 58) & 1;
    dst[59] = (v >> 59) & 1;
}

fn unpack30(v: u64, dst: &mut [u64]) {
    dst[0] = v & 3;
    dst[1] = (v >> 2) & 3;
    dst[2] = (v >> 4) & 3;
    dst[3] = (v >> 6) & 3;
    dst[4] = (v >> 8) & 3;
    dst[5] = (v >> 10) & 3;
    dst[6] = (v >> 12) & 3;
    dst[7] = (v >> 14) & 3;
    dst[8] = (v >> 16) & 3;
    dst[9] = (v >> 18) & 3;
    dst[10] = (v >> 20) & 3;
    dst[11] = (v >> 22) & 3;
    dst[12] = (v >> 24) & 3;
    dst[13] = (v >> 26) & 3;
    dst[14] = (v >> 28) & 3;
    dst[15] = (v >> 30) & 3;
    dst[16] = (v >> 32) & 3;
    dst[17] = (v >> 34) & 3;
    dst[18] = (v >> 36) & 3;
    dst[19] = (v >> 38) & 3;
    dst[20] = (v >> 40) & 3;
    dst[21] = (v >> 42) & 3;
    dst[22] = (v >> 44) & 3;
    dst[23] = (v >> 46) & 3;
    dst[24] = (v >> 48) & 3;
    dst[25] = (v >> 50) & 3;
    dst[26] = (v >> 52) & 3;
    dst[27] = (v >> 54) & 3;
    dst[28] = (v >> 56) & 3;
    dst[29] = (v >> 58) & 3;
}

fn unpack20(v: u64, dst: &mut [u64]) {
    dst[0] = v & 7;
    dst[1] = (v >> 3) & 7;
    dst[2] = (v >> 6) & 7;
    dst[3] = (v >> 9) & 7;
    dst[4] = (v >> 12) & 7;
    dst[5] = (v >> 15) & 7;
    dst[6] = (v >> 18) & 7;
    dst[7] = (v >> 21) & 7;
    dst[8] = (v >> 24) & 7;
    dst[9] = (v >> 27) & 7;
    dst[10] = (v >> 30) & 7;
    dst[11] = (v >> 33) & 7;
    dst[12] = (v >> 36) & 7;
    dst[13] = (v >> 39) & 7;
    dst[14] = (v >> 42) & 7;
    dst[15] = (v >> 45) & 7;
    dst[16] = (v >> 48) & 7;
    dst[17] = (v >> 51) & 7;
    dst[18] = (v >> 54) & 7;
    dst[19] = (v >> 57) & 7;
}

fn unpack15(v: u64, dst: &mut [u64]) {
    dst[0] = v & 15;
    dst[1] = (v >> 4) & 15;
    dst[2] = (v >> 8) & 15;
    dst[3] = (v >> 12) & 15;
    dst[4] = (v >> 16) & 15;
    dst[5] = (v >> 20) & 15;
    dst[6] = (v >> 24) & 15;
    dst[7] = (v >> 28) & 15;
    dst[8] = (v >> 32) & 15;
    dst[9] = (v >> 36) & 15;
    dst[10] = (v >> 40) & 15;
    dst[11] = (v >> 44) & 15;
    dst[12] = (v >> 48) & 15;
    dst[13] = (v >> 52) & 15;
    dst[14] = (v >> 56) & 15;
}

fn unpack12(v: u64, dst: &mut [u64]) {
    dst[0] = v & 31;
    dst[1] = (v >> 5) & 31;
    dst[2] = (v >> 10) & 31;
    dst[3] = (v >> 15) & 31;
    dst[4] = (v >> 20) & 31;
    dst[5] = (v >> 25) & 31;
    dst[6] = (v >> 30) & 31;
    dst[7] = (v >> 35) & 31;
    dst[8] = (v >> 40) & 31;
    dst[9] = (v >> 45) & 31;
    dst[10] = (v >> 50) & 31;
    dst[11] = (v >> 55) & 31;
}

fn unpack10(v: u64, dst: &mut [u64]) {
    dst[0] = v & 63;
    dst[1] = (v >> 6) & 63;
    dst[2] = (v >> 12) & 63;
    dst[3] = (v >> 18) & 63;
    dst[4] = (v >> 24) & 63;
    dst[5] = (v >> 30) & 63;
    dst[6] = (v >> 36) & 63;
    dst[7] = (v >> 42) & 63;
    dst[8] = (v >> 48) & 63;
    dst[9] = (v >> 54) & 63;
}

fn unpack8(v: u64, dst: &mut [u64]) {
    dst[0] = v & 127;
    dst[1] = (v >> 7) & 127;
    dst[2] = (v >> 14) & 127;
    dst[3] = (v >> 21) & 127;
    dst[4] = (v >> 28) & 127;
    dst[5] = (v >> 35) & 127;
    dst[6] = (v >> 42) & 127;
    dst[7] = (v >> 49) & 127;
}

fn unpack7(v: u64, dst: &mut [u64]) {
    dst[0] = v & 255;
    dst[1] = (v >> 8) & 255;
    dst[2] = (v >> 16) & 255;
    dst[3] = (v >> 24) & 255;
    dst[4] = (v >> 32) & 255;
    dst[5] = (v >> 40) & 255;
    dst[6] = (v >> 48) & 255;
}

fn unpack6(v: u64, dst: &mut [u64]) {
    dst[0] = v & 1023;
    dst[1] = (v >> 10) & 1023;
    dst[2] = (v >> 20) & 1023;
    dst[3] = (v >> 30) & 1023;
    dst[4] = (v >> 40) & 1023;
    dst[5] = (v >> 50) & 1023;
}

fn unpack5(v: u64, dst: &mut [u64]) {
    dst[0] = v & 4095;
    dst[1] = (v >> 12) & 4095;
    dst[2] = (v >> 24) & 4095;
    dst[3] = (v >> 36) & 4095;
    dst[4] = (v >> 48) & 4095;
}

fn unpack4(v: u64, dst: &mut [u64]) {
    dst[0] = v & 32767;
    dst[1] = (v >> 15) & 32767;
    dst[2] = (v >> 30) & 32767;
    dst[3] = (v >> 45) & 32767;
}

fn unpack3(v: u64, dst: &mut [u64]) {
    dst[0] = v & 1048575;
    dst[1] = (v >> 20) & 1048575;
    dst[2] = (v >> 40) & 1048575;
}

fn unpack2(v: u64, dst: &mut [u64]) {
    dst[0] = v & 1073741823;
    dst[1] = (v >> 30) & 1073741823;
}

fn unpack1(v: u64, dst: &mut [u64]) {
    dst[0] = v & 1152921504606846975;
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::encoding::simple8b_encoder::{
        count_bytes, count_bytes_between, decode_all, encode_all, Decoder, Encoder,
    };

    #[test]
    fn test_encode_no_values() {
        let mut src = vec![];

        // check for error
        let encoded = {
            let sz = encode_all(&mut src).expect("failed to encode src");
            &src[..sz]
        };

        let mut decoded = vec![];
        let n = decode_all(decoded.as_mut_slice(), encoded).expect("failed to decode src");

        // verify encoded no values.
        assert_eq!(src.len(), decoded[..n].len());
    }

    #[test]
    fn test_too_big() {
        let values = 1;
        let mut src = Vec::with_capacity(values);
        for _ in 0..values {
            src.push(2 << 61 - 1);
        }

        let r = encode_all(src.as_mut_slice());
        assert_eq!(r.is_err(), true);
    }

    #[test]
    fn test_few_values() {
        test_encode(20, 2);
    }

    #[test]
    fn test_encode_multiple_zeros() {
        test_encode(250, 0)
    }

    #[test]
    fn test_encode_multiple_ones() {
        test_encode(250, 1);
    }

    #[test]
    fn test_encode_multiple_large() {
        test_encode(250, 134);
    }

    #[test]
    fn test_encode_240ones() {
        test_encode(240, 1);
    }

    #[test]
    fn test_encode_120ones() {
        test_encode(120, 1);
    }

    #[test]
    fn test_encode_60() {
        test_encode(60, 1);
    }

    #[test]
    fn test_encode_30() {
        test_encode(30, 3);
    }

    #[test]
    fn test_encode_20() {
        test_encode(20, 7);
    }

    #[test]
    fn test_encode_15() {
        test_encode(15, 15);
    }

    #[test]
    fn test_encode_12() {
        test_encode(12, 31);
    }

    #[test]
    fn test_encode_10() {
        test_encode(10, 63);
    }

    #[test]
    fn test_encode_8() {
        test_encode(8, 127);
    }

    #[test]
    fn test_encode_7() {
        test_encode(7, 255);
    }

    #[test]
    fn test_encode_6() {
        test_encode(6, 1023);
    }

    #[test]
    fn test_encode_5() {
        test_encode(5, 4095);
    }

    #[test]
    fn test_encode_4() {
        test_encode(4, 32767);
    }

    #[test]
    fn test_encode_3() {
        test_encode(3, 1048575);
    }

    #[test]
    fn test_encode_2() {
        test_encode(2, 1073741823);
    }

    #[test]
    fn test_encode_1() {
        test_encode(1, 1152921504606846975);
    }

    fn test_encode(n: usize, val: u64) {
        let mut enc = Encoder::new();
        let mut src = Vec::with_capacity(n);
        for _ in 0..n {
            src.push(val);
            enc.write(val).unwrap();
        }

        let encoded = enc.bytes().unwrap();

        let mut dec = Decoder::new(encoded);
        let mut i = 0;
        while dec.next() {
            assert_eq!(
                i >= src.len(),
                false,
                "Decoded too many values: got {}, exp {}",
                i,
                src.len()
            );
            assert_eq!(
                dec.read(),
                src[i],
                "Decoded[{}] != {}, got {}",
                i,
                src[i],
                dec.read()
            );
            i += 1;
        }

        let exp = n;
        let got = i;

        assert_eq!(got, exp, "Decode len mismatch: exp {}, got {}", exp, got);

        let n = count_bytes(encoded).unwrap();
        assert_eq!(i, n, "Count mismatch: got {}, exp {}", got, n);
    }

    #[test]
    fn test_bytes() {
        let mut enc = Encoder::new();
        for i in 0..30 {
            enc.write(i as u64).unwrap();
        }
        let b = enc.bytes().unwrap();

        let mut dec = Decoder::new(b);
        let mut x = 0u64;
        while dec.next() {
            assert_eq!(x, dec.read(), "mismatch: got {}, exp {}", dec.read(), x);
            x += 1;
        }
    }

    #[test]
    fn test_encode_value_too_large() {
        let mut enc = Encoder::new();

        let values = [1442369134000000000u64, 0u64];

        for v in values {
            enc.write(v).unwrap();
        }

        let r = enc.bytes();
        assert_eq!(r.is_err(), true, "Expected error, got nil");
    }

    #[test]
    fn test_decode_not_enough_bytes() {
        let mut dec = Decoder::new([0].as_slice());
        assert_eq!(
            dec.next(),
            false,
            "Expected Next to return false but it returned true"
        );
    }

    #[test]
    fn test_count_bytes_between() {
        let mut enc = Encoder::new();
        let mut src = [0u64; 8];
        for i in 0..src.len() {
            src[i] = i as u64;
            enc.write(src[i]).unwrap();
        }

        let encoded = enc.bytes().unwrap();

        let mut dec = Decoder::new(encoded);
        let mut i = 0;
        while dec.next() {
            assert_eq!(
                i >= src.len(),
                false,
                "Decoded too many values: got {}, exp {}",
                i,
                src.len()
            );
            assert_eq!(
                dec.read(),
                src[i],
                "Decoded[{}] != {}, got {}",
                i,
                src[i],
                dec.read()
            );
            i += 1;
        }

        let exp = src.len();
        let got = i;

        assert_eq!(got, exp, "Decode len mismatch: exp {}, got {}", exp, got);

        let got = count_bytes_between(encoded, 2, 6).expect("Unexpected error in Count");
        assert_eq!(got, 4, "Count mismatch: got {}, exp {}", got, 4);
    }

    #[test]
    fn test_count_bytes_between_skip_min() {
        let mut enc = Encoder::new();
        let mut src = [0u64; 9];
        for i in 0..src.len() - 1 {
            src[i] = i as u64;
            enc.write(src[i]).unwrap();
        }
        src[8] = 100000;
        enc.write(src[8]).unwrap();

        let encoded = enc.bytes().unwrap();

        let mut dec = Decoder::new(encoded);
        let mut i = 0;
        while dec.next() {
            assert_eq!(
                i >= src.len(),
                false,
                "Decoded too many values: got {}, exp {}",
                i,
                src.len()
            );
            assert_eq!(
                dec.read(),
                src[i],
                "Decoded[{}] != {}, got {}",
                i,
                src[i],
                dec.read()
            );
            i += 1;
        }

        let exp = src.len();
        let got = i;

        assert_eq!(got, exp, "Decode len mismatch: exp {}, got {}", exp, got);

        let got = count_bytes_between(encoded, 100000, 100001).expect("Unexpected error in Count");
        assert_eq!(got, 1, "Count mismatch: got {}, exp {}", got, 1);
    }
}

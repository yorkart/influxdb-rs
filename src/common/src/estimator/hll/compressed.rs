use std::io::{Cursor, Read};

use bytes::{Buf, BufMut};

pub(crate) trait Iterable {
    fn decode(&self, i: usize, last: u32) -> (u32, usize);
    fn len(&self) -> usize;
    fn iter(&self) -> Iterator<Self>
    where
        Self: Sized;
}

pub(crate) struct Iterator<'a, I: Iterable> {
    i: usize,
    last: u32,
    v: &'a I,
}

impl<'a, I: Iterable> Iterator<'a, I> {
    pub fn new(i: usize, last: u32, v: &'a I) -> Self {
        Self { i, last, v }
    }

    pub fn next(&mut self) -> u32 {
        let (n, i) = self.v.decode(self.i, self.last);
        self.last = n;
        self.i = i;
        n
    }

    pub fn peek(&self) -> u32 {
        let (n, _) = self.v.decode(self.i, self.last);
        n
    }

    pub fn has_next(&self) -> bool {
        self.i < self.v.len()
    }
}

#[derive(Clone)]
pub(crate) struct CompressedList {
    count: u32,
    last: u32,
    b: Vec<u8>,
}

impl Iterable for CompressedList {
    fn decode(&self, i: usize, _last: u32) -> (u32, usize) {
        let v = self.b.as_slice();

        let mut x = 0_u32;
        let mut j = i;
        while v[j] & 0x80 != 0 {
            x |= ((v[j] & 0x7f) as u32) << (((j - i) as u32) * 7);
            j += 1;
        }
        x |= (v[j] as u32) << ((j - i) as u32 * 7);
        (x, j + 1)
    }

    fn len(&self) -> usize {
        self.b.len()
    }

    fn iter(&self) -> Iterator<Self> {
        Iterator {
            i: 0,
            last: 0,
            v: self,
        }
    }
}
impl CompressedList {
    pub fn new(size: usize) -> Self {
        Self {
            count: 0,
            last: 0,
            b: Vec::with_capacity(size),
        }
    }

    pub fn append(&mut self, x: u32) {
        self.count += 1;
        // self.b.push((x - self.last) as u8);

        let mut x = x - self.last;
        while x & 0xffffff80 != 0 {
            self.b.push(((x & 0x7f) | 0x80) as u8);
            x >>= 7;
        }
        self.b.push((x & 0x7f) as u8);

        self.last = x;
    }

    pub fn encode(&self) -> Vec<u8> {
        let len = 4 + self.b.len() + 4 + 4;
        let mut data = Vec::with_capacity(len);
        data.put_u32(self.count);
        data.put_u32(self.last);
        data.put_u32(self.b.len() as u32);
        data.extend_from_slice(self.b.as_slice());
        data
    }
}

impl<'a> TryFrom<&'a [u8]> for CompressedList {
    type Error = anyhow::Error;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let mut cursor = Cursor::new(value);
        if cursor.remaining() < 4 {
            return Err(anyhow!(""));
        }
        let count = cursor.get_u32();

        if cursor.remaining() < 4 {
            return Err(anyhow!(""));
        }
        let last = cursor.get_u32();

        if cursor.remaining() < 4 {
            return Err(anyhow!(""));
        }
        let sz = cursor.get_u32() as usize;

        if cursor.remaining() < sz {
            return Err(anyhow!(""));
        }
        let mut b = Vec::with_capacity(sz);
        b.resize(sz, 0);
        cursor.read(&mut b)?;

        Ok(Self { count, last, b })
    }
}

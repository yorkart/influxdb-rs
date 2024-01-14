use std::any::Any;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use crate::engine::tsm1::value::value::{TimeValue, Value};
use crate::engine::tsm1::value::FieldType;

pub trait Array: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn min_time(&self) -> i64;
    fn max_time(&self) -> i64;
    fn len(&self) -> usize;
    fn clear(&mut self);
    fn size(&self) -> usize;
    fn ordered(&self) -> bool;
    fn deduplicate(&mut self);
    fn exclude(&mut self, min: i64, max: i64);
    fn include(&mut self, min: i64, max: i64);
    fn find_range(&self, min: i64, max: i64) -> (isize, isize);
    // fn merge(self, b: Box<dyn Array>) -> Box<dyn Array>;
    fn decode(&mut self, block: &[u8]) -> anyhow::Result<()>;
    fn decode_v1(block: &[u8]) -> anyhow::Result<Box<dyn Array>>
    where
        Self: Sized;
}

pub type ArrayRef = Box<dyn Array>;

#[derive(Debug)]
pub struct ReuseVec<T> {
    vec: Vec<T>,
    start: usize,
}

impl<T> ReuseVec<T> {
    pub fn new() -> Self {
        Self {
            vec: Vec::new(),
            start: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vec: Vec::with_capacity(capacity),
            start: 0,
        }
    }

    pub fn push(&mut self, v: T) {
        self.vec.push(v);
    }

    pub fn skip(&mut self, n: usize) {
        self.start += n;
    }

    pub fn truncate(&mut self, n: usize) {
        self.vec.truncate(n + self.start);
    }

    pub fn len(&self) -> usize {
        self.vec.len() - self.start
    }

    pub fn capacity(&self) -> usize {
        self.vec.capacity() - self.start
    }
}

impl<T> Deref for ReuseVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.vec[self.start..]
    }
}

impl<T> DerefMut for ReuseVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.vec[self.start..]
    }
}

pub type TypeValues<T> = Vec<TimeValue<T>>;

impl<T> Array for TypeValues<T>
where
    T: FieldType + 'static,
    TimeValue<T>: Value,
{
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn min_time(&self) -> i64 {
        self[0].unix_nano
    }

    fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn size(&self) -> usize {
        self.iter().map(|x| x.encode_size()).sum()
    }

    fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            let a = self[i - 1].unix_nano;
            let b = self[i].unix_nano;
            if a >= b {
                return false;
            }
        }
        return true;
    }

    fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        if self.ordered() {
            return;
        }

        self.sort_by(|x, y| x.unix_nano.cmp(&y.unix_nano));
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano != self[i].unix_nano {
                i += 1;
            }
            self[i] = v.clone();
        }

        i += 1;
        if i == self.len() {
            return;
        }

        self.truncate(i);
    }

    /// Exclude returns the subset of values not in [min, max].  The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if rmax < self.len() as isize {
            if self[rmax as usize].unix_nano == max {
                rmax += 1;
            }
            let rest = self.len() as isize - rmax;
            if rest > 0 {
                let right = self[rmax as usize..].to_vec();
                self.truncate((rmin + rest) as usize);
                self.extend_from_slice(right.as_slice());

                return;
            }
        }

        self.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if rmax < self.len() as isize && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            let b = self[rmin as usize..rmax as usize].to_vec();
            self.clear();
            self.extend_from_slice(b.as_slice());
            return;
        }

        self.truncate(rmax as usize);
    }

    /// FindRange returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    fn find_range(&self, min: i64, max: i64) -> (isize, isize) {
        if self.len() == 0 || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano;
        let max_val = self[self.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (search(self, min) as isize, search(self, max) as isize)
    }

    // /// Merge overlays b to top of a.  If two values conflict with
    // /// the same timestamp, b is used.  Both a and b must be sorted
    // /// in ascending order.
    // fn merge(mut self, mut b: Box<dyn Array>) -> Box<dyn Array> {
    //     if self.len() == 0 {
    //         return b;
    //     }
    //     if b.len() == 0 {
    //         return self;
    //     }
    //
    //     // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
    //     // possible stored blocks might contain duplicate values.  Remove them if they exists before
    //     // merging.
    //     self.deduplicate();
    //     b.deduplicate();
    //
    //     if self[self.len() - 1].unix_nano < b[0].unix_nano {
    //         self.extend_from_slice(b.as_slice());
    //         return self;
    //     }
    //
    //     if b[b.len() - 1].unix_nano < self[0].unix_nano {
    //         b.extend_from_slice(self.as_slice());
    //         return b;
    //     }
    //
    //     let mut out = Vec::with_capacity(self.len() + b.len());
    //     let mut a = self.as_slice();
    //     let mut b = b.as_slice();
    //
    //     while a.len() > 0 && b.len() > 0 {
    //         if a[0].unix_nano < b[0].unix_nano {
    //             out.push(a[0].clone());
    //             a = &a[1..];
    //         } else if b.len() > 0 && a[0].unix_nano == b[0].unix_nano {
    //             a = &a[1..];
    //         } else {
    //             out.push(b[0].clone());
    //             b = &b[1..];
    //         }
    //     }
    //
    //     if a.len() > 0 {
    //         out.extend_from_slice(a);
    //     }
    //     if b.len() > 0 {
    //         out.extend_from_slice(b);
    //     }
    //
    //     out
    // }

    fn decode(&mut self, block: &[u8]) -> anyhow::Result<()> {
        Value::decode(self, block)
    }

    fn decode_v1(block: &[u8]) -> anyhow::Result<Box<dyn Array>>
    where
        Self: Sized,
    {
        let mut values: Vec<TimeValue<T>> = vec![];
        Value::decode(&mut values, block)?;
        Ok(Box::new(values))
    }
}

pub type FloatValues = TypeValues<f64>;
pub type IntegerValues = TypeValues<i64>;
pub type BooleanValues = TypeValues<bool>;
pub type StringValues = TypeValues<Vec<u8>>;
pub type UnsignedValues = TypeValues<u64>;

/// Values describes the various types of block data that can be held within a TSM file.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Values {
    Float(FloatValues),
    Integer(IntegerValues),
    Bool(BooleanValues),
    String(StringValues),
    Unsigned(UnsignedValues),
}

impl Default for Values {
    fn default() -> Self {
        Values::Float(vec![])
    }
}

impl Into<FloatValues> for Values {
    fn into(self) -> FloatValues {
        match self {
            Self::Float(values) => values,
            _ => panic!("xx"),
        }
    }
}

impl Into<IntegerValues> for Values {
    fn into(self) -> IntegerValues {
        match self {
            Self::Integer(values) => values,
            _ => panic!("xx"),
        }
    }
}

// impl TryFrom<(u8, &[u8])> for Values {
//     type Error = anyhow::Error;
//
//     fn try_from((typ, block): (u8, &[u8])) -> Result<Self, Self::Error> {
//         match typ {
//             0 => {
//                 let mut values = FloatValues::decode_v1(block)?;
//                 Ok(Values::Float(values))
//             }
//             1 => {
//                 let mut values: TypeValues<i64> = Vec::with_capacity(0);
//                 values.decode(block)?;
//                 Ok(Values::Integer(values))
//             }
//             2 => {
//                 let mut values: TypeValues<bool> = Vec::with_capacity(0);
//                 values.decode(block)?;
//                 Ok(Values::Bool(values))
//             }
//             3 => {
//                 let mut values: TypeValues<Vec<u8>> = Vec::with_capacity(0);
//                 values.decode(block)?;
//                 Ok(Values::String(values))
//             }
//             4 => {
//                 let mut values: TypeValues<u64> = Vec::with_capacity(0);
//                 values.decode(block)?;
//                 Ok(Values::Unsigned(values))
//             }
//             _ => Err(anyhow!("unsupported type {}", typ)),
//         }
//     }
// }

impl Values {
    pub fn len(&self) -> usize {
        match self {
            Self::Float(values) => values.len(),
            Self::Integer(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::String(values) => values.len(),
            Self::Unsigned(values) => values.len(),
        }
    }
}

impl Array for Values {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        todo!()
    }

    fn min_time(&self) -> i64 {
        match self {
            Self::Float(values) => values.min_time(),
            Self::Integer(values) => values.min_time(),
            Self::Bool(values) => values.min_time(),
            Self::String(values) => values.min_time(),
            Self::Unsigned(values) => values.min_time(),
        }
    }

    fn max_time(&self) -> i64 {
        match self {
            Self::Float(values) => values.max_time(),
            Self::Integer(values) => values.max_time(),
            Self::Bool(values) => values.max_time(),
            Self::String(values) => values.max_time(),
            Self::Unsigned(values) => values.max_time(),
        }
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn clear(&mut self) {
        todo!()
    }

    fn size(&self) -> usize {
        match self {
            Self::Float(values) => values.size(),
            Self::Integer(values) => values.size(),
            Self::Bool(values) => values.size(),
            Self::String(values) => values.size(),
            Self::Unsigned(values) => values.size(),
        }
    }

    fn ordered(&self) -> bool {
        match self {
            Self::Float(values) => values.ordered(),
            Self::Integer(values) => values.ordered(),
            Self::Bool(values) => values.ordered(),
            Self::String(values) => values.ordered(),
            Self::Unsigned(values) => values.ordered(),
        }
    }

    fn deduplicate(&mut self) {
        match self {
            Self::Float(values) => values.deduplicate(),
            Self::Integer(values) => values.deduplicate(),
            Self::Bool(values) => values.deduplicate(),
            Self::String(values) => values.deduplicate(),
            Self::Unsigned(values) => values.deduplicate(),
        }
    }

    fn exclude(&mut self, min: i64, max: i64) {
        match self {
            Self::Float(values) => values.exclude(min, max),
            Self::Integer(values) => values.exclude(min, max),
            Self::Bool(values) => values.exclude(min, max),
            Self::String(values) => values.exclude(min, max),
            Self::Unsigned(values) => values.exclude(min, max),
        }
    }

    fn include(&mut self, min: i64, max: i64) {
        match self {
            Self::Float(values) => values.include(min, max),
            Self::Integer(values) => values.include(min, max),
            Self::Bool(values) => values.include(min, max),
            Self::String(values) => values.include(min, max),
            Self::Unsigned(values) => values.include(min, max),
        }
    }

    fn find_range(&self, min: i64, max: i64) -> (isize, isize) {
        match self {
            Self::Float(values) => values.find_range(min, max),
            Self::Integer(values) => values.find_range(min, max),
            Self::Bool(values) => values.find_range(min, max),
            Self::String(values) => values.find_range(min, max),
            Self::Unsigned(values) => values.find_range(min, max),
        }
    }

    // fn merge(self, b: Self) -> Self {
    //     match self {
    //         Self::Float(values) => {
    //             if let Self::Float(values_b) = b {
    //                 Self::Float(values.merge(values_b))
    //             } else {
    //                 panic!("expect Float values")
    //             }
    //         }
    //         Self::Integer(values) => {
    //             if let Self::Integer(values_b) = b {
    //                 Self::Integer(values.merge(values_b))
    //             } else {
    //                 panic!("expect Float values")
    //             }
    //         }
    //         Self::Bool(values) => {
    //             if let Self::Bool(values_b) = b {
    //                 Self::Bool(values.merge(values_b))
    //             } else {
    //                 panic!("expect Float values")
    //             }
    //         }
    //         Self::String(values) => {
    //             if let Self::String(values_b) = b {
    //                 Self::String(values.merge(values_b))
    //             } else {
    //                 panic!("expect Float values")
    //             }
    //         }
    //         Self::Unsigned(values) => {
    //             if let Self::Unsigned(values_b) = b {
    //                 Self::Unsigned(values.merge(values_b))
    //             } else {
    //                 panic!("expect Float values")
    //             }
    //         }
    //     }
    // }

    fn decode(&mut self, block: &[u8]) -> anyhow::Result<()> {
        match self {
            Self::Float(values) => values.decode(block),
            Self::Integer(values) => values.decode(block),
            Self::Bool(values) => values.decode(block),
            Self::String(values) => values.decode(block),
            Self::Unsigned(values) => values.decode(block),
        }
    }

    fn decode_v1(_block: &[u8]) -> anyhow::Result<Box<dyn Array>> {
        todo!()
    }
}

/// search performs a binary search for UnixNano() v in a
/// and returns the position, i, where v would be inserted.
/// An additional check of a[i].UnixNano() == v is necessary
/// to determine if the value v exists.
fn search<T>(values: &[TimeValue<T>], v: i64) -> usize
where
    T: FieldType,
{
    // Define: f(x) → a[x].UnixNano() < v
    // Define: f(-1) == true, f(n) == false
    // Invariant: f(lo-1) == true, f(hi) == false
    let mut lo = 0usize;
    let mut hi = values.len();
    while lo < hi {
        let mid = (lo + hi) >> 1;
        if values[mid].unix_nano < v {
            lo = mid + 1; // preserves f(lo-1) == true
        } else {
            hi = mid; // preserves f(hi) == false
        }
    }

    // lo == hi
    lo
}

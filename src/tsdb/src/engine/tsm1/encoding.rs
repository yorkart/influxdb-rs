use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Value<T>
where
    T: Debug + Clone + PartialOrd + PartialEq,
{
    pub unix_nano: i64,
    pub value: T,
}

impl<T> Value<T>
where
    T: Debug + Clone + PartialOrd + PartialEq,
{
    pub fn new(unix_nano: i64, value: T) -> Self {
        Self { unix_nano, value }
    }
}

pub trait Capacity {
    fn encode_size(&self) -> usize;
}

impl Capacity for Value<f64> {
    fn encode_size(&self) -> usize {
        16
    }
}

impl Capacity for Value<i64> {
    fn encode_size(&self) -> usize {
        16
    }
}

impl Capacity for Value<u64> {
    fn encode_size(&self) -> usize {
        16
    }
}

impl Capacity for Value<bool> {
    fn encode_size(&self) -> usize {
        9
    }
}

impl Capacity for Value<Vec<u8>> {
    fn encode_size(&self) -> usize {
        8 + self.value.len()
    }
}

pub trait TValues {
    fn min_time(&self) -> i64;
    fn max_time(&self) -> i64;
    fn size(&self) -> usize;
    fn ordered(&self) -> bool;
    fn deduplicate(self) -> Self;
    fn exclude(self, min: i64, max: i64) -> Self;
    fn include(self, min: i64, max: i64) -> Self;
    fn find_range(&self, min: i64, max: i64) -> (isize, isize);
    fn merge(self, b: Self) -> Self;
}

pub type TypeValue<T> = Vec<Value<T>>;

impl<T> TValues for TypeValue<T>
where
    T: Debug + Clone + PartialOrd + PartialEq,
    Value<T>: Capacity,
{
    fn min_time(&self) -> i64 {
        self[0].unix_nano
    }

    fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano
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

    fn deduplicate(mut self) -> Self {
        if self.len() <= 1 {
            return self;
        }

        if self.ordered() {
            return self;
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
            return self;
        }

        self.truncate(i);
        self
    }

    fn exclude(mut self, min: i64, max: i64) -> Self {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return self;
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

                return self;
            }
        }

        self.truncate(rmin as usize);
        self
    }

    fn include(mut self, min: i64, max: i64) -> Self {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return vec![];
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if rmax < self.len() as isize && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            return self[rmin as usize..rmax as usize].to_vec();
        }

        self.truncate(rmax as usize);
        self
    }

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

    fn merge(mut self, mut b: Self) -> Self {
        if self.len() == 0 {
            return b;
        }
        if b.len() == 0 {
            return self;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        self = self.deduplicate();
        b = b.deduplicate();

        if self[self.len() - 1].unix_nano < b[0].unix_nano {
            self.extend_from_slice(b.as_slice());
            return self;
        }

        if b[b.len() - 1].unix_nano < self[0].unix_nano {
            b.extend_from_slice(self.as_slice());
            return b;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();

        while a.len() > 0 && b.len() > 0 {
            if a[0].unix_nano < b[0].unix_nano {
                out.push(a[0].clone());
                a = &a[1..];
            } else if b.len() > 0 && a[0].unix_nano == b[0].unix_nano {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if a.len() > 0 {
            out.extend_from_slice(a);
        }
        if b.len() > 0 {
            out.extend_from_slice(b);
        }

        out
    }
}

/// Values describes the various types of block data that can be held within a TSM file.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Values {
    Float(Vec<Value<f64>>),
    Integer(Vec<Value<i64>>),
    Bool(Vec<Value<bool>>),
    Str(Vec<Value<Vec<u8>>>),
    Unsigned(Vec<Value<u64>>),
}

impl TValues for Values {
    fn min_time(&self) -> i64 {
        match self {
            Self::Float(values) => values.min_time(),
            Self::Integer(values) => values.min_time(),
            Self::Bool(values) => values.min_time(),
            Self::Str(values) => values.min_time(),
            Self::Unsigned(values) => values.min_time(),
        }
    }

    fn max_time(&self) -> i64 {
        match self {
            Self::Float(values) => values.max_time(),
            Self::Integer(values) => values.max_time(),
            Self::Bool(values) => values.max_time(),
            Self::Str(values) => values.max_time(),
            Self::Unsigned(values) => values.max_time(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Float(values) => values.size(),
            Self::Integer(values) => values.size(),
            Self::Bool(values) => values.size(),
            Self::Str(values) => values.size(),
            Self::Unsigned(values) => values.size(),
        }
    }

    fn ordered(&self) -> bool {
        match self {
            Self::Float(values) => values.ordered(),
            Self::Integer(values) => values.ordered(),
            Self::Bool(values) => values.ordered(),
            Self::Str(values) => values.ordered(),
            Self::Unsigned(values) => values.ordered(),
        }
    }

    fn deduplicate(self) -> Self {
        match self {
            Self::Float(values) => Self::Float(values.deduplicate()),
            Self::Integer(values) => Self::Integer(values.deduplicate()),
            Self::Bool(values) => Self::Bool(values.deduplicate()),
            Self::Str(values) => Self::Str(values.deduplicate()),
            Self::Unsigned(values) => Self::Unsigned(values.deduplicate()),
        }
    }

    fn exclude(self, min: i64, max: i64) -> Self {
        match self {
            Self::Float(values) => Self::Float(values.exclude(min, max)),
            Self::Integer(values) => Self::Integer(values.exclude(min, max)),
            Self::Bool(values) => Self::Bool(values.exclude(min, max)),
            Self::Str(values) => Self::Str(values.exclude(min, max)),
            Self::Unsigned(values) => Self::Unsigned(values.exclude(min, max)),
        }
    }

    fn include(self, min: i64, max: i64) -> Self {
        match self {
            Self::Float(values) => Self::Float(values.include(min, max)),
            Self::Integer(values) => Self::Integer(values.include(min, max)),
            Self::Bool(values) => Self::Bool(values.include(min, max)),
            Self::Str(values) => Self::Str(values.include(min, max)),
            Self::Unsigned(values) => Self::Unsigned(values.include(min, max)),
        }
    }

    fn find_range(&self, min: i64, max: i64) -> (isize, isize) {
        match self {
            Self::Float(values) => values.find_range(min, max),
            Self::Integer(values) => values.find_range(min, max),
            Self::Bool(values) => values.find_range(min, max),
            Self::Str(values) => values.find_range(min, max),
            Self::Unsigned(values) => values.find_range(min, max),
        }
    }

    fn merge(self, b: Self) -> Self {
        match self {
            Self::Float(values) => {
                if let Self::Float(values_b) = b {
                    Self::Float(values.merge(values_b))
                } else {
                    panic!("expect Float values")
                }
            }
            Self::Integer(values) => {
                if let Self::Integer(values_b) = b {
                    Self::Integer(values.merge(values_b))
                } else {
                    panic!("expect Float values")
                }
            }
            Self::Bool(values) => {
                if let Self::Bool(values_b) = b {
                    Self::Bool(values.merge(values_b))
                } else {
                    panic!("expect Float values")
                }
            }
            Self::Str(values) => {
                if let Self::Str(values_b) = b {
                    Self::Str(values.merge(values_b))
                } else {
                    panic!("expect Float values")
                }
            }
            Self::Unsigned(values) => {
                if let Self::Unsigned(values_b) = b {
                    Self::Unsigned(values.merge(values_b))
                } else {
                    panic!("expect Float values")
                }
            }
        }
    }
}

/// search performs a binary search for UnixNano() v in a
/// and returns the position, i, where v would be inserted.
/// An additional check of a[i].UnixNano() == v is necessary
/// to determine if the value v exists.
fn search<T>(values: &[Value<T>], v: i64) -> usize
where
    T: Debug + Clone + PartialOrd + PartialEq,
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

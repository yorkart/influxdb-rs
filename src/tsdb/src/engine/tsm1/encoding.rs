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

/// Values describes the various types of block data that can be held within a TSM file.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Values {
    Float(Vec<Value<f64>>),
    Integer(Vec<Value<i64>>),
    Bool(Vec<Value<bool>>),
    Str(Vec<Value<Vec<u8>>>),
    Unsigned(Vec<Value<u64>>),
}

impl Values {
    pub fn min_time(&self) -> i64 {
        match self {
            Self::Float(values) => values[0].unix_nano,
            Self::Integer(values) => values[0].unix_nano,
            Self::Bool(values) => values[0].unix_nano,
            Self::Str(values) => values[0].unix_nano,
            Self::Unsigned(values) => values[0].unix_nano,
        }
    }

    pub fn max_time(&self) -> i64 {
        match self {
            Self::Float(values) => values[values.len() - 1].unix_nano,
            Self::Integer(values) => values[values.len() - 1].unix_nano,
            Self::Bool(values) => values[values.len() - 1].unix_nano,
            Self::Str(values) => values[values.len() - 1].unix_nano,
            Self::Unsigned(values) => values[values.len() - 1].unix_nano,
        }
    }

    pub fn size(&self) -> usize {
        let mut sz = 0;
        match self {
            Self::Float(values) => {
                for _i in 0..values.len() {
                    sz += 16
                }
            }
            Self::Integer(values) => {
                for _i in 0..values.len() {
                    sz += 16
                }
            }
            Self::Bool(values) => {
                for _i in 0..values.len() {
                    sz += 9
                }
            }
            Self::Str(values) => {
                for v in values {
                    sz += 8 + v.value.len();
                }
            }
            Self::Unsigned(values) => {
                for _i in 0..values.len() {
                    sz += 16
                }
            }
        };

        sz
    }

    fn ordered_values<T>(values: &[Value<T>]) -> bool
    where
        T: Debug + Clone + PartialOrd + PartialEq,
    {
        if values.len() <= 1 {
            return true;
        }

        for i in 1..values.len() {
            let a = values[i - 1].unix_nano;
            let b = values[i].unix_nano;
            if a >= b {
                return false;
            }
        }
        return true;
    }

    fn ordered(&self) -> bool {
        match self {
            Self::Float(values) => Self::ordered_values(values),
            Self::Integer(values) => Self::ordered_values(values),
            Self::Bool(values) => Self::ordered_values(values),
            Self::Str(values) => Self::ordered_values(values),
            Self::Unsigned(values) => Self::ordered_values(values),
        }
    }

    fn deduplicate_values<T>(mut values: Vec<Value<T>>) -> Vec<Value<T>>
    where
        T: Debug + Clone + PartialOrd + PartialEq,
    {
        if values.len() <= 1 {
            return values;
        }

        if Self::ordered_values(values.as_slice()) {
            return values;
        }

        values.sort_by(|x, y| x.unix_nano.cmp(&y.unix_nano));
        let mut i = 0;
        for j in 1..values.len() {
            let v = &values[j];
            if v.unix_nano != values[i].unix_nano {
                i += 1;
            }
            values[i] = v.clone();
        }

        i += 1;
        if i == values.len() {
            return values;
        }

        values.truncate(i);
        values
    }

    /// Deduplicate returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(self) -> Self {
        match self {
            Self::Float(values) => Self::Float(Self::deduplicate_values(values)),
            Self::Integer(values) => Self::Integer(Self::deduplicate_values(values)),
            Self::Bool(values) => Self::Bool(Self::deduplicate_values(values)),
            Self::Str(values) => Self::Str(Self::deduplicate_values(values)),
            Self::Unsigned(values) => Self::Unsigned(Self::deduplicate_values(values)),
        }
    }

    fn exclude_values<T>(mut values: Vec<Value<T>>, min: i64, max: i64) -> Vec<Value<T>>
    where
        T: Debug + Clone + PartialOrd + PartialEq,
    {
        let (rmin, mut rmax) = Self::find_range_values(values.as_slice(), min, max);
        if rmin == -1 && rmax == -1 {
            return values;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if rmax < values.len() as isize {
            if values[rmax as usize].unix_nano == max {
                rmax += 1;
            }
            let rest = values.len() as isize - rmax;
            if rest > 0 {
                let right = values[rmax as usize..].to_vec();
                values.truncate((rmin + rest) as usize);
                values.extend_from_slice(right.as_slice());

                return values;
            }
        }

        values.truncate(rmin as usize);
        values
    }

    /// Exclude returns the subset of values not in [min, max].  The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(self, min: i64, max: i64) -> Self {
        match self {
            Self::Float(values) => Self::Float(Self::exclude_values(values, min, max)),
            Self::Integer(values) => Self::Integer(Self::exclude_values(values, min, max)),
            Self::Bool(values) => Self::Bool(Self::exclude_values(values, min, max)),
            Self::Str(values) => Self::Str(Self::exclude_values(values, min, max)),
            Self::Unsigned(values) => Self::Unsigned(Self::exclude_values(values, min, max)),
        }
    }

    fn include_values<T>(mut values: Vec<Value<T>>, min: i64, max: i64) -> Vec<Value<T>>
    where
        T: Debug + Clone + PartialOrd + PartialEq,
    {
        let (rmin, mut rmax) = Self::find_range_values(values.as_slice(), min, max);
        if rmin == -1 && rmax == -1 {
            return vec![];
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if rmax < values.len() as isize && values[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            return values[rmin as usize..rmax as usize].to_vec();
        }

        values.truncate(rmax as usize);
        values
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(self, min: i64, max: i64) -> Self {
        match self {
            Self::Float(values) => Self::Float(Self::include_values(values, min, max)),
            Self::Integer(values) => Self::Integer(Self::include_values(values, min, max)),
            Self::Bool(values) => Self::Bool(Self::include_values(values, min, max)),
            Self::Str(values) => Self::Str(Self::include_values(values, min, max)),
            Self::Unsigned(values) => Self::Unsigned(Self::include_values(values, min, max)),
        }
    }

    fn find_range_values<T>(values: &[Value<T>], min: i64, max: i64) -> (isize, isize)
    where
        T: Debug + Clone + PartialOrd + PartialEq,
    {
        if values.len() == 0 || min > max {
            return (-1, -1);
        }

        let min_val = values[0].unix_nano;
        let max_val = values[values.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (
            Self::search(values, min) as isize,
            Self::search(values, max) as isize,
        )
    }

    /// FindRange returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (isize, isize) {
        match self {
            Self::Float(values) => Self::find_range_values(values, min, max),
            Self::Integer(values) => Self::find_range_values(values, min, max),
            Self::Bool(values) => Self::find_range_values(values, min, max),
            Self::Str(values) => Self::find_range_values(values, min, max),
            Self::Unsigned(values) => Self::find_range_values(values, min, max),
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

    fn merge_values<T>(mut values_a: Vec<Value<T>>, mut values_b: Vec<Value<T>>) -> Vec<Value<T>>
    where
        T: Debug + Clone + PartialOrd + PartialEq,
    {
        if values_a.len() == 0 {
            return values_b;
        }
        if values_b.len() == 0 {
            return values_a;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        values_a = Self::deduplicate_values(values_a);
        values_b = Self::deduplicate_values(values_b);

        if values_a[values_a.len() - 1].unix_nano < values_b[0].unix_nano {
            values_a.extend_from_slice(values_b.as_slice());
            return values_a;
        }

        if values_b[values_b.len() - 1].unix_nano < values_a[0].unix_nano {
            values_b.extend_from_slice(values_a.as_slice());
            return values_b;
        }

        let mut out = Vec::with_capacity(values_a.len() + values_b.len());
        let mut a = values_a.as_slice();
        let mut b = values_b.as_slice();

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

    pub fn merge(self, b: Values) -> anyhow::Result<Self> {
        match self {
            Self::Float(values) => {
                if let Self::Float(values_b) = b {
                    Ok(Self::Float(Self::merge_values(values, values_b)))
                } else {
                    Err(anyhow!("expect Float values"))
                }
            }
            Self::Integer(values) => {
                if let Self::Integer(values_b) = b {
                    Ok(Self::Integer(Self::merge_values(values, values_b)))
                } else {
                    Err(anyhow!("expect Integer values"))
                }
            }
            Self::Bool(values) => {
                if let Self::Bool(values_b) = b {
                    Ok(Self::Bool(Self::merge_values(values, values_b)))
                } else {
                    Err(anyhow!("expect Bool values"))
                }
            }
            Self::Str(values) => {
                if let Self::Str(values_b) = b {
                    Ok(Self::Str(Self::merge_values(values, values_b)))
                } else {
                    Err(anyhow!("expect Str values"))
                }
            }
            Self::Unsigned(values) => {
                if let Self::Unsigned(values_b) = b {
                    Ok(Self::Unsigned(Self::merge_values(values, values_b)))
                } else {
                    Err(anyhow!("expect Unsigned values"))
                }
            }
        }
    }
}

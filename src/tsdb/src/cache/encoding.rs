use std::any::Any;

use influxdb_utils::time::{time_format, unix_nano_to_time};
use ordered_float::OrderedFloat;

pub trait TValue: Send + Sync {
    fn unix_nano(&self) -> i64;
    fn value(&self) -> &dyn Any;
    fn size(&self) -> usize;
    fn string(&self) -> String;
    fn internal_only(&self);
}

trait_enum! {
    #[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
    pub enum Value: TValue {
        EmptyValue,
        BooleanValue,
        FloatValue,
        IntegerValue,
        StringValue,
        UnsignedValue,
    }
}

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct EmptyValue {}

impl TValue for EmptyValue {
    fn unix_nano(&self) -> i64 {
        i64::MAX
    }

    fn value(&self) -> &dyn Any {
        &false
    }

    fn size(&self) -> usize {
        0
    }

    fn string(&self) -> String {
        "".to_string()
    }

    fn internal_only(&self) {}
}

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct BooleanValue {
    unixnano: i64,
    value: bool,
}

impl TValue for BooleanValue {
    fn unix_nano(&self) -> i64 {
        self.unixnano
    }

    fn value(&self) -> &dyn Any {
        &self.value
    }

    fn size(&self) -> usize {
        9
    }

    fn string(&self) -> String {
        format!(
            "{} {}",
            time_format(unix_nano_to_time(self.unixnano)),
            self.value
        )
    }

    fn internal_only(&self) {}
}

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct FloatValue {
    unixnano: i64,
    value: OrderedFloat<f64>,
}

impl TValue for FloatValue {
    fn unix_nano(&self) -> i64 {
        self.unixnano
    }

    fn value(&self) -> &dyn Any {
        &self.value.0
    }

    fn size(&self) -> usize {
        16
    }

    fn string(&self) -> String {
        format!(
            "{} {}",
            time_format(unix_nano_to_time(self.unixnano)),
            self.value
        )
    }

    fn internal_only(&self) {}
}

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct IntegerValue {
    unixnano: i64,
    value: i64,
}

impl TValue for IntegerValue {
    fn unix_nano(&self) -> i64 {
        self.unixnano
    }

    fn value(&self) -> &dyn Any {
        &self.value
    }

    fn size(&self) -> usize {
        16
    }

    fn string(&self) -> String {
        format!(
            "{} {}",
            time_format(unix_nano_to_time(self.unixnano)),
            self.value
        )
    }

    fn internal_only(&self) {}
}

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct StringValue {
    unixnano: i64,
    value: Vec<u8>,
}

impl TValue for StringValue {
    fn unix_nano(&self) -> i64 {
        self.unixnano
    }

    fn value(&self) -> &dyn Any {
        &self.value
    }

    fn size(&self) -> usize {
        16
    }

    fn string(&self) -> String {
        format!(
            "{} {:?}",
            time_format(unix_nano_to_time(self.unixnano)),
            self.value
        )
    }

    fn internal_only(&self) {}
}

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq)]
pub struct UnsignedValue {
    unixnano: i64,
    value: u64,
}

impl TValue for UnsignedValue {
    fn unix_nano(&self) -> i64 {
        self.unixnano
    }

    fn value(&self) -> &dyn Any {
        &self.value
    }

    fn size(&self) -> usize {
        16
    }

    fn string(&self) -> String {
        format!(
            "{} {}",
            time_format(unix_nano_to_time(self.unixnano)),
            self.value
        )
    }

    fn internal_only(&self) {}
}
//
// pub struct Values {
//     values: Vec<Box<Value>>,
// }
//
// impl Values {}

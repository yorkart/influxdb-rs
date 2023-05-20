pub use arrow;
pub use arrow_format;

pub type ArrayRef = Box<dyn ::arrow::array::Array>;

pub type Timestamps = ::arrow::array::Int64Array;
pub type FloatValues = ::arrow::array::Float64Array;
pub type IntegerValues = ::arrow::array::Int64Array;
pub type BoolValues = ::arrow::array::BooleanArray;
pub type StringValues = ::arrow::array::Utf8Array<i32>;
pub type Unsigned = ::arrow::array::UInt64Array;

pub type TimestampsVec = ::arrow::array::Int64Vec;
pub type FloatValuesVec = ::arrow::array::Float64Vec;
pub type IntegerValuesVec = ::arrow::array::Int64Vec;
pub type BoolValuesVec = ::arrow::array::MutableBooleanArray;
pub type StringValuesVec = ::arrow::array::MutableUtf8Array<i32>;
pub type UnsignedVec = ::arrow::array::UInt64Vec;

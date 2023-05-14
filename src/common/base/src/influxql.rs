/// MIN_TIME is the minumum time that can be represented.
///
/// 1677-09-21 00:12:43.145224194 +0000 UTC
///
/// The two lowest minimum integers are used as sentinel values.  The
/// minimum value needs to be used as a value lower than any other value for
/// comparisons and another separate value is needed to act as a sentinel
/// default value that is unusable by the user, but usable internally.
/// Because these two values need to be used for a special purpose, we do
/// not allow users to write points at these two times.
pub const MIN_TIME: i64 = i64::MIN + 2;

/// MAX_TIME is the maximum time that can be represented.
///
/// 2262-04-11 23:47:16.854775806 +0000 UTC
///
/// The highest time represented by a nanosecond needs to be used for an
/// exclusive range in the shard group, so the maximum time needs to be one
/// less than the possible maximum number of nanoseconds representable by an
/// int64 so that we don't lose a point at that one time.
pub const MAX_TIME: i64 = i64::MAX - 1;

/// DataType represents the primitive data types available in InfluxQL.
pub enum DataType {
    /// Unknown primitive data type.
    Unknown,
    /// Float means the data type is a float.
    Float,
    /// Integer means the data type is an integer.
    Integer,
    /// String means the data type is a string of text.
    String,
    /// Boolean means the data type is a boolean.
    Boolean,
    /// Time means the data type is a time.
    Time,
    /// Duration means the data type is a duration of time.
    Duration,
    /// Tag means the data type is a tag.
    Tag,
    /// AnyField means the data type is any field.
    AnyField,
    /// Unsigned means the data type is an unsigned integer.
    Unsigned,
}

impl<'a> From<&'a str> for DataType {
    fn from(value: &'a str) -> Self {
        match value {
            "float" => DataType::Float,
            "integer" => DataType::Integer,
            "unsigned" => DataType::Unsigned,
            "string" => DataType::String,
            "boolean" => DataType::Boolean,
            "time" => DataType::Time,
            "duration" => DataType::Duration,
            "tag" => DataType::Tag,
            "field" => DataType::AnyField,
            _ => DataType::Unknown,
        }
    }
}

impl ToString for DataType {
    fn to_string(&self) -> String {
        self.as_str().to_string()
    }
}
impl DataType {
    pub fn value(&self) -> usize {
        match self {
            DataType::Unknown => 0,
            DataType::Float => 1,
            DataType::Integer => 2,
            DataType::String => 3,
            DataType::Boolean => 4,
            DataType::Time => 5,
            DataType::Duration => 6,
            DataType::Tag => 7,
            DataType::AnyField => 8,
            DataType::Unsigned => 9,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::Unknown => "unknown",
            DataType::Float => "float",
            DataType::Integer => "integer",
            DataType::String => "string",
            DataType::Boolean => "boolean",
            DataType::Time => "time",
            DataType::Duration => "duration",
            DataType::Tag => "tag",
            DataType::AnyField => "field",
            DataType::Unsigned => "unsigned",
        }
    }
}

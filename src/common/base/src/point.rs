use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::str::from_utf8_unchecked;

/// ZERO_TIME is the Unix nanosecond timestamp for no time.
/// This time is not used by the query engine or the storage engine as a valid time.
pub const ZERO_TIME: i64 = i64::MIN;

/// keyFieldSeparator separates the series key from the field name in the composite key
/// that identifies a specific field in series
pub const KEY_FIELD_SEPARATOR: &'static str = "#!~#";

pub fn series_field_key(series: &[u8], field: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(series.len() + KEY_FIELD_SEPARATOR.len() + field.len());
    key.extend_from_slice(series);
    key.extend_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
    key.extend_from_slice(field);
    key
}

#[derive(Clone)]
pub struct Tag {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Tag {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    pub fn size(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let key = unsafe { from_utf8_unchecked(self.key.as_slice()) };
        let value = unsafe { from_utf8_unchecked(self.value.as_slice()) };

        f.debug_struct("Tag")
            .field("key", &key)
            .field("value", &value)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct Tags(Vec<Tag>);

impl Tags {
    pub fn new(tags: Vec<Tag>) -> Self {
        Self(tags)
    }

    pub fn size(&self) -> usize {
        self.0.iter().map(|x| x.size()).sum()
    }
}

impl Deref for Tags {
    type Target = [Tag];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

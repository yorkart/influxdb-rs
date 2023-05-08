use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::str::from_utf8_unchecked;

/// ZERO_TIME is the Unix nanosecond timestamp for no time.
/// This time is not used by the query engine or the storage engine as a valid time.
pub const ZERO_TIME: i64 = i64::MIN;

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
        write!(f, "Tag{{key: {}, value: {}}}", key, value)
    }
}

#[derive(Clone)]
pub struct Tags(Vec<Tag>);

impl Tags {
    pub fn new(tags: Vec<Tag>) -> Self {
        Self(tags)
    }

    // pub fn len(&self) -> usize {
    //     self.0.len()
    // }

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

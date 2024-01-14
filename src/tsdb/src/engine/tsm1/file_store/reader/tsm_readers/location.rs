use std::sync::Arc;

use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::field_reader::FieldReader;
use crate::engine::tsm1::value::Array;

pub struct Location {
    pub(crate) reader: Arc<Box<dyn FieldReader>>,
    pub(crate) entry: IndexEntry,
    pub(crate) pre_read: Option<Box<dyn Array>>,

    pub(crate) read_min: i64,
    pub(crate) read_max: i64,
}

impl Location {
    pub(crate) fn new(reader: Arc<Box<dyn FieldReader>>, entry: IndexEntry) -> Self {
        Self {
            reader,
            entry,
            pre_read: None,
            read_min: 0,
            read_max: 0,
        }
    }

    pub async fn load_values(&mut self, mut buf: Box<dyn Array>) -> anyhow::Result<()> {
        self.reader.read_at(&self.entry, &mut buf).await?;
        self.pre_read = Some(buf);
        Ok(())
    }

    pub fn read(&self) -> bool {
        self.read_min <= self.entry.min_time && self.read_max >= self.entry.max_time
    }

    pub fn mark_read(&mut self, min: i64, max: i64) {
        if min < self.read_min {
            self.read_min = min;
        }

        if max > self.read_max {
            self.read_max = max;
        }
    }
}

/// sort by max time
pub fn sort_desc(locations: &mut Vec<Location>) {
    locations.sort_by(|i, j| {
        if i.entry.overlaps(&j.entry) {
            return i.reader.path().cmp(j.reader.path());
        }
        i.entry.max_time.cmp(&j.entry.max_time)
    });
}

/// sort by min time
pub fn sort_asc(locations: &mut Vec<Location>) {
    locations.sort_by(|i, j| {
        if i.entry.overlaps(&j.entry) {
            return i.reader.path().cmp(j.reader.path());
        }
        i.entry.min_time.cmp(&j.entry.min_time)
    });
}

/// seek Location which contain or greater than t
pub fn seek_asc(locations: &Vec<Location>, t: i64) -> Vec<usize> {
    let mut current = Vec::with_capacity(locations.len());
    for (i, location) in locations.iter().enumerate() {
        if t < location.entry.min_time || location.entry.contains(t) {
            current.push(i);
        }
    }
    current
}

/// seek Location which contain or less than t
pub fn seek_desc(locations: &Vec<Location>, t: i64) -> Vec<usize> {
    let mut current = Vec::with_capacity(locations.len());
    for (i, location) in locations.iter().enumerate().rev() {
        if t > location.entry.max_time || location.entry.contains(t) {
            current.push(i);
        }
    }
    current
}

pub fn overlap_block(locations: &Vec<Location>) {
    if locations.len() == 0 {
        return;
    }

    let first = &locations[0];
    let mut min_t = first.read_min;
    let mut max_t = first.read_max;
    for location in locations {
        if location.read_min < min_t {
            min_t = location.read_min;
        }
        if location.read_max < max_t {
            max_t = location.read_max;
        }
    }
}

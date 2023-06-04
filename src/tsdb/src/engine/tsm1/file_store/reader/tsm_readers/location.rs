use std::sync::Arc;

use crate::engine::tsm1::file_store::index::IndexEntry;
use crate::engine::tsm1::file_store::reader::tsm_iterator_v2::field_reader::FieldReader;

pub struct Location {
    pub(crate) reader: Arc<Box<dyn FieldReader>>,
    pub(crate) entry: IndexEntry,

    pub(crate) read_min: i64,
    pub(crate) read_max: i64,
}

impl Location {
    pub(crate) fn new(reader: Arc<Box<dyn FieldReader>>, entry: IndexEntry) -> Self {
        Self {
            reader,
            entry,
            read_min: 0,
            read_max: 0,
        }
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

pub fn sort_desc(locations: &mut Vec<Location>) {
    locations.sort_by(|i, j| {
        if i.entry.overlaps(&j.entry) {
            return i.reader.path().cmp(j.reader.path());
        }
        i.entry.max_time.cmp(&j.entry.max_time)
    });
}

pub fn sort_asc(locations: &mut Vec<Location>) {
    locations.sort_by(|i, j| {
        if i.entry.overlaps(&j.entry) {
            return j.reader.path().cmp(i.reader.path());
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

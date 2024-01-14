use crate::engine::tsm1::file_store::reader::tsm_readers::location::{
    seek_asc, seek_desc, sort_asc, sort_desc, Location,
};
use crate::engine::tsm1::value::{Array, FloatValues};

pub struct KeyCursor {
    key: Vec<u8>,

    /// seeks is all the file locations that we need to return during iteration.
    seeks: Vec<Location>,

    /// current is the set of blocks possibly containing the next set of points.
    /// Normally this is just one entry, but there may be multiple if points have
    /// been overwritten.
    current: Vec<usize>,

    /// pos is the index within seeks.  Based on ascending, it will increment or
    /// decrement through the size of seeks slice.
    pos: usize,
    ascending: bool,
}

impl KeyCursor {
    pub fn new(key: Vec<u8>, mut seeks: Vec<Location>, t: i64, ascending: bool) -> Self {
        let current = if ascending {
            sort_asc(&mut seeks);
            seek_asc(&seeks, t)
        } else {
            sort_desc(&mut seeks);
            seek_desc(&seeks, t)
        };

        let pos = if current.len() > 0 { current[0] } else { 0 };

        Self {
            key,
            seeks,
            current,
            pos,
            ascending,
        }
    }

    fn current(&self, n: usize) -> &Location {
        &self.seeks[self.current[n]]
    }

    fn current_mut(&mut self, n: usize) -> &mut Location {
        &mut self.seeks[self.current[n]]
    }

    /// Next moves the cursor to the next position.
    /// Data should be read by the ReadBlock functions.
    pub fn next(&mut self) {
        if self.current.len() == 0 {
            return;
        }

        let location = self.current(0);
        // Do we still have unread values in the current block
        if location.read() {
            return;
        }

        if self.ascending {
            self.next_ascending();
        } else {
            self.next_descending();
        }
    }

    fn next_ascending(&mut self) {
        loop {
            self.pos += 1;
            if self.pos >= self.seeks.len() {
                return;
            } else if !self.seeks[self.pos].read() {
                break;
            }
        }

        // Append the first matching block
        self.current.clear();
        self.current.push(self.pos);

        // If we have overlapping blocks, append all their values so we can dedup
        for i in self.pos + 1..self.seeks.len() {
            if self.seeks[i].read() {
                continue;
            }

            self.current.push(i);
        }
    }

    fn next_descending(&mut self) {
        loop {
            self.pos -= 1;
            if self.pos < 0 {
                return;
            } else if !self.seeks[self.pos].read() {
                break;
            }
        }

        // Append the first matching block
        self.current.clear();
        self.current.push(self.pos);

        // If we have overlapping blocks, append all their values so we can dedup
        for i in (0..self.pos).rev() {
            if self.seeks[i].read() {
                continue;
            }

            self.current.push(i);
        }
    }

    pub async fn read_float_block(&mut self, values: &mut Box<dyn Array>) -> anyhow::Result<()> {
        // 1. 读取第一个有值的block
        loop {
            // No matching blocks to decode
            if self.current.len() == 0 {
                return Ok(());
            }

            // First block is the oldest block containing the points we're searching for.
            let first = &self.seeks[self.current[0]];

            values.clear();
            first.reader.read_at(&first.entry, values).await?;

            // Remove values we already read
            values.exclude(first.read_min, first.read_max);

            // If there are no values in this first block (all tombstoned or previously read) and
            // we have more potential blocks too search.  Try again.
            if values.len() == 0 && self.current.len() > 0 {
                self.current.remove(0);
            } else {
                break;
            }
        }

        // Only one block with this key and time range so return it
        // 2. 只有一个block，直接返回
        if self.current.len() == 1 {
            if values.len() > 0 {
                let _ = &mut self.seeks[self.current[0]]
                    .mark_read(values.min_time(), values.max_time());
            }
            return Ok(());
        }

        // Use the current block time range as our overlapping window
        let mut min_t = self.seeks[self.current[0]].read_min;
        let mut max_t = self.seeks[self.current[0]].read_max;
        if values.len() > 0 {
            min_t = values.min_time();
            max_t = values.max_time();
        }

        if self.ascending {
            // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
            // expand the window to include the min time range to ensure values are returned in ascending
            // order
            // 向左扩展窗口
            for i in 1..self.current.len() {
                let cur = &self.seeks[self.current[i]];
                if cur.entry.min_time < min_t && !cur.read() {
                    min_t = cur.entry.min_time;
                }
            }

            // Find first block that overlaps our window
            for i in 1..self.current.len() {
                let cur = &self.seeks[self.current[i]];
                if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                    // Shrink our window so it's the intersection of the first overlapping block and the
                    // first block.  We do this to minimize the region that overlaps and needs to
                    // be merged.
                    if cur.entry.max_time > max_t {
                        max_t = cur.entry.max_time
                    }
                    values.include(min_t, max_t);
                    break;
                }
            }

            // Search the remaining blocks that overlap our window and append their values so we can
            // merge them.
            for i in 1..self.current.len() {
                let cur = &mut self.seeks[self.current[i]];
                // Skip this block if it doesn't contain points we looking for or they have already been read
                if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                    cur.mark_read(min_t, max_t);
                    continue;
                }

                let v: FloatValues = vec![];
                let mut v: Box<dyn Array> = Box::new(v);
                cur.reader.read_at(&cur.entry, &mut v).await?;

                // cur.reader.tombstone_range(self.key.as_slice()).await;
            }
        }

        Ok(())
    }
}

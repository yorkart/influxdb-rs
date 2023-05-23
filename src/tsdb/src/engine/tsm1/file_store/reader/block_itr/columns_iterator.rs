use std::cmp::min;
use std::sync::Arc;

use common_arrow::arrow::array::{Array, MutableArray};
use common_arrow::arrow::chunk::Chunk;
use common_arrow::TimestampsVec;
use common_base::iterator::AsyncIterator;

use crate::engine::tsm1::file_store::reader::block_itr::array_builder::ArrayBuilder;

pub struct FieldsBatchIterator {
    array_builders: Vec<Box<dyn ArrayBuilder>>,
    finish: bool,

    capacity: usize,
    buf: Option<TimestampsVec>,
}

impl FieldsBatchIterator {
    pub async fn new(
        mut array_builders: Vec<Box<dyn ArrayBuilder>>,
        capacity: usize,
    ) -> anyhow::Result<Self> {
        for builder in &mut array_builders {
            builder.next().await?;
        }

        Ok(Self {
            array_builders,
            finish: false,
            capacity,
            buf: Some(TimestampsVec::with_capacity(capacity)),
        })
    }
}

#[async_trait]
impl AsyncIterator for FieldsBatchIterator {
    type Item = Chunk<Arc<dyn Array>>;

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        if self.finish {
            return Ok(None);
        }

        for _i in 0..self.capacity {
            let mut min_time = i64::MAX;
            for builder in &mut self.array_builders {
                if let Some(v) = builder.next_time() {
                    min_time = min(min_time, v);
                }
            }

            if min_time == i64::MAX {
                self.finish = true;
                break;
            }

            for builder in &mut self.array_builders {
                if let Some(v) = builder.next_time() {
                    if v == min_time {
                        builder.fill_value()?;
                        builder.next().await?;
                    } else {
                        builder.fill_null();
                    }
                }
            }

            self.buf.as_mut().unwrap().push(Some(min_time));
        }

        let mut fields_array: Vec<Arc<dyn Array>> =
            Vec::with_capacity(self.array_builders.len() + 1);

        let time_array = self.buf.take().unwrap();
        let size = time_array.len();
        self.buf = Some(TimestampsVec::with_capacity(self.capacity));
        fields_array.push(time_array.into_arc());

        self.array_builders.iter_mut().for_each(|x| {
            let array = x.build();
            if array.len() != size {
                panic!(
                    "length inconsistency, expect: {}, found: {}",
                    size,
                    array.len()
                );
            }
            fields_array.push(array);
        });

        Ok(Some(Chunk::new(fields_array)))
    }
}

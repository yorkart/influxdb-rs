// /// https://stackoverflow.com/questions/65663021/how-to-call-an-async-function-in-poll-method
// impl Stream for SeriesEntryIterator {
//     type Item = anyhow::Result<(SeriesEntry, u64)>;
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let entry_offset = self.read_offset;
//         if entry_offset >= self.max_offset {
//             return Poll::Ready(None);
//         }
//
//         let mut f = SeriesEntry::read_from(&mut self.reader);
//         let f = Pin::new(&mut f);
//         let n = f.poll(cx);
//
//         match n {
//             Poll::Ready(r) => match r {
//                 Ok((se, len)) => {
//                     self.read_offset += len as u32;
//
//                     let offset = join_series_offset(self.segment_id, entry_offset as u32);
//                     Poll::Ready(Some(Ok((se, offset))))
//                 }
//                 Err(e) => Poll::Ready(Some(Err(e))),
//             },
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }

pub trait TryIterator {
    type Item;
    fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>>;
}

#[async_trait]
pub trait AsyncIterator {
    type Item;
    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>>;
}

#[async_trait]
pub trait RefAsyncIterator {
    type Item<'a>
    where
        Self: 'a;
    async fn try_next<'a>(&'a mut self) -> anyhow::Result<Option<Self::Item<'a>>>;
}

pub struct AsyncIterators<ITEM, ITR>
where
    ITR: AsyncIterator<Item = ITEM> + Send,
{
    itrs: Vec<ITR>,
    i: usize,
}

impl<ITEM, ITR> AsyncIterators<ITEM, ITR>
where
    ITR: AsyncIterator<Item = ITEM> + Send,
{
    pub fn new(itrs: Vec<ITR>) -> Self {
        Self { itrs, i: 0 }
    }
}

#[async_trait]
impl<ITEM, ITR> AsyncIterator for AsyncIterators<ITEM, ITR>
where
    ITR: AsyncIterator<Item = ITEM> + Send,
{
    type Item = ITEM;

    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>> {
        if self.itrs.len() == 0 {
            return Ok(None);
        }

        loop {
            let itr = &mut self.itrs[self.i];
            if let Some(v) = itr.try_next().await? {
                return Ok(Some(v));
            }

            self.i += 1;
        }
    }
}

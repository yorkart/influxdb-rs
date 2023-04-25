#[async_trait]
pub trait AsyncIterator {
    type Item;
    async fn try_next(&mut self) -> anyhow::Result<Option<Self::Item>>;
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

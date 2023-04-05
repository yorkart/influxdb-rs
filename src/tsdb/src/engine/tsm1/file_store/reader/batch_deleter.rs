pub trait BatchDeleter {
    fn delete_range(&mut self, keys: &[&[u8]], min: i64, max: i64) -> anyhow::Result<()>;
    fn commit(&mut self) -> anyhow::Result<()>;
    fn rollback(&mut self) -> anyhow::Result<()>;
}

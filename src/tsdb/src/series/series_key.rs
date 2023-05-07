pub struct SeriesKey<'a> {
    key: &'a [u8],
}

impl<'a> SeriesKey<'a> {
    pub fn new(key: &'a [u8]) -> Self {
        Self { key }
    }

    pub fn name(&self) -> anyhow::Result<&[u8]> {}
}

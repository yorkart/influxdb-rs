pub mod hll;

/// Sketch is the interface representing a sketch for estimating cardinality.
pub trait Sketch {
    /// Add adds a single value to the sketch.
    fn add(&mut self, v: &[u8]);

    /// Count returns a cardinality estimate for the sketch.
    fn count(&self) -> u64;

    /// Merge merges another sketch into this one.
    fn merge<T: Sketch>(&mut self, s: T) -> anyhow::Result<()>;

    /// Bytes estimates the memory footprint of the sketch, in bytes.
    fn bytes(&self) -> usize;

    fn encode(&self) -> Vec<u8>;

    fn decode(data: &[u8]) -> anyhow::Result<Self>;
}

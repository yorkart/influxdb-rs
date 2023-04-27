pub mod hll;

/// Sketch is the interface representing a sketch for estimating cardinality.
pub trait Sketch {
    /// Add adds a single value to the sketch.
    fn add(&mut self, v: &[u8]);

    /// Count returns a cardinality estimate for the sketch.
    fn count(&mut self) -> u64;

    /// Merge merges another sketch into this one.
    fn merge(&mut self, s: &Self) -> anyhow::Result<()>;
}

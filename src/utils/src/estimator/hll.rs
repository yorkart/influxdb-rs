use crate::estimator::Sketch;
use anyhow::anyhow;
use hyperloglogplus::{HyperLogLog, HyperLogLogError, HyperLogLogPlus};
use twox_hash::RandomXxHashBuilder64;

/// DEFAULT_PRECISION is the default precision.
const DEFAULT_PRECISION: u8 = 16;

pub struct Plus {
    hllp: HyperLogLogPlus<u8, RandomXxHashBuilder64>,
}

impl Plus {
    pub fn new() -> Result<Self, HyperLogLogError> {
        Self::with_p(DEFAULT_PRECISION)
    }

    pub fn with_p(p: u8) -> Result<Self, HyperLogLogError> {
        let hllp = HyperLogLogPlus::new(p, RandomXxHashBuilder64::default())?;
        Ok(Self { hllp })
    }
}

impl Sketch for Plus {
    fn add(&mut self, values: &[u8]) {
        for v in values {
            self.hllp.insert(v);
        }
    }

    fn count(&mut self) -> u64 {
        self.hllp.count() as u64
    }

    fn merge(&mut self, s: &Self) -> anyhow::Result<()> {
        self.hllp.merge(&s.hllp).map_err(|e| anyhow!(e))
    }

    fn encode(&self) -> anyhow::Result<Vec<u8>> {
        // serde_json::to_vec(&self.hllp).map_err(|e| anyhow!(e))
        todo!()
    }
}

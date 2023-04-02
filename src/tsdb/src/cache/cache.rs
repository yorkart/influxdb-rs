use std::sync::{Arc, RwLock};

use crate::cache::encoding::Value;

pub struct Values {
    values: Vec<Value>,
}

impl Values {
    pub fn new() -> Self {
        Self { values: vec![] }
    }

    pub fn deduplicate(&mut self) {
        if self.values.len() == 0 {
            return;
        }

        let mut need_sort = false;
        for i in 1..self.values.len() {
            if self.values[i - 1].unix_nano() >= self.values[i].unix_nano() {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        self.values.sort();

        let mut i = 0;
        for j in 1..self.values.len() {
            let v = &self.values[j];
            if v.unix_nano() != self.values[i].unix_nano() {
                i += 1;
            }
            self.values[i] = v.clone();
        }

        self.values.truncate(i + 1);
    }
}

#[derive(Clone)]
pub struct Entry {
    mu: Arc<RwLock<Values>>,
    vtype: u8,
}

impl Entry {
    pub fn new(vtype: u8) -> Self {
        Self {
            mu: Arc::new(RwLock::new(Values::new())),
            vtype,
        }
    }

    pub fn get_value_type(&self, values: &[Value]) -> anyhow::Result<u8> {
        let et = value_type(&values[0]);

        for v in values {
            if et != value_type(v) {
                return Err(anyhow!(""));
            }
        }

        return Ok(et);
    }

    pub fn add(&self, values: &[Value]) -> anyhow::Result<()> {
        if values.len() == 0 {
            return Ok(());
        }

        if self.vtype != 0 {
            for v in values {
                if self.vtype != value_type(v) {
                    return Err(anyhow!(""));
                }
            }
        }

        let mut inner = self.mu.write().unwrap();
        inner.values.extend_from_slice(values);
        Ok(())
    }

    pub fn deduplicate(&self) {
        let mut inner = self.mu.write().unwrap();

        if inner.values.len() == 0 {
            return;
        }

        inner.deduplicate();
    }

    pub fn count(&self) -> usize {
        let inner = self.mu.read().unwrap();
        inner.values.len()
    }
}

pub fn value_type(v: &Value) -> u8 {
    match v {
        Value::FloatValue(_) => 1,
        Value::IntegerValue(_) => 2,
        Value::StringValue(_) => 3,
        Value::BooleanValue(_) => 4,
        _ => 0,
    }
}

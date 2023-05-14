pub mod generated_with_pure {
    include!(concat!(env!("OUT_DIR"), "/generated_with_pure/mod.rs"));
}

pub use generated_with_pure::*;

#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;

// pub mod cache;
pub mod engine;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

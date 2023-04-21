#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;

// pub mod cache;
pub mod engine;
pub mod index;
pub mod series;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}

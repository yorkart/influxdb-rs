pub mod bit;
pub mod simple8b;
pub mod varint;
pub mod zigzag;

pub mod boolean;
pub mod float;
pub mod integer;
// pub mod number;
pub mod string;
pub mod timestamp;
pub mod unsigned;

pub trait Encoder<T> {
    fn write(&mut self, v: T);
    fn flush(&mut self);
    fn bytes(&mut self) -> anyhow::Result<Vec<u8>>;
}

pub trait Decoder<T> {
    fn next(&mut self) -> bool;
    fn read(&self) -> T;
    fn err(&self) -> Option<&anyhow::Error>;
}

// pub struct MyStruct {
//     buf: Vec<u8>,
// }
//
// impl<'a> Decoder<&'a [u8]> for MyStruct
// where
//     Self: 'a,
// {
//     fn next(&mut self) -> bool {
//         todo!()
//     }
//
//     fn read(&'a self) -> &'a [u8] {
//         todo!()
//     }
//
//     fn err(&self) -> Option<&anyhow::Error> {
//         todo!()
//     }
// }

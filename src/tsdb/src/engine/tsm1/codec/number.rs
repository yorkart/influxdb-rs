pub trait Number: Sized + Copy {
    fn required_space(self) -> usize;
    fn decode(src: &[u8]) -> Option<(Self, usize)>;
    fn encode(self, src: &mut [u8]);
}

impl Number for u64 {
    fn required_space(self) -> usize {
        8
    }

    fn decode(_src: &[u8]) -> Option<(Self, usize)> {
        todo!()
    }

    fn encode(self, src: &mut [u8]) {
        let src = &mut src[..self.required_space()];
        src.copy_from_slice(self.to_be_bytes().as_slice());
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::tsm1::codec::number::Number;

    #[test]
    fn test_u64() {
        let a = 10_u64;

        let mut src = [0; 8];
        a.encode(&mut src);

        println!("{:?}", src);
    }
}

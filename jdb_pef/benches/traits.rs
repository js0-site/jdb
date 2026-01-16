use criterion::Criterion;

pub trait Bench {
    const NAME: &'static str;

    fn new(data: &[u64]) -> Self;
    fn size_in_bytes(&self) -> usize;
    fn get(&self, index: usize) -> Option<u64>;
    fn next_ge(&self, target: u64) -> Option<u64>;
    // fn iter(&self) -> impl Iterator<Item = u64>; // Iterators vary too much to traitify easily without Box
}

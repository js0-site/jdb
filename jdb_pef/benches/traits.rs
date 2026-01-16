pub trait Bench {
  const NAME: &'static str;
  type Iter<'a>: Iterator<Item = u64>
  where
    Self: 'a;

  fn new(data: &[u64]) -> Self;
  fn size_in_bytes(&self) -> usize;
  fn get(&self, index: usize) -> Option<u64>;
  fn next_ge(&self, target: u64) -> Option<u64>;
  fn iter<'a>(&'a self) -> Self::Iter<'a>;
}

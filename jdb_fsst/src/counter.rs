use crate::CODE_MAX;

#[derive(Clone)]
pub struct Counters {
  pub count1: Vec<u16>,
  pub count2: Vec<Vec<u16>>,
}

impl Counters {
  pub fn new() -> Self {
    Self {
      count1: vec![0; CODE_MAX as usize],
      count2: vec![vec![0; CODE_MAX as usize]; CODE_MAX as usize],
    }
  }

  #[inline]
  pub fn count1_set(&mut self, pos1: usize, val: u16) {
    self.count1[pos1] = val;
  }

  #[inline]
  pub fn count1_inc(&mut self, pos1: u16) {
    self.count1[pos1 as usize] = self.count1[pos1 as usize].saturating_add(1);
  }

  #[inline]
  pub fn count2_inc(&mut self, pos1: usize, pos2: usize) {
    self.count2[pos1][pos2] = self.count2[pos1][pos2].saturating_add(1);
  }

  #[inline]
  pub fn count1_get(&self, pos1: usize) -> u16 {
    self.count1[pos1]
  }

  #[inline]
  pub fn count2_get(&self, pos1: usize, pos2: usize) -> u16 {
    self.count2[pos1][pos2]
  }
}

impl Default for Counters {
  fn default() -> Self {
    Self::new()
  }
}

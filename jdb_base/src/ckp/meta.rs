use std::{borrow::Borrow, cmp::Ordering, ops::Deref};

use bitcode::{Decode, Encode};

use crate::sst;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Meta {
  /// Level number (0 = L0, 1 = L1, ...)
  /// 层级编号
  pub sst_level: u8,
  pub meta: sst::Meta,
}

impl PartialEq for Meta {
  fn eq(&self, other: &Self) -> bool {
    self.meta.id == other.meta.id
  }
}

impl Eq for Meta {}

impl PartialOrd for Meta {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Meta {
  fn cmp(&self, other: &Self) -> Ordering {
    self.meta.min.cmp(&other.meta.min)
  }
}

impl Deref for Meta {
  type Target = sst::Meta;

  fn deref(&self) -> &Self::Target {
    &self.meta
  }
}

impl Borrow<sst::Meta> for Meta {
  fn borrow(&self) -> &sst::Meta {
    &self.meta
  }
}

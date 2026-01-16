use std::hash::Hash;

use bitcode::{Decode, Encode};

use crate::Flag;

#[derive(Debug, Encode, Decode, Hash, Clone, Copy, Eq, PartialEq)]
pub struct Pos {
  pub ver: u64,
  pub wal_id: u64,
  pub offset_or_file_id: u64,
  pub len: u32,
  pub flag: Flag,
}

impl Pos {
  pub const SIZE: usize = std::mem::size_of::<Self>();
}

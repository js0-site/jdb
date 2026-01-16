use bitcode::{Decode, Encode};
use jdb_xorf::{Bf, Bf8};

// 放到 Foot
#[derive(Encode, Decode)]
pub struct Sst {
  pub xorf: Bf<[u8], Bf8>,
  pub block_head_li: Vec<BlockHead>,
}

#[derive(Debug, Encode, Decode)]
pub struct BlockLen {
  pub li: Vec<u32>,
}

#[derive(Debug, Encode, Decode)]
pub struct BlockHead {
  pub prefix_len: u16,
  pub begin: Vec<u8>, // 包含 prefix
  pub end: Vec<u8>,   // 不包含 prefix
  pub index_len: u32, // 第一个的offset的0，后面就是前面一个的offset加上len
  pub body_len: u32,
}

#[derive(Debug, Encode, Decode)]
pub struct BlockIndex {
  // pub pgm: TODO,
  // pub fsst: TODO,
  // pub pos_li: Vec<Pos>,
  // pub key_pos_li: Vec<u32>,
}

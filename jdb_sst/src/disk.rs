use std::ops::{Deref, DerefMut};

use bitcode::{Decode, Encode};
use jdb_base::Pos;
use jdb_pgm::PgmIndex;
use jdb_xorf::{Bf, Bf8};
use zerocopy::{
  FromBytes, Immutable, IntoBytes, KnownLayout,
  little_endian::{U16, U32, U64},
};

pub const BLOCK_N: usize = 16;

/// Binary Fuse filters
pub type Bfuse = Bf<[u8], Bf8>;
pub type Pgm = PgmIndex<u64>;

#[derive(Debug, Encode, Decode)]
pub enum KeyCompress {
  None,
  Fsst(Box<jdb_fsst::Decode>),
}

#[derive(Debug, Encode, Decode)]
pub struct BlockKey {
  pub prefix: Vec<u8>,
  pub begin: Vec<u8>,
  pub end: Vec<u8>,
}

#[derive(Debug, Encode, Decode)]
pub struct BlockKeyLi(Vec<BlockKey>);

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Foot {
  // block 的 key 放一起，方便整个加载
  pub block_key_li_len: U32,
  pub block_pgm_len_li: [U32; BLOCK_N],
  pub block_key_compress_len_li: [U16; BLOCK_N],
  pub block_body_len: [U32; BLOCK_N],
  // 整个 sst 复用一个 bfuse
  pub bfuse_len: U32,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct End {
  pub foot: Foot,
  pub foot_crc: U32,
  pub magic_ver: U64,
}

impl End {
  pub const SIZE: usize = size_of::<Self>();
}

#[derive(Encode, Decode)]
pub struct Kv {
  pub key: Box<[u8]>,
  pub val: Pos,
}

impl Deref for BlockKeyLi {
  type Target = Vec<BlockKey>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for BlockKeyLi {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

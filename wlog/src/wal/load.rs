use jdb_fs::{
  head::{HEAD_SIZE, HEAD_TOTAL, Head, MAGIC},
  load::{INVALID, Load},
};
use zerocopy::FromBytes;

/// WAL entry type for Load trait / WAL 条目类型用于 Load trait
pub struct WalEntry;

impl Load for WalEntry {
  type Head = Head;

  const MAGIC: u8 = MAGIC;
  const MIN_SIZE: usize = HEAD_TOTAL + 1; // magic + head + crc + min_key(1)
  const META_OFFSET: usize = 1;

  #[inline]
  fn len(buf: &[u8]) -> usize {
    if buf.len() < HEAD_TOTAL || buf[0] != MAGIC {
      return INVALID;
    }
    let Some(head) = Head::read_from_bytes(&buf[1..1 + HEAD_SIZE]).ok() else {
      return INVALID;
    };
    1 + head.record_size()
  }

  #[inline]
  fn crc_offset(_len: usize) -> usize {
    1 + HEAD_SIZE
  }

  #[inline]
  fn meta_len(_len: usize) -> usize {
    HEAD_SIZE
  }

  fn parse_head(buf: &[u8], _len: usize) -> Option<Self::Head> {
    if buf.len() < HEAD_SIZE {
      return None;
    }
    Head::read_from_bytes(&buf[..HEAD_SIZE]).ok()
  }
}

//! SSTable footer writer
//! SSTable 尾部写入器

use compio::io::AsyncWrite;
use crc32fast::Hasher;
use jdb_sst::{
  Foot, VERSION,
  error::{Error, Result},
};
use jdb_xorf::BinaryFuse8;
use zerocopy::IntoBytes;

use crate::{pgm, push, state::State};

pub(crate) async fn write(w: &mut (impl AsyncWrite + Unpin), s: &mut State) -> Result<()> {
  let mut hasher = Hasher::new();

  let filter_offset = s.file_offset;
  let filter = BinaryFuse8::try_from(&s.hashes).map_err(|_| Error::FilterBuildFailed)?;
  let filter_data = bitcode::encode(&filter);
  let filter_size = filter_data.len() as u32;
  hasher.update(&filter_data);
  s.file_offset += push(w, &filter_data).await?;

  let index_data = bitcode::encode(&s.first_keys);
  let index_size = index_data.len() as u32;
  hasher.update(&index_data);
  s.file_offset += push(w, &index_data).await?;

  let offsets_data = bitcode::encode(&s.offsets);
  let offsets_size = offsets_data.len() as u32;
  hasher.update(&offsets_data);
  s.file_offset += push(w, &offsets_data).await?;

  let (pgm_data, prefix_len) = pgm::build(&s.first_keys, s.epsilon);
  let pgm_size = pgm_data.len() as u32;
  hasher.update(&pgm_data);
  s.file_offset += push(w, &pgm_data).await?;

  hasher.update(&[VERSION]);
  let checksum = hasher.finalize();
  let foot = Foot {
    filter_offset,
    filter_size,
    index_size,
    offsets_size,
    pgm_size,
    block_count: s.offsets.len() as u32,
    max_ver: s.max_ver,
    rmed_size: s.rmed_size,
    prefix_len,
    level: s.level,
    version: VERSION,
    checksum,
  };
  let foot_bytes = foot.as_bytes();
  s.file_offset += push(w, foot_bytes).await?;

  s.meta.file_size = s.file_offset;
  s.meta.rmed_size = s.rmed_size;
  Ok(())
}

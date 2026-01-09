//! SSTable footer writer
//! SSTable 尾部写入器

use compio::fs::File;
use crc32fast::Hasher;
use jdb_sst::{Error, Foot, Result, VERSION};
use jdb_xorf::BinaryFuse8;
use zerocopy::IntoBytes;

use crate::{pgm, state::State, write_at};

pub(crate) async fn write(file: &mut File, w: &mut State, mut offset: u64) -> Result<()> {
  let mut hasher = Hasher::new();

  let filter_offset = offset;
  let filter = BinaryFuse8::try_from(&w.hashes).map_err(|_| Error::FilterBuildFailed)?;
  let filter_data = bitcode::encode(&filter);
  let filter_size = filter_data.len() as u32;
  hasher.update(&filter_data);
  offset += write_at(file, &filter_data, offset).await?;

  let index_data = bitcode::encode(&w.first_keys);
  let index_size = index_data.len() as u32;
  hasher.update(&index_data);
  offset += write_at(file, &index_data, offset).await?;

  let offsets_data = bitcode::encode(&w.offsets);
  let offsets_size = offsets_data.len() as u32;
  hasher.update(&offsets_data);
  offset += write_at(file, &offsets_data, offset).await?;

  let (pgm_data, prefix_len) = pgm::build(&w.first_keys, w.epsilon);
  let pgm_size = pgm_data.len() as u32;
  hasher.update(&pgm_data);
  offset += write_at(file, &pgm_data, offset).await?;

  hasher.update(&[VERSION]);
  let checksum = hasher.finalize();
  let foot = Foot {
    filter_offset,
    filter_size,
    index_size,
    offsets_size,
    pgm_size,
    block_count: w.offsets.len() as u32,
    max_ver: w.max_ver,
    rmed_size: w.rmed_size,
    prefix_len,
    level: w.level,
    version: VERSION,
    checksum,
  };
  offset += write_at(file, foot.as_bytes(), offset).await?;

  w.meta.file_size = offset;
  w.meta.rmed_size = w.rmed_size;
  Ok(())
}

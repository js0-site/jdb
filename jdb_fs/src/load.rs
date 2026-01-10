//! Load struct for loading data from file
//! 从文件加载数据的结构体

use std::{io, mem, path::Path};

use compio::{fs::File, io::AsyncReadAt};

use crate::{
  consts::BUF_READ_SIZE,
  file::read_write,
  item::{Decode, ParseResult},
};

/// Load struct for loading data from file
/// 从文件加载数据的结构体
pub struct Load {
  pub pos: u64,
  pub file: File,
}

impl Load {
  /// Open file and load data
  /// 打开文件并加载数据
  pub async fn open<T: Decode>(
    path: impl AsRef<Path>,
    mut on_data: impl FnMut(T::Data<'_>),
  ) -> io::Result<Self> {
    let file = read_write(path).await?;
    let mut this = Self { pos: 0, file };
    this.load::<T>(&mut on_data).await?;
    Ok(this)
  }

  /// Load data from file with callback
  /// 从文件加载数据并回调
  async fn load<T: Decode>(&mut self, on_data: &mut impl FnMut(T::Data<'_>)) -> io::Result<()> {
    // Main buffer for accumulation
    // 用于累积的主缓冲区
    let mut buf = Vec::with_capacity(BUF_READ_SIZE);
    // Read buffer (swap space)
    // 读取缓冲区（交换空间）
    let mut chunk = vec![0u8; BUF_READ_SIZE];
    let mut file_pos = 0u64;

    loop {
      // Read into chunk
      // 读取到 chunk
      let result = self.file.read_at(chunk, file_pos).await;
      chunk = result.1;
      let n = result.0?;

      if n == 0 {
        break;
      }

      file_pos += n as u64;

      // Efficient buffer management
      // 高效的缓冲区管理
      if buf.is_empty() {
        // Zero-copy swap if buf is empty
        // 如果 buf 为空，零拷贝交换
        mem::swap(&mut buf, &mut chunk);
        buf.truncate(n);
        // Use resize to reuse memory instead of reallocating
        // 使用 resize 复用内存，而不是重新分配
        chunk.resize(BUF_READ_SIZE, 0);
      } else {
        buf.extend_from_slice(&chunk[..n]);
      }

      let mut offset = 0;
      // Parse loop
      // 解析循环
      while offset < buf.len() {
        let slice = &buf[offset..];
        let len = match T::decode(slice) {
          ParseResult::Ok(data, len) => {
            (*on_data)(data);
            self.pos += len as u64;
            len
          }
          ParseResult::NeedMore => 0,
          ParseResult::Err(e, skip) => {
            log::warn!("Load {e}, skipping {skip}");
            self.pos += skip as u64;
            skip
          }
        };
        if len == 0 {
          break;
        }
        offset += len;
      }

      // Compact buffer: move remaining bytes to front
      // 压缩缓冲区：将剩余字节移至前部
      if offset > 0 {
        if offset == buf.len() {
          buf.clear();
        } else {
          // O(remaining) copy, cheaper than drain
          // O(remaining) 拷贝，比 drain 更廉价
          buf.copy_within(offset.., 0);
          buf.truncate(buf.len() - offset);
        }
      }
    }

    Ok(())
  }
}

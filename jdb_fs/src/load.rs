//! Load trait for loading data from file
//! 从文件加载数据的 trait

use std::{future::Future, io, mem};

use compio::{fs::File, io::AsyncReadAt};

use crate::{
  consts::BUF_READ_SIZE,
  item::{Decode, ParseResult},
};

/// Parsed length
/// 解析长度
pub type Len = u64;

/// Load trait for loading data from file
/// 从文件加载数据的 trait
pub trait Load: Decode {
  type Loaded;

  /// Handle parse result, return parsed length
  /// 处理解析结果，返回解析长度
  fn on_parse(&mut self, result: ParseResult<Self::Data<'_>>) -> Len;

  /// Get final loaded result
  /// 获取最终加载结果
  fn end(&self) -> Self::Loaded;

  /// Load data from file
  /// 从文件加载数据
  fn load(&mut self, file: &File) -> impl Future<Output = io::Result<Self::Loaded>> {
    async {
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
        let result = file.read_at(chunk, file_pos).await;
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
          // Restore chunk capacity for next read
          // 恢复 chunk 容量以供下次读取
          if chunk.capacity() < BUF_READ_SIZE {
            chunk = vec![0u8; BUF_READ_SIZE];
          } else {
            chunk.resize(BUF_READ_SIZE, 0);
          }
        } else {
          buf.extend_from_slice(&chunk[..n]);
        }

        let mut offset = 0;
        // Parse loop
        // 解析循环
        while offset < buf.len() {
          let slice = &buf[offset..];
          let len = self.on_parse(Self::decode(slice));
          if len == 0 {
            break;
          }
          offset += len as usize;
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

      Ok(self.end())
    }
  }
}

//! AsyncReadAt implementation for BufFile
//! BufFile 的 AsyncReadAt 实现

use compio::{
  buf::{IntoInner, IoBufMut},
  io::AsyncReadAt,
};

use super::BufFile;

impl AsyncReadAt for BufFile {
  async fn read_at<T: IoBufMut>(&self, mut buf: T, pos: u64) -> compio::BufResult<usize, T> {
    let cap = buf.buf_capacity();
    if cap == 0 {
      return compio::BufResult(Ok(0), buf);
    }

    let mut total = 0;
    let mut cur_pos = pos;
    let end_pos = pos + cap as u64;

    while cur_pos < end_pos {
      let remain = (end_pos - cur_pos) as usize;
      let inner = self.i();

      // Try read from buffer
      // 尝试从缓冲读取
      if let Some(data) = inner.find(cur_pos, remain) {
        let n = data.len();
        let buf_ptr = buf.as_buf_mut_ptr();
        unsafe {
          std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(total), n);
        }
        total += n;
        cur_pos += n as u64;
        continue;
      }

      // Read from file
      // 从文件读取
      let file_read_len = inner.file_read_len(cur_pos, remain);
      if file_read_len == 0 {
        break;
      }

      if let Some(f) = &inner.file {
        let slice = buf.slice(total..total + file_read_len);

        let compio::BufResult(res, slice) = f.read_at(slice, cur_pos).await;
        buf = slice.into_inner();

        match res {
          Ok(n) if n > 0 => {
            total += n;
            cur_pos += n as u64;
            if n < file_read_len {
              break;
            }
          }
          _ => break,
        }
      } else {
        break;
      }
    }

    if total > 0 {
      unsafe { buf.set_buf_init(total) };
    }
    compio::BufResult(Ok(total), buf)
  }
}

//! Push functions for writing items
//! 写入条目的函数

use compio::io::AsyncWrite;
use zbin::Bin;

use crate::{
  Size,
  item::{Item, Result, write},
};

/// Push multiple items with data to writer
/// 写入多个条目和数据到写入器
pub async fn push_iter<'a, I: Item, D: Bin<'a>>(
  iter: impl IntoIterator<Item = (I::Head, D)>,
  w: &mut (impl AsyncWrite + Unpin),
) -> Result<Size>
where
  I::Head: 'static,
{
  let mut total = 0;
  for (head, data) in iter {
    total += write::<I>(head, data, w).await?;
  }
  Ok(total)
}

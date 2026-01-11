//! Push functions for writing items
//! 写入条目的函数

use compio::io::AsyncWrite;

use crate::{
  Len,
  item::{Item, Result, write},
};

/// Push multiple items to writer
/// 写入多个条目到写入器
pub async fn push_iter<'a, I: Item>(
  iter: impl IntoIterator<Item = &'a I::Head>,
  w: &mut (impl AsyncWrite + Unpin),
) -> Result<Len>
where
  I::Head: 'a,
{
  let mut total = 0;
  for data in iter {
    total += write::<I>(*data, &[], w).await?;
  }
  Ok(total)
}

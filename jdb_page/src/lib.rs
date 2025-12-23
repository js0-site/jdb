#![cfg_attr(docsrs, feature(doc_cfg))]

//! Buffer Manager 缓冲区管理器

mod cache;
mod page;

pub use cache::BufferPool;
pub use page::{Page, PageState};

#![cfg_attr(docsrs, feature(doc_cfg))]

mod consts;
mod err;
mod hash;

pub use consts::{BLOB_THRESHOLD, FILE_MAGIC, INVALID_PAGE_ID, PAGE_HEADER_SIZE, PAGE_SIZE};
pub use err::{E, R};
pub use hash::{hash128, hash64, now_sec};

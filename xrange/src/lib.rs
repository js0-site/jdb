#![cfg_attr(docsrs, feature(doc_cfg))]

mod borrow_range;
mod is_overlap;
mod overlap_for_sorted;

pub use borrow_range::BorrowRange;
pub use is_overlap::is_overlap;
pub use overlap_for_sorted::overlap_for_sorted;

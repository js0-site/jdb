#![cfg_attr(docsrs, feature(doc_cfg))]

mod is_overlap;
mod overlap_for_sorted;

pub use is_overlap::is_overlap;
pub use overlap_for_sorted::overlap_for_sorted;

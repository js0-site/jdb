// Adapter modules

#[cfg(feature = "jdb_slab")]
pub mod jdb_slab;

#[cfg(feature = "fjall")]
pub mod fjall;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

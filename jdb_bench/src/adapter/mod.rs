// Adapter modules

#[cfg(feature = "jdb")]
pub mod jdb;

#[cfg(feature = "sled")]
pub mod sled;

#[cfg(feature = "fjall")]
pub mod fjall;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

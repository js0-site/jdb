//! Random number generation for xor filters.
//!
//! This module provides different random number generation strategies that can be
//! selected via feature flags:
//! - `gxhash`: Uses GXHash finalization mix for fast non-cryptographic hashing

/// Applies a finalization mix to a randomly-seeded key, resulting in an avalanched hash.
/// This helps avoid high false-positive ratios (see Section 4 in the paper).
///
/// When the `gxhash` feature is enabled, this uses GXHash's finalization mix.
/// Otherwise, uses a simple fallback implementation.
#[inline]
#[cfg(feature = "gxhash")]
pub const fn mix(key: u64, seed: u64) -> u64 {
  crate::gxhash::mix64(key.overflowing_add(seed).0)
}

/// Applies a finalization mix to a randomly-seeded key, resulting in an avalanched hash.
/// This helps avoid high false-positive ratios (see Section 4 in the paper).
///
/// Fallback implementation when gxhash is not enabled.
#[inline]
#[cfg(not(feature = "gxhash"))]
pub const fn mix(key: u64, seed: u64) -> u64 {
  let k = key.overflowing_add(seed).0;
  k ^ k >> 33
}
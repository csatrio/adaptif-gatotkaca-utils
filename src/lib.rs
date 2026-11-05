//! Low-level building blocks used by the adaptif-gatotkaca transport system.
//!
//! ```
//! use adaptif_gatotkaca_utils::FastU64Map;
//!
//! let mut map = FastU64Map::new();
//! map.insert(42, "answer");
//! assert_eq!(map.get(&42), Some(&"answer"));
//! ```
//!
//! - [`fast_hashmap`]: [`FastU64Map`], a `HashMap` keyed by `u64` that skips hashing.
//! - [`refcounted`]: [`GkRefCounted`], an `Arc`-like pointer with an intrusive reference count.
//! - [`treiber_stack`]: lock-free Treiber stack helpers and a bounded [`TreiberQueue`].
//!
//! The crate's dependencies, [`crossbeam_utils`] and [`nohash_hasher`], are
//! re-exported at the root, so you can use them without depending on them yourself.

// Re-exported so dependents use the exact versions this crate's public types
// are built on, without adding their own dependency (or risking a mismatch).
pub use crossbeam_utils;
pub use nohash_hasher;

pub mod fast_hashmap;
pub mod refcounted;
pub mod treiber_stack;

pub use fast_hashmap::{FastU64Map, SqliteFastU64Map};
pub use refcounted::{GkIntrinsicRefCounted, GkRefCounted};
pub use treiber_stack::{
    GkTreiberPtr, GkTreiberStackIterator, TreiberNode, TreiberQueue, TreiberStackIterator,
};

// Compile and run the README examples as part of `cargo test`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

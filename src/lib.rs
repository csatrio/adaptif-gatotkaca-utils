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
//! - [`treiber_channel::channel_waker`]: [`GkWaker`], how a channel's receiver sleeps and is woken (std threads by default, async
//!   tasks through [`GkTaskWaker`], or your own thread pool or runtime).
//! - [`treiber_channel::channel_memory`]: [`GkChannelMemory`], where a channel allocates (the heap by default, or a region you map
//!   yourself through [`GkRegionMemory`]).
//! - [`treiber_channel`]: [`GkTreiberChannel`], a many-producer single-consumer channel made of per-thread Treiber stacks.
//! - [`treiber_channel::mpmc`]: [`GkTreiberMpmc`], a many-producer many-consumer job queue for thread pools: one lane per
//!   producer, each worker reaps a whole lane of intrusive jobs at once, oldest first, and shares the rest so a blocked
//!   worker does not hold it.
//!
//! The crate's dependencies, [`crossbeam_utils`] and [`nohash_hasher`], are
//! re-exported at the root, so you can use them without depending on them yourself.

// Re-exported so dependents use the exact versions this crate's public types
// are built on, without adding their own dependency (or risking a mismatch).
pub use crossbeam_utils;
pub use nohash_hasher;

pub mod fast_hashmap;
pub mod refcounted;
pub mod treiber_channel;
pub mod treiber_stack;

pub use fast_hashmap::{FastU64Map, SqliteFastU64Map};
pub use refcounted::{GkIntrinsicRefCounted, GkRefCounted};
pub use treiber_channel::channel_memory::{
    GkChannelMemory, GkHeapMemory, GkRegionError, GkRegionMemory,
};
pub use treiber_channel::channel_waker::{GkAsyncWaker, GkTaskWaker, GkThreadWaker, GkWaker};
pub use treiber_channel::mpmc::{
    GK_MPMC_MAX_CHAINS, GK_MPMC_MAX_LANES, GK_MPMC_RESCUE_AFTER, GkMpmcChain, GkMpmcLane,
    GkTreiberMpmc, GkTreiberMpmcConfig,
};
pub use treiber_channel::{
    GK_GROW_NODES, GK_LIFO_SHORT_CHAIN, GK_RETURN_BATCH, GK_SLAB_NODES, GK_SPIN_POLLS, GkReceiver,
    GkRecvFuture, GkSender, GkTreiberChannel, GkTreiberChannelConfig, GkTreiberChannelError,
    GkTrySendError,
};
pub use treiber_stack::{
    GkTreiberPtr, GkTreiberStackIterator, TreiberNode, TreiberQueue, TreiberStackIterator,
};

// Compile and run the README examples as part of `cargo test`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

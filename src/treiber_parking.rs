//! Treiber stacks that also tell producers when the consumer is asleep.
//!
//! - [`GkParkingStack`]: an unbounded many-producer single-consumer stack whose head doubles as the consumer's sleep
//!   flag. A push reports whether it must wake the consumer, so a wake-up (an eventfd write, an unpark) happens once
//!   per sleep, not once per push.
//! - [`GkParkingQueue`]: the same stack with a size limit, using [`TreiberQueue`](crate::TreiberQueue)'s admission
//!   counter. A push into a full queue is refused and the caller keeps the node.
//!
//! # Protocol
//!
//! The head holds one of three kinds of value:
//!
//! | head | meaning |
//! |---|---|
//! | `PARKED` | the consumer is asleep: the next push must wake it |
//! | null | the consumer is awake (draining, or about to run); nothing is queued |
//! | a node | nodes are queued; the consumer is awake or a wake-up is already on its way |
//!
//! `PARKED` is the address of a private static, never a node and never dereferenced. It is a value of its own, not a
//! tag bit on node pointers.
//!
//! - **Push**: one compare-and-swap, the same as a plain Treiber push. It returns `true` when it replaced `PARKED`.
//!   Exactly one push per sleep does, and that caller must wake the consumer.
//! - **Reap**: [`reap`](GkParkingStack::reap) takes every queued node with one swap and leaves the head null. From then
//!   on the consumer counts as awake: pushes do not wake it.
//! - **Park**: when a reap finds nothing, the consumer calls [`try_park`](GkParkingStack::try_park) (null to
//!   `PARKED`). It fails if a node arrived in between; reap again.
//!
//! Invariant: while the head is not `PARKED`, the consumer is running or a wake-up is guaranteed. No push is
//! lost, and wake-ups are never needed while the consumer is busy.
//!
//! A consumer whose wake-up signal is level-triggered (an eventfd, a semaphore count) must **clear the signal before
//! `try_park`**, never after. Clearing after a successful park could swallow the wake-up of a push that landed in
//! between.
//!
//! ```text
//! consumer loop:
//!     loop {
//!         while let Some(chain) = stack.reap() { process(chain) }
//!         signal.clear();                 // before parking
//!         if stack.try_park() { signal.wait(); }
//!     }
//! producer:
//!     if unsafe { stack.push(node) } { signal.notify(); }
//! ```
//!
//! # Memory ordering
//!
//! - Push: the compare-and-swap is `AcqRel` on success. `Release` publishes the node (payload and `next`) to the
//!   reaper. `Acquire` pairs with the consumer's park, so everything the consumer did before parking (clearing its
//!   signal) happens before the producer's wake-up.
//! - Reap: an `Acquire` swap. Every push is a read-modify-write on the head, so all of them belong to one release
//!   sequence and the swap sees every node's writes.
//! - Park: a `Release` compare-and-swap (see Push).
//! - [`is_parked`](GkParkingStack::is_parked), [`is_empty`](GkParkingStack::is_empty) and the queue's count are hints
//!   (`Relaxed`).
//!
//! # ABA
//!
//! Nodes are only ever taken all at once, and a push sets `next` to the very value its compare-and-swap expects. A
//! head that went `A -> null -> ... -> A` still yields a valid stack, so no hazard pointers, epochs or tags are needed.
//! Only the consumer writes null and `PARKED`.

use crate::TreiberNode;
use core::ptr::{self, NonNull};
use crossbeam_utils::{Backoff, CachePadded};

#[cfg(not(gk_loom))]
use core::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
#[cfg(gk_loom)]
use loom::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Its address is `PARKED`. A `u8` with static storage has an address no node can share.
static PARKED_SENTINEL: u8 = 0;

#[inline(always)]
const fn parked<N>() -> *mut N {
    (&raw const PARKED_SENTINEL).cast_mut().cast()
}

// ============================================================================
// GkParkingStack
// ============================================================================

/// An unbounded many-producer single-consumer Treiber stack that knows whether its consumer is asleep.
///
/// See the [module docs](self) for the protocol. Nodes are intrusive ([`TreiberNode`]): pushing allocates nothing.
/// The stack starts **parked**, so the first push wakes the consumer.
///
/// Dropping the stack frees the nodes still in it with [`TreiberNode::drop_node`].
pub struct GkParkingStack<N: TreiberNode> {
    head: CachePadded<AtomicPtr<N>>,
}

// Safety: the stack only moves node pointers between threads; nodes are owned by whoever pushed or reaped them.
unsafe impl<N: TreiberNode + Send> Send for GkParkingStack<N> {}
unsafe impl<N: TreiberNode + Send> Sync for GkParkingStack<N> {}

impl<N: TreiberNode> Default for GkParkingStack<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N: TreiberNode> GkParkingStack<N> {
    /// An empty stack with the consumer parked.
    #[cfg(not(gk_loom))]
    pub const fn new() -> Self {
        Self { head: CachePadded::new(AtomicPtr::new(parked())) }
    }

    /// An empty stack with the consumer parked.
    #[cfg(gk_loom)]
    pub fn new() -> Self {
        Self { head: CachePadded::new(AtomicPtr::new(parked())) }
    }

    /// Pushes one node. Returns `true` if the consumer was parked: the caller must wake it.
    ///
    /// Lock-free (a compare-and-swap loop), callable from any number of threads.
    ///
    /// # Safety
    /// `node` must be valid, not in any stack, and owned by the caller. After the call the stack owns it: the caller
    /// must not touch it again until the consumer hands it back.
    #[inline]
    #[must_use = "when this returns true the consumer is asleep and must be woken"]
    pub unsafe fn push(&self, node: NonNull<N>) -> bool {
        // Safety: forwarded from the caller.
        unsafe { self.push_chain(node, node) }
    }

    /// Pushes a chain `first ..= last`, already linked through `next` with `first` the newest, in one
    /// compare-and-swap. Returns `true` if the consumer was parked. Use [`TreiberNode::link_chain`] to build the chain.
    ///
    /// # Safety
    /// Every node of the chain must be valid, not in any stack, and owned by the caller; `first` and `last` belong to
    /// the same chain. `last.next` is overwritten.
    #[must_use = "when this returns true the consumer is asleep and must be woken"]
    pub unsafe fn push_chain(&self, first: NonNull<N>, last: NonNull<N>) -> bool {
        let backoff = Backoff::new();
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // `PARKED` is a state, not a node: the new chain ends there.
            let link = if head == parked() { ptr::null_mut() } else { head };
            // Safety: the caller owns the chain until the compare-and-swap succeeds.
            unsafe { last.as_ref().next().store(link, core::sync::atomic::Ordering::Relaxed) };
            match self.head.compare_exchange_weak(head, first.as_ptr(), Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return head == parked(),
                Err(actual) => {
                    head = actual;
                    backoff.spin();
                }
            }
        }
    }

    /// Takes every queued node, **newest first** (reverse with [`TreiberNode::reverse_in_place`] for push order).
    /// `None` if nothing was queued. Consumer only.
    ///
    /// Afterwards the consumer counts as awake (even if the stack was parked), so it must call
    /// [`try_park`](Self::try_park) before it sleeps. The caller owns the returned chain.
    #[inline]
    pub fn reap(&self) -> Option<NonNull<N>> {
        // A null head needs no write: polling an awake, empty stack does not take the line from the producers.
        if self.head.load(Ordering::Relaxed).is_null() {
            return None;
        }
        let head = self.head.swap(ptr::null_mut(), Ordering::Acquire);
        if head == parked() { None } else { NonNull::new(head) }
    }

    /// Tries to put the consumer to sleep. Returns `true` if it is now parked: the next push will wake it. Returns
    /// `false` if nodes arrived since the last reap: reap them first. Consumer only, wait-free.
    ///
    /// Clear any level-triggered wake-up signal **before** calling this (see the module docs).
    #[inline]
    pub fn try_park(&self) -> bool {
        match self.head.compare_exchange(ptr::null_mut(), parked(), Ordering::Release, Ordering::Relaxed) {
            Ok(_) => true,
            Err(actual) => {
                // Only the consumer writes PARKED, and it was awake: a failed park means nodes are waiting.
                debug_assert!(actual != parked(), "try_park called while already parked");
                false
            }
        }
    }

    /// Whether the consumer is parked. A hint: a push may change it at any time.
    #[inline]
    pub fn is_parked(&self) -> bool {
        self.head.load(Ordering::Relaxed) == parked()
    }

    /// Whether no node is queued. A hint: use [`reap`](Self::reap) to take nodes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        head.is_null() || head == parked()
    }
}

impl<N: TreiberNode> Drop for GkParkingStack<N> {
    fn drop(&mut self) {
        let mut node = self.head.swap(ptr::null_mut(), Ordering::Acquire);
        if node == parked() {
            return;
        }
        while !node.is_null() {
            // Safety: `&mut self`: nobody else can reach the chain; every node in it is valid.
            unsafe {
                let next = (*node).next().load(core::sync::atomic::Ordering::Relaxed);
                N::drop_node(node);
                node = next;
            }
        }
    }
}

// ============================================================================
// GkParkingQueue
// ============================================================================

/// A push was refused because the queue is full. The caller still owns the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GkQueueFull;

impl core::fmt::Display for GkQueueFull {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("queue is full")
    }
}

impl std::error::Error for GkQueueFull {}

/// A [`GkParkingStack`] with a size limit, for work that may be refused (an HTTP server answering 503).
///
/// The count is of nodes **admitted and not yet released**: a push adds one, and the consumer calls
/// [`release`](Self::release) once it is done with nodes (reaping alone does not). That makes the limit exact
/// across reaps, unlike [`TreiberQueue::reap_all`](crate::TreiberQueue::reap_all).
///
/// The limit is approximate under a race: the count can briefly reach `limit` plus the number of threads pushing at
/// that moment, before the refused pushes subtract again. A node is never admitted unless the count was below `limit`.
pub struct GkParkingQueue<N: TreiberNode> {
    stack: GkParkingStack<N>,
    count: CachePadded<AtomicUsize>,
    limit: usize,
}

impl<N: TreiberNode> GkParkingQueue<N> {
    /// An empty queue that admits up to about `limit` nodes, with the consumer parked.
    #[cfg(not(gk_loom))]
    pub const fn new(limit: usize) -> Self {
        Self { stack: GkParkingStack::new(), count: CachePadded::new(AtomicUsize::new(0)), limit }
    }

    /// An empty queue that admits up to about `limit` nodes, with the consumer parked.
    #[cfg(gk_loom)]
    pub fn new(limit: usize) -> Self {
        Self { stack: GkParkingStack::new(), count: CachePadded::new(AtomicUsize::new(0)), limit }
    }

    /// Pushes `node` unless the queue is full. `Ok(true)`: the consumer was parked and must be woken. `Err`: refused,
    /// the caller still owns `node`.
    ///
    /// # Safety
    /// As [`GkParkingStack::push`]. On `Err` the node was not touched.
    #[inline]
    pub unsafe fn try_push(&self, node: NonNull<N>) -> Result<bool, GkQueueFull> {
        // A full queue refuses with one shared load, without two read-modify-writes on the counter's line.
        if self.count.load(Ordering::Relaxed) >= self.limit {
            return Err(GkQueueFull);
        }
        // Relaxed: the count publishes nothing; the push's compare-and-swap does.
        if self.count.fetch_add(1, Ordering::Relaxed) >= self.limit {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return Err(GkQueueFull);
        }
        // Safety: forwarded from the caller.
        Ok(unsafe { self.stack.push(node) })
    }

    /// Takes every queued node, newest first. See [`GkParkingStack::reap`]. Does not change the count.
    #[inline]
    pub fn reap(&self) -> Option<NonNull<N>> {
        self.stack.reap()
    }

    /// Gives `n` admitted nodes back to the limit. Consumer side, after it is done with them.
    #[inline]
    pub fn release(&self, n: usize) {
        if n != 0 {
            let before = self.count.fetch_sub(n, Ordering::Relaxed);
            debug_assert!(before >= n, "released more nodes than were admitted");
        }
    }

    /// See [`GkParkingStack::try_park`].
    #[inline]
    pub fn try_park(&self) -> bool {
        self.stack.try_park()
    }

    /// See [`GkParkingStack::is_parked`].
    #[inline]
    pub fn is_parked(&self) -> bool {
        self.stack.is_parked()
    }

    /// Admitted and not yet released nodes. A hint.
    #[inline]
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Whether the count is 0. A hint.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the count has reached the limit. A hint.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.limit
    }

    /// The limit given to [`new`](Self::new).
    #[inline]
    pub fn limit(&self) -> usize {
        self.limit
    }
}

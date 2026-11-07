//! Many-producer, one-consumer channel built from per-thread Treiber stacks.
//!
//! Every producing thread owns a [`GkSender`] and with it a thread channel of three stacks:
//!
//! - `local`: empty nodes the owner can fill. Only the owner touches it, so it is a plain chain, no atomics.
//! - `inbox`: empty nodes coming home from the receiver.
//! - `outbox`: filled nodes submitted to the receiver. The owner pushes, the receiver takes the whole chain.
//!
//! A sender takes a node from `local`, else takes the whole `inbox` into `local`, else grows. Each thread channel
//! starts with one slab of [`GkTreiberChannelConfig::slab_nodes`] nodes, so a warm sender never allocates. An unbounded
//! sender that runs out grows by a whole chunk of [`GkTreiberChannelConfig::grow_nodes`] nodes in one allocation, never
//! one node at a time. Like a `Vec`, it keeps what it grew into (no allocate / free churn under bursts) until it is
//! dropped, or until [`GkSender::shrink`] frees the chunks whose nodes are all at home.
//!
//! # Ownership of the payload
//!
//! [`GkReceiver::recv`] moves the payload out of its node and returns it: the receiver owns the value and drops it on
//! its own thread. The emptied node goes home at once, without waiting for the value. The receiver collects emptied
//! nodes per producer and sends them home with one compare-and-swap per batch (at most
//! [`GkTreiberChannelConfig::return_batch`] nodes, and always before it runs out of jobs), so the sender finds empty
//! nodes in its inbox, ready for the next job.
//!
//! # Waking the receiver
//!
//! The [`GkReceiver`] polls the outboxes, with `Backoff::spin` between polls
//! ([`GkTreiberChannelConfig::spin_polls`]), before it sleeps through its [`GkWaker`] (`std::thread::park` by default;
//! a thread pool or runtime can supply its own, and [`GkTaskWaker`](crate::GkTaskWaker) receives from async code with
//! [`GkReceiver::recv_async`]). A sleeping receiver publishes its waker; the first producer to swap it out calls
//! `unpark`, so a sleeping receiver is woken once, not once per send. The waker is handed back after the unpark and
//! reused by the next sleep, so sleeping does not allocate.
//!
//! A send publishes its node with a `SeqCst` compare-and-swap and then reads the waiter with a `SeqCst` load; the
//! receiver publishes the waiter, runs a `SeqCst` fence and polls. Either the producer sees the waiter or the receiver
//! sees the job, and the send pays for no separate fence.
//!
//! A poll takes every outbox with one atomic swap each, reverses each chain (FIFO per producer) and links the chains
//! after the chain still pending, so a wakeup that arrives meanwhile is simply chained on (see
//! [`GkReceiver::collect`]).
//!
//! # Lifetime of a thread channel
//!
//! A thread channel is freed by whoever accounts for the last of: the owning sender (dropped), the receiver's
//! registration (retired) and every node still away from home. The hot path pays no counter for this: dropping the
//! sender swaps the inbox head to a sentinel and adds `nodes away + 1` to `pending`; a batch of nodes or the receiver
//! that arrives later sees the sentinel and subtracts its share. The side that brings `pending` to zero frees the slab
//! and the channel.
//!
//! # Allocation
//!
//! Every allocation goes through the channel's [`GkChannelMemory`] (the heap by default, or a region you map yourself
//! through [`GkTreiberChannel::with_memory`] and [`GkRegionMemory`](crate::GkRegionMemory)). Only four things allocate:
//! creating the channel (its shared state), creating a sender (its thread channel and slab), taking the receiver (its
//! waker), and an unbounded send whose sender has no empty node left. Warm sends and receives never allocate, and no
//! error path does. When the memory is exhausted, creating fails with `OUT_OF_MEMORY` and an unbounded send returns
//! `Full`.
//!
//! # One producer
//!
//! [`GkTreiberChannelConfig::single_producer`] makes a 1:1 channel: one sender for the channel's whole life, polled
//! directly by the receiver.
//!
//! Order is FIFO per producer and unspecified between producers.

// ==== Import Definition ====

pub mod channel_memory;
pub mod channel_waker;

use crate::treiber_stack::{GkTreiberPtr, TreiberNode};
use channel_memory::{GkChannelMemory, GkHeapMemory};
use channel_waker::{GkAsyncWaker, GkThreadWaker, GkWaker};
use crossbeam_utils::{Backoff, CachePadded};

use std::{
    fmt,
    marker::PhantomData,
    mem::MaybeUninit,
    alloc::Layout,
    future::Future,
    ops::Deref,
    pin::Pin,
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicPtr, AtomicUsize, Ordering, fence},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

// ==== Constants ====

/// Default for [`GkTreiberChannelConfig::slab_nodes`].
pub const GK_SLAB_NODES: usize = 4096;
/// Default for [`GkTreiberChannelConfig::grow_nodes`].
pub const GK_GROW_NODES: usize = 4096;
/// Default for [`GkTreiberChannelConfig::spin_polls`].
pub const GK_SPIN_POLLS: usize = 16;
/// Default for [`GkTreiberChannelConfig::return_batch`].
pub const GK_RETURN_BATCH: usize = 64;

// ==== Error Definition ====

/// Why a channel call failed.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct GkTreiberChannelError(u8);

impl GkTreiberChannelError {
    /// The receiver is gone.
    pub const CLOSED: Self = Self(2);
    /// The single receiver was already taken.
    pub const RECEIVER_TAKEN: Self = Self(3);
    /// Every sender is gone and nothing is left to receive.
    pub const DISCONNECTED: Self = Self(4);
    /// `recv_timeout` found nothing in time.
    pub const TIMEOUT: Self = Self(5);
    /// [`GkTreiberChannel::with_config`] was given a configuration it cannot use.
    pub const INVALID_CONFIG: Self = Self(6);
    /// The channel's [`GkChannelMemory`] had no room left.
    pub const OUT_OF_MEMORY: Self = Self(7);
    /// A `single_producer` channel already handed out its sender.
    pub const SENDER_TAKEN: Self = Self(8);

    /// The numeric code of the error.
    #[inline(always)]
    pub const fn code(self) -> u8 {
        self.0
    }

    const fn name(self) -> &'static str {
        // BRANCH: one arm per named constant
        match self.0 {
            2 => "the receiver is gone",
            3 => "the receiver was already taken",
            4 => "all senders are gone and the channel is empty",
            5 => "timed out waiting for a job",
            6 => "invalid treiber channel configuration",
            7 => "the channel memory is exhausted",
            8 => "the single sender was already taken",
            _ => "unknown treiber channel error",
        }
    }
}

impl fmt::Debug for GkTreiberChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GkTreiberChannelError({}: {})", self.0, self.name())
    }
}

impl fmt::Display for GkTreiberChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::error::Error for GkTreiberChannelError {}

/// Why [`GkSender::try_send`] did not send; the value is handed back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum GkTrySendError<T> {
    /// A bounded channel whose sender has every node in flight.
    Full(T),
    /// The receiver is gone.
    Closed(T),
}

impl<T> fmt::Debug for GkTrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full(_) => "GkTrySendError::Full(..)",
            Self::Closed(_) => "GkTrySendError::Closed(..)",
        })
    }
}

// ==== Config Definition ====

/// Settings of one channel, fixed when it is created. Every field defaults to the matching `GK_*` constant.
///
/// ```
/// use adaptif_gatotkaca_utils::{GkTreiberChannel, GkTreiberChannelConfig};
///
/// let config = GkTreiberChannelConfig::DEFAULT
///     .with_slab_nodes(256)
///     .with_bounded(true);
/// let channel = GkTreiberChannel::<u64>::with_config(config).unwrap();
/// # drop(channel);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GkTreiberChannelConfig {
    /// Nodes allocated up front, in one slab, for every sender. In a bounded channel this is the most jobs a sender
    /// can have sent and not yet received. Must be at least 1 when `bounded`.
    pub slab_nodes: usize,
    /// Nodes an unbounded sender adds, in one allocation, each time it runs out. It keeps them until it is dropped or
    /// [`GkSender::shrink`] is called. Must be at least 1 when not `bounded`.
    pub grow_nodes: usize,
    /// How many times the receiver polls, waiting with `Backoff::spin` (1, 2, 4 ... up to 64 spin hints) between
    /// polls, before it parks the thread. A producer that is about to send is then answered without a park and an
    /// unpark. 0 parks right away.
    pub spin_polls: usize,
    /// Most emptied nodes the receiver holds for one producer before it sends them home in one batch. Larger batches
    /// cost fewer atomic operations; smaller ones hand nodes back sooner. Must be at least 1.
    pub return_batch: usize,
    /// Senders never allocate beyond their slab: [`GkSender::send`] waits and [`GkSender::try_send`] returns `Full`.
    pub bounded: bool,
    /// One sender for the channel's whole life (a 1:1 channel): the second `sender()` fails with `SENDER_TAKEN`, and the
    /// receiver polls that sender's outbox directly instead of walking its list of senders.
    pub single_producer: bool,
}

impl GkTreiberChannelConfig {
    /// Every field at its `GK_*` default, unbounded.
    pub const DEFAULT: Self = Self {
        slab_nodes: GK_SLAB_NODES,
        grow_nodes: GK_GROW_NODES,
        spin_polls: GK_SPIN_POLLS,
        return_batch: GK_RETURN_BATCH,
        bounded: false,
        single_producer: false,
    };

    /// Sets [`slab_nodes`](Self::slab_nodes).
    pub const fn with_slab_nodes(mut self, slab_nodes: usize) -> Self {
        self.slab_nodes = slab_nodes;
        self
    }

    /// Sets [`grow_nodes`](Self::grow_nodes).
    pub const fn with_grow_nodes(mut self, grow_nodes: usize) -> Self {
        self.grow_nodes = grow_nodes;
        self
    }

    /// Sets [`spin_polls`](Self::spin_polls).
    pub const fn with_spin_polls(mut self, spin_polls: usize) -> Self {
        self.spin_polls = spin_polls;
        self
    }

    /// Sets [`return_batch`](Self::return_batch).
    pub const fn with_return_batch(mut self, return_batch: usize) -> Self {
        self.return_batch = return_batch;
        self
    }

    /// Sets [`bounded`](Self::bounded).
    pub const fn with_bounded(mut self, bounded: bool) -> Self {
        self.bounded = bounded;
        self
    }

    /// Sets [`single_producer`](Self::single_producer).
    pub const fn with_single_producer(mut self, single_producer: bool) -> Self {
        self.single_producer = single_producer;
        self
    }

    /// Checks the rules listed on the fields.
    pub const fn validate(&self) -> Result<(), GkTreiberChannelError> {
        // BRANCH: a bounded sender without a slab, or an unbounded one that cannot grow, could never send; an empty batch
        // is meaningless
        if (self.bounded && self.slab_nodes == 0)
            || (!self.bounded && self.grow_nodes == 0)
            || self.return_batch == 0
        {
            return Err(GkTreiberChannelError::INVALID_CONFIG);
        }
        Ok(())
    }
}

impl Default for GkTreiberChannelConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// ==== Memory Helpers ====

/// Moves `value` into memory from `memory`. `None` (and `value` dropped) if the memory is exhausted; never panics.
#[inline]
fn alloc_value<X, M: GkChannelMemory>(memory: &M, value: X) -> Option<*mut X> {
    let ptr = memory.allocate(Layout::new::<X>())?.as_ptr().cast::<X>();
    // Safety: fresh memory for one `X`.
    unsafe { ptr.write(value) };
    Some(ptr)
}

/// Drops the value at `ptr` and gives its memory back.
///
/// # Safety
/// `ptr` must come from [`alloc_value`] on a clone of `memory`, hold a live value and not be used again.
#[inline]
unsafe fn free_value<X, M: GkChannelMemory>(memory: &M, ptr: *mut X) {
    // Safety: forwarded from this function's contract.
    unsafe {
        ptr::drop_in_place(ptr);
        memory.deallocate(NonNull::new_unchecked(ptr.cast()), Layout::new::<X>());
    }
}

// ==== Node Definition ====

/// One job slot. Lives in a thread channel's slab or in one of the chunks it grew into; freed with the channel.
struct GkJobNode<T, M: GkChannelMemory> {
    next: AtomicPtr<GkJobNode<T, M>>,
    /// The thread channel this node goes home to.
    home: *const GkThreadChannel<T, M>,
    /// Initialised from the send until the receiver moves it out; uninitialised everywhere else.
    payload: MaybeUninit<T>,
}

impl<T, M: GkChannelMemory> GkJobNode<T, M> {
    /// The thread channel this node goes home to.
    #[inline(always)]
    fn channel(&self) -> *const GkThreadChannel<T, M> {
        self.home
    }
}

/// Writes `len` empty nodes of `channel` at `base`, chained `base[0] -> base[1] -> ... -> tail`. Returns `base` (or
/// `tail` when `len` is 0).
///
/// # Safety
/// `base` must be valid for writes of `len` nodes that nothing else references.
unsafe fn link_nodes<T, M: GkChannelMemory>(
    base: *mut GkJobNode<T, M>,
    len: usize,
    channel: *const GkThreadChannel<T, M>,
    tail: *mut GkJobNode<T, M>,
) -> *mut GkJobNode<T, M> {
    for i in 0..len {
        // Safety: `i` is inside the block; `i + 1` is at most one past the end.
        unsafe {
            let next = if i + 1 < len { base.add(i + 1) } else { tail };
            base.add(i).write(GkJobNode {
                next: AtomicPtr::new(next),
                home: channel,
                payload: MaybeUninit::uninit(),
            });
        }
    }
    if len == 0 { tail } else { base }
}

/// Header of a block of `len` nodes an unbounded sender grew into; the nodes follow it in the same allocation.
struct GkChunk {
    next: *mut GkChunk,
    len: usize,
    /// Scratch for [`GkSender::shrink`]: nodes of this chunk found at home.
    home: usize,
}

/// Layout of a chunk of `len` nodes, and the offset of its first node.
fn chunk_layout<T, M: GkChannelMemory>(len: usize) -> Option<(Layout, usize)> {
    let (layout, offset) = Layout::new::<GkChunk>()
        .extend(Layout::array::<GkJobNode<T, M>>(len).ok()?)
        .ok()?;
    Some((layout.pad_to_align(), offset))
}

/// First node of `chunk`.
#[inline(always)]
fn chunk_nodes<T, M: GkChannelMemory>(chunk: *mut GkChunk) -> *mut GkJobNode<T, M> {
    let (_, offset) = chunk_layout::<T, M>(0).expect("an empty chunk has a layout");
    chunk.cast::<u8>().wrapping_add(offset).cast()
}

impl<T, M: GkChannelMemory> TreiberNode for GkJobNode<T, M> {
    type Value = MaybeUninit<T>;

    #[inline(always)]
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &MaybeUninit<T> {
        &self.payload
    }

    /// Never called: the channel walks its job chains itself and never leaves one in a stack iterator.
    unsafe fn drop_node(_ptr: *mut Self) {}
}

/// The inbox head once the owner is gone. Never a valid node: nodes are aligned and this is 1.
#[inline(always)]
fn orphan<T, M: GkChannelMemory>() -> *mut GkJobNode<T, M> {
    ptr::without_provenance_mut(1)
}

/// Pushes a filled node onto an outbox and publishes it with a `SeqCst` compare-and-swap. `SeqCst` (instead of
/// `Release`) lets the following waiter check in [`GkShared::wake_if_parked`] go without a separate fence.
///
/// # Safety
/// `node` must be valid, owned by the caller and in no stack; `outbox` must belong to the caller's thread channel.
#[inline(always)]
unsafe fn submit<T, M: GkChannelMemory>(outbox: &AtomicPtr<GkJobNode<T, M>>, node: *mut GkJobNode<T, M>) {
    let mut head = outbox.load(Ordering::Relaxed);
    loop {
        // Safety: the node is ours until the CAS publishes it.
        unsafe { (*node).next.store(head, Ordering::Relaxed) };
        match outbox.compare_exchange_weak(head, node, Ordering::SeqCst, Ordering::Relaxed) {
            // BRANCH: published
            Ok(_) => return,
            // BRANCH: the receiver took the chain meanwhile (only it competes with the owner), retry on the new head
            Err(current) => head = current,
        }
    }
}

/// Sends `count` empty nodes of `channel`, linked `first -> ... -> last`, home with one compare-and-swap.
///
/// # Safety
/// The chain must be valid, owned by the caller, belong to `channel` and hold no initialised payload. `count >= 1`.
unsafe fn return_chain<T, M: GkChannelMemory>(
    channel: *const GkThreadChannel<T, M>,
    first: *mut GkJobNode<T, M>,
    last: *mut GkJobNode<T, M>,
    count: usize,
) {
    // Safety: a thread channel outlives every node away from home (see `pending`).
    let inbox = unsafe { &(*channel).inbox };
    let backoff = Backoff::new();
    let mut head = inbox.load(Ordering::Relaxed);
    loop {
        // BRANCH: the owner is gone, so there is no inbox to return to
        if head == orphan() {
            // Safety: forwarded from this function's contract.
            unsafe { return_orphaned(channel, first, count) };
            return;
        }
        // Safety: the chain is ours until the CAS publishes it.
        unsafe { (*last).next.store(head, Ordering::Relaxed) };
        // Release: the sender's Acquire swap must see the nodes' links (and that the payloads were moved out).
        match inbox.compare_exchange_weak(head, first, Ordering::Release, Ordering::Relaxed) {
            // BRANCH: published
            Ok(_) => return,
            // BRANCH: lost a race with the sender taking its inbox, retry on the new head
            Err(current) => {
                head = current;
                backoff.spin();
            }
        }
    }
}

/// Nodes came home after their owner was dropped: free the heap ones and release their counts.
///
/// # Safety
/// Same as [`return_chain`].
#[cold]
unsafe fn return_orphaned<T, M: GkChannelMemory>(channel: *const GkThreadChannel<T, M>, _first: *mut GkJobNode<T, M>, count: usize) {
    // Every node lives in the slab or a chunk, both freed with the channel: only the counts are released.
    // Safety: the nodes held `count` counts; the channel is alive until they are released.
    unsafe { release(channel, count as isize) };
}

// ==== Thread Channel Definition ====

/// The outbox head and the owner-gone flag share a cache line: the receiver reads the flag right after the head.
struct GkOutbox<T, M: GkChannelMemory> {
    /// Submitted nodes: the owner pushes, the receiver takes the chain.
    head: AtomicPtr<GkJobNode<T, M>>,
    /// Set by the owner's drop, after its last push.
    gone: AtomicBool,
}

/// The shared stacks of one producing thread (`local` lives in its [`GkSender`]).
struct GkThreadChannel<T, M: GkChannelMemory> {
    /// Empty nodes coming home from the receiver; `orphan()` once the owner is gone.
    inbox: CachePadded<AtomicPtr<GkJobNode<T, M>>>,
    outbox: CachePadded<GkOutbox<T, M>>,
    /// Link in the registry stack, then in the receiver's private list.
    registry_next: AtomicPtr<GkThreadChannel<T, M>>,
    /// Counts still owed after the owner is gone; see the module docs.
    pending: AtomicIsize,
    /// The slab (`slab_len` nodes), freed with the channel.
    slab: *mut GkJobNode<T, M>,
    slab_len: usize,
    /// Chunks the owner grew into, freed with the channel. Only the owner changes the list (on growth and `shrink`).
    chunks: AtomicPtr<GkChunk>,
    /// Where the channel, its slab and its heap nodes were allocated. Kept here so whoever frees the channel (any
    /// thread, after the receiver and the sender are gone) can give the memory back.
    memory: M,
}

impl<T, M: GkChannelMemory> TreiberNode for GkThreadChannel<T, M> {
    type Value = ();

    #[inline(always)]
    fn next(&self) -> &AtomicPtr<Self> {
        &self.registry_next
    }

    fn value(&self) -> &() {
        &()
    }

    /// Channels are never freed by an iterator; the owner and `pending` decide.
    unsafe fn drop_node(_ptr: *mut Self) {}
}

/// Gives back `count` counts of `pending`; the caller that reaches zero frees the channel.
///
/// # Safety
/// `channel` must be valid and the caller must hold `count` of the counts that `pending` is waiting for.
unsafe fn release<T, M: GkChannelMemory>(channel: *const GkThreadChannel<T, M>, count: isize) {
    // Safety: the channel is valid while the caller holds a count.
    // AcqRel: the thread that frees must see every other thread's last use of the channel.
    let before = unsafe { (*channel).pending.fetch_sub(count, Ordering::AcqRel) };
    // BRANCH: these were the last counts
    if before == count {
        // Safety: nobody else can reach the channel any more.
        unsafe { free_channel(channel) };
    }
}

/// Frees the slab and the channel.
///
/// # Safety
/// No node of the channel may be in use and no pointer to it may be used afterwards.
unsafe fn free_channel<T, M: GkChannelMemory>(channel: *const GkThreadChannel<T, M>) {
    // Safety: `slab` and `channel` come from `GkTreiberChannel::sender` and are freed once.
    unsafe {
        let memory = (*channel).memory.clone();
        let mut chunk = (*channel).chunks.load(Ordering::Relaxed);
        // BRANCH: one iteration per chunk the owner grew into
        while !chunk.is_null() {
            let (next, len) = ((*chunk).next, (*chunk).len);
            free_chunk::<T, M>(&memory, chunk, len);
            chunk = next;
        }
        free_slab(&memory, (*channel).slab, (*channel).slab_len);
        free_value(&memory, channel as *mut GkThreadChannel<T, M>);
    }
}

/// Memory for `len` nodes, uninitialised; a dangling pointer for none. `None` if the memory is exhausted.
fn alloc_slab<T, M: GkChannelMemory>(memory: &M, len: usize) -> Option<*mut GkJobNode<T, M>> {
    let layout = Layout::array::<GkJobNode<T, M>>(len).ok()?;
    // BRANCH: an empty slab takes no memory
    if layout.size() == 0 {
        return Some(NonNull::dangling().as_ptr());
    }
    Some(memory.allocate(layout)?.as_ptr().cast())
}

/// Gives a chunk back (its nodes hold no payload, so nothing is dropped).
///
/// # Safety
/// `chunk` must come from [`GkSender::grow`] on a clone of `memory` with `len` nodes, and not be used again.
unsafe fn free_chunk<T, M: GkChannelMemory>(memory: &M, chunk: *mut GkChunk, len: usize) {
    let (layout, _) = chunk_layout::<T, M>(len).expect("chunk layout was valid when allocated");
    // Safety: forwarded from this function's contract.
    unsafe { memory.deallocate(NonNull::new_unchecked(chunk.cast()), layout) };
}

/// Gives a slab back (its nodes hold no payload, so nothing is dropped).
///
/// # Safety
/// `slab` must come from [`alloc_slab`] on a clone of `memory` with the same `len`, and not be used again.
unsafe fn free_slab<T, M: GkChannelMemory>(memory: &M, slab: *mut GkJobNode<T, M>, len: usize) {
    let layout = Layout::array::<GkJobNode<T, M>>(len).expect("slab layout was valid when allocated");
    // BRANCH: an empty slab took no memory
    if layout.size() != 0 {
        // Safety: forwarded from this function's contract.
        unsafe { memory.deallocate(NonNull::new_unchecked(slab.cast()), layout) };
    }
}

/// Drops the payloads of everything in an outbox and sends the nodes home in one batch.
///
/// # Safety
/// `channel` must be valid.
unsafe fn drain_outbox<T, M: GkChannelMemory>(channel: *const GkThreadChannel<T, M>) {
    // Safety: the outbox is valid; reaping gives this thread the whole chain.
    let outbox = unsafe { &(*channel).outbox };
    let first = outbox.head.reap_all().into_inner();
    // BRANCH: nothing submitted
    if first.is_null() {
        return;
    }
    let (mut node, mut last, mut count) = (first, first, 0usize);
    while !node.is_null() {
        // Safety: nodes of a reaped chain are valid, owned by this loop, and hold a payload.
        unsafe {
            let next = (*node).next.load(Ordering::Relaxed);
            ptr::drop_in_place((*node).payload.as_mut_ptr());
            last = node;
            node = next;
        }
        count += 1;
    }
    // Safety: the chain is ours, empty now, and all of `channel`.
    unsafe { return_chain(channel, first, last, count) };
}

/// Counts a private chain of empty nodes.
///
/// # Safety
/// The chain must be null-terminated, valid and owned by the caller.
unsafe fn count_chain<T, M: GkChannelMemory>(mut node: *mut GkJobNode<T, M>) -> usize {
    let mut count = 0;
    while !node.is_null() {
        // Safety: forwarded from this function's contract.
        node = unsafe { (*node).next.load(Ordering::Relaxed) };
        count += 1;
    }
    count
}

// ==== Shared State Definition ====

/// State shared by the receiver and every sender. Allocated in the channel's memory and counted by [`GkSharedRef`].
struct GkShared<T, W: GkWaker, M: GkChannelMemory> {
    /// The receiver's waker while it is (about to be) asleep; null otherwise. The receiver publishes it before it parks;
    /// the first producer to swap it out calls `unpark` and hands it back through `spare`.
    waiter: CachePadded<AtomicPtr<W>>,
    /// Thread channels that have not been seen by the receiver yet.
    registry: CachePadded<AtomicPtr<GkThreadChannel<T, M>>>,
    /// The receiver's waker while it is not published in `waiter` (and not held by a waking producer). There is one per
    /// receiver, allocated with it, so parking never allocates.
    spare: AtomicPtr<W>,
    /// Handles to this state: channel factories, senders and the receiver.
    refs: AtomicUsize,
    /// Live senders; the receiver reports `DISCONNECTED` when it reaches zero and the chains are empty.
    senders: AtomicUsize,
    /// Set when the receiver is dropped.
    closed: AtomicBool,
    /// Set by `receiver()`; there is one receiver.
    receiver_taken: AtomicBool,
    /// Set by the first `sender()` of a `single_producer` channel.
    sender_taken: AtomicBool,
    config: GkTreiberChannelConfig,
    memory: M,
}

impl<T, W: GkWaker, M: GkChannelMemory> GkShared<T, W, M> {
    /// Wakes the receiver if it is parked. Call right after a `SeqCst` publish of the change it must see.
    #[inline(always)]
    fn wake_if_parked(&self) {
        // SeqCst: with the caller's SeqCst publish, either this load sees the waiter or the receiver's poll (after its
        // SeqCst fence) sees the publish.
        // BRANCH: nobody is waiting, which is almost always
        if !self.waiter.load(Ordering::SeqCst).is_null() {
            self.wake();
        }
    }

    #[cold]
    #[inline(never)]
    fn wake(&self) {
        // Claim the waiter: only the producer that swaps it out unparks, so a parked receiver costs one wakeup, not one
        // per send. Acquire: the receiver's `prepare` / `register` must be visible before `unpark` reads the waker.
        let waiter = self.waiter.swap(ptr::null_mut(), Ordering::Acquire);
        // BRANCH: another producer (or the receiver itself) claimed it first
        if !waiter.is_null() {
            // Safety: the swap gave this thread the only claim on the receiver's waker; `unpark` takes `&self`.
            unsafe { (*waiter).unpark() };
            self.stash(waiter);
        }
    }

    /// Hands the receiver's waker back for its next park.
    #[inline]
    fn stash(&self, waker: *mut W) {
        // Release: our last use of the waker happens before the receiver changes it again.
        let previous = self.spare.swap(waker, Ordering::Release);
        debug_assert!(previous.is_null(), "one waker per receiver");
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> Drop for GkShared<T, W, M> {
    fn drop(&mut self) {
        for waker in [*self.waiter.get_mut(), *self.spare.get_mut()] {
            // BRANCH: the receiver's waker is still around
            if !waker.is_null() {
                // Safety: nothing else can reach the shared state any more; the waker came from `alloc_value`.
                unsafe { free_value(&self.memory, waker) };
            }
        }
        // Every sender and the receiver are gone, so these channels are orphaned and nobody else uses the registry.
        let mut channel = self.registry.reap_all().into_inner();
        // BRANCH: one iteration per channel that the receiver never saw
        while !channel.is_null() {
            // Safety: the channel is valid until its registration is released, so read the link first.
            let next = unsafe { (*channel).registry_next.load(Ordering::Relaxed) };
            unsafe { retire(channel) };
            channel = next;
        }
    }
}

/// A counted handle to the shared state, like `Arc` but allocated in, and freed to, the channel's memory.
struct GkSharedRef<T, W: GkWaker, M: GkChannelMemory> {
    ptr: NonNull<GkShared<T, W, M>>,
}

impl<T, W: GkWaker, M: GkChannelMemory> GkSharedRef<T, W, M> {
    /// Allocates the shared state with one handle (this one). `None` if the memory is exhausted.
    fn new(shared: GkShared<T, W, M>) -> Option<Self> {
        let memory = shared.memory.clone();
        let ptr = alloc_value(&memory, shared)?;
        // Safety: `alloc_value` never returns null.
        Some(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        })
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> Clone for GkSharedRef<T, W, M> {
    #[inline]
    fn clone(&self) -> Self {
        // Relaxed: a new handle is made from a live one, which keeps the state alive.
        self.refs.fetch_add(1, Ordering::Relaxed);
        Self { ptr: self.ptr }
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> Deref for GkSharedRef<T, W, M> {
    type Target = GkShared<T, W, M>;

    #[inline(always)]
    fn deref(&self) -> &GkShared<T, W, M> {
        // Safety: the state lives while a handle exists.
        unsafe { self.ptr.as_ref() }
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> Drop for GkSharedRef<T, W, M> {
    fn drop(&mut self) {
        // Release: our uses of the state happen before whoever frees it.
        // BRANCH: other handles remain
        if self.refs.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        fence(Ordering::Acquire);
        // Safety: this was the last handle; the memory clone outlives the free.
        unsafe {
            let memory = self.ptr.as_ref().memory.clone();
            free_value(&memory, self.ptr.as_ptr());
        }
    }
}

/// The receiver is done with a thread channel: drop what is left in its outbox and release the registration.
///
/// # Safety
/// `channel` must be valid and registered, and must not be retired twice.
unsafe fn retire<T, M: GkChannelMemory>(channel: *const GkThreadChannel<T, M>) {
    // Safety: forwarded from this function's contract.
    unsafe {
        drain_outbox(channel);
        release(channel, 1);
    }
}

// ==== Channel Definition ====

/// Creates senders and the receiver of one channel.
///
/// - `W`: how the receiver sleeps and is woken ([`GkThreadWaker`] by default; see [`GkWaker`]).
/// - `M`: where the channel allocates ([`GkHeapMemory`] by default; see [`GkChannelMemory`] and
///   [`with_memory`](Self::with_memory)).
pub struct GkTreiberChannel<T, W: GkWaker = GkThreadWaker, M: GkChannelMemory = GkHeapMemory> {
    shared: GkSharedRef<T, W, M>,
}

// Safety: the factory only hands out senders and the receiver; the shared state is atomics, `W` and `M` are `Send + Sync`.
unsafe impl<T: Send, W: GkWaker, M: GkChannelMemory> Send for GkTreiberChannel<T, W, M> {}
unsafe impl<T: Send, W: GkWaker, M: GkChannelMemory> Sync for GkTreiberChannel<T, W, M> {}

impl<T, W: GkWaker, M: GkChannelMemory> Clone for GkTreiberChannel<T, W, M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T: Send, W: GkWaker, M: GkChannelMemory + Default> Default for GkTreiberChannel<T, W, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send, W: GkWaker, M: GkChannelMemory + Default> GkTreiberChannel<T, W, M> {
    /// Creates an unbounded channel with the default configuration.
    pub fn new() -> Self {
        Self::with_memory(GkTreiberChannelConfig::DEFAULT, M::default()).expect("default memory allocates")
    }

    /// Creates a bounded channel with the default configuration: every sender owns exactly
    /// [`slab_nodes`](GkTreiberChannelConfig::slab_nodes) nodes and never allocates more. When all of them hold jobs the
    /// receiver has not taken yet, [`GkSender::send`] waits (spinning, then yielding) and [`GkSender::try_send`] returns
    /// `Full`. The bound is per sender, so the channel holds at most `senders * slab_nodes` jobs.
    pub fn new_bounded() -> Self {
        Self::with_memory(GkTreiberChannelConfig::DEFAULT.with_bounded(true), M::default())
            .expect("default memory allocates")
    }

    /// Creates a channel with `config`. Fails with `INVALID_CONFIG` (allocating nothing) if
    /// [`GkTreiberChannelConfig::validate`] rejects it.
    pub fn with_config(config: GkTreiberChannelConfig) -> Result<Self, GkTreiberChannelError> {
        Self::with_memory(config, M::default())
    }
}

impl<T: Send, W: GkWaker, M: GkChannelMemory> GkTreiberChannel<T, W, M> {
    /// Creates a channel whose every allocation (shared state, thread channels, slabs, heap nodes, the receiver's waker)
    /// comes from `memory`. Fails with `INVALID_CONFIG` if `config` is rejected and with `OUT_OF_MEMORY` if `memory` has
    /// no room; neither error allocates.
    pub fn with_memory(config: GkTreiberChannelConfig, memory: M) -> Result<Self, GkTreiberChannelError> {
        config.validate()?;
        let shared = GkSharedRef::new(GkShared {
            waiter: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            registry: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            spare: AtomicPtr::new(ptr::null_mut()),
            refs: AtomicUsize::new(1),
            senders: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            receiver_taken: AtomicBool::new(false),
            sender_taken: AtomicBool::new(false),
            config,
            memory,
        })
        .ok_or(GkTreiberChannelError::OUT_OF_MEMORY)?;
        Ok(Self { shared })
    }

    /// The configuration the channel was created with.
    pub fn config(&self) -> GkTreiberChannelConfig {
        self.shared.config
    }

    /// The memory the channel allocates from.
    pub fn memory(&self) -> &M {
        &self.shared.memory
    }

    /// Creates the sender of the calling thread: allocates its thread channel (one slab) and registers it. Fails with
    /// `CLOSED` if the receiver is gone, `SENDER_TAKEN` on the second call of a `single_producer` channel, and
    /// `OUT_OF_MEMORY` if the memory has no room (nothing stays allocated).
    ///
    /// A sender is meant to be used by one thread at a time; it can be moved to another thread.
    pub fn sender(&self) -> Result<GkSender<T, W, M>, GkTreiberChannelError> {
        // BRANCH: no point registering with a receiver that is gone
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(GkTreiberChannelError::CLOSED);
        }
        let config = self.shared.config;
        // BRANCH: a single-producer channel has exactly one sender for its whole life
        if config.single_producer && self.shared.sender_taken.swap(true, Ordering::AcqRel) {
            return Err(GkTreiberChannelError::SENDER_TAKEN);
        }
        let memory = &self.shared.memory;
        let nodes = config.slab_nodes;
        let channel = alloc_slab::<T, M>(memory, nodes).and_then(|slab| {
            let channel = GkThreadChannel {
                inbox: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
                outbox: CachePadded::new(GkOutbox {
                    head: AtomicPtr::new(ptr::null_mut()),
                    gone: AtomicBool::new(false),
                }),
                registry_next: AtomicPtr::new(ptr::null_mut()),
                pending: AtomicIsize::new(0),
                slab,
                slab_len: nodes,
                chunks: AtomicPtr::new(ptr::null_mut()),
                memory: memory.clone(),
            };
            let allocated = alloc_value(memory, channel);
            // BRANCH: no room for the channel; give the slab back
            if allocated.is_none() {
                // Safety: the slab was just allocated with this length and holds nothing.
                unsafe { free_slab(memory, slab, nodes) };
            }
            allocated.map(|channel| (channel, slab))
        });
        let Some((channel, slab)) = channel else {
            // BRANCH: let a later call try again
            if config.single_producer {
                self.shared.sender_taken.store(false, Ordering::Release);
            }
            return Err(GkTreiberChannelError::OUT_OF_MEMORY);
        };
        // The channel exists first, so the slab is built and chained into the local list in one pass.
        // Safety: the slab is fresh and holds `nodes` nodes.
        let local = unsafe { link_nodes(slab, nodes, channel, ptr::null_mut()) };
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        // Release (inside the push) publishes the initialised channel to the receiver.
        self.shared.registry.push_to_stack(channel);
        Ok(GkSender {
            channel,
            local,
            allocated: nodes,
            grow_nodes: config.grow_nodes,
            bounded: config.bounded,
            shared: self.shared.clone(),
            _not_sync: PhantomData,
        })
    }

    /// Takes the single receiver, with a default waker. See [`receiver_with_waker`](Self::receiver_with_waker).
    pub fn receiver(&self) -> Result<GkReceiver<T, W, M>, GkTreiberChannelError>
    where
        W: Default,
    {
        self.receiver_with_waker(W::default())
    }

    /// Takes the single receiver, which sleeps and is woken with `waker`. Fails with `RECEIVER_TAKEN` if it was taken
    /// already, and with `OUT_OF_MEMORY` if the memory has no room for the waker (the receiver can be taken again).
    pub fn receiver_with_waker(&self, waker: W) -> Result<GkReceiver<T, W, M>, GkTreiberChannelError> {
        // BRANCH: only one receiver may exist
        if self.shared.receiver_taken.swap(true, Ordering::AcqRel) {
            return Err(GkTreiberChannelError::RECEIVER_TAKEN);
        }
        // The one waker the receiver parks with, for its whole life, so parking never allocates.
        let Some(waker) = alloc_value(&self.shared.memory, waker) else {
            self.shared.receiver_taken.store(false, Ordering::Release);
            return Err(GkTreiberChannelError::OUT_OF_MEMORY);
        };
        self.shared.stash(waker);
        let config = self.shared.config;
        Ok(GkReceiver {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            returns: GkReturnBatch::EMPTY,
            channels: ptr::null_mut(),
            single: config.single_producer,
            published: false,
            return_batch: config.return_batch,
            spin_polls: config.spin_polls,
            shared: self.shared.clone(),
            _not_sync: PhantomData,
        })
    }
}

// ==== Sender Definition ====

/// The producing end owned by one thread. Dropping it ends the thread channel; jobs still in flight are delivered.
pub struct GkSender<T, W: GkWaker = GkThreadWaker, M: GkChannelMemory = GkHeapMemory> {
    channel: *const GkThreadChannel<T, M>,
    /// Empty nodes ready to fill; only this sender touches them.
    local: *mut GkJobNode<T, M>,
    /// Nodes this thread channel owns (slab + chunks), at home or away.
    allocated: usize,
    /// Copied from the config so the send path does not reach into the shared state for them.
    grow_nodes: usize,
    bounded: bool,
    shared: GkSharedRef<T, W, M>,
    _not_sync: PhantomData<std::cell::Cell<()>>,
}

// Safety: the sender can move to another thread as a whole (the local chain moves with it); `Cell` in the marker keeps it !Sync.
unsafe impl<T: Send, W: GkWaker, M: GkChannelMemory> Send for GkSender<T, W, M> {}

impl<T: Send, W: GkWaker, M: GkChannelMemory> GkSender<T, W, M> {
    /// Submits `value` to the receiver. Hands the value back if the receiver is gone. On a bounded channel it first waits
    /// for an empty node if every node of this sender holds a job the receiver has not taken yet.
    pub fn send(&mut self, mut value: T) -> Result<(), T> {
        // BRANCH: one try is the common case; the backoff is only built when the bounded channel is full
        match self.try_send(value) {
            Ok(()) => return Ok(()),
            Err(GkTrySendError::Closed(back)) => return Err(back),
            Err(GkTrySendError::Full(back)) => value = back,
        }
        let backoff = Backoff::new();
        loop {
            backoff.snooze();
            match self.try_send(value) {
                // BRANCH: an empty node came home
                Ok(()) => return Ok(()),
                // BRANCH: the receiver is gone
                Err(GkTrySendError::Closed(back)) => return Err(back),
                // BRANCH: still full; wait for the receiver to take a job
                Err(GkTrySendError::Full(back)) => value = back,
            }
        }
    }

    /// Submits `value` without waiting. Fails with `Full` on a bounded channel whose nodes are all in flight, or when the
    /// channel's memory has no room for another node.
    #[inline(always)]
    pub fn try_send(&mut self, value: T) -> Result<(), GkTrySendError<T>> {
        // BRANCH: the receiver is gone
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(GkTrySendError::Closed(value));
        }
        let node = self.local;
        // BRANCH: a local node is the common case; otherwise refill from the inbox or allocate
        let node = if !node.is_null() {
            // Safety: local nodes are valid and owned by this sender.
            self.local = unsafe { (*node).next.load(Ordering::Relaxed) };
            node
        } else {
            match self.take_node_slow() {
                Some(node) => node,
                // BRANCH: a bounded channel with no empty node
                None => return Err(GkTrySendError::Full(value)),
            }
        };
        // Safety: the node is ours and its payload is uninitialised; the channel lives at least as long as this sender.
        unsafe {
            (*node).payload.write(value);
            let outbox = &(*self.channel).outbox;
            submit(&outbox.head, node);
        }
        self.shared.wake_if_parked();
        Ok(())
    }

    /// Number of nodes this thread channel owns (the slab plus the chunks it grew into).
    pub fn allocated(&self) -> usize {
        self.allocated
    }

    /// `local` is empty: take the whole inbox, else allocate (unbounded only).
    #[inline(never)]
    fn take_node_slow(&mut self) -> Option<*mut GkJobNode<T, M>> {
        self.refill_local();
        let node = self.local;
        // BRANCH: empty nodes came home
        if !node.is_null() {
            // Safety: local nodes are valid and owned by this sender.
            self.local = unsafe { (*node).next.load(Ordering::Relaxed) };
            return Some(node);
        }
        // BRANCH: a bounded channel never allocates
        if self.bounded {
            return None;
        }
        // BRANCH: the memory is exhausted; like a full bounded channel, wait for a node to come home
        let node = self.grow()?;
        // Safety: the chunk's nodes are ours.
        self.local = unsafe { (*node).next.load(Ordering::Relaxed) };
        Some(node)
    }

    /// Adds a chunk of `grow_nodes` nodes (one allocation) to the empty `local` list and returns its first node, or
    /// `None` if the memory is exhausted.
    #[cold]
    fn grow(&mut self) -> Option<*mut GkJobNode<T, M>> {
        let len = self.grow_nodes;
        let (layout, _) = chunk_layout::<T, M>(len)?;
        let chunk = self.shared.memory.allocate(layout)?.as_ptr().cast::<GkChunk>();
        // Safety: the channel lives at least as long as this sender, and only the owner changes its chunk list.
        let chunks = unsafe { &(*self.channel).chunks };
        // Safety: fresh memory for the header and `len` nodes.
        unsafe {
            chunk.write(GkChunk {
                next: chunks.load(Ordering::Relaxed),
                len,
                home: 0,
            })
        };
        chunks.store(chunk, Ordering::Relaxed);
        self.allocated += len;
        // Safety: as above.
        Some(unsafe { link_nodes(chunk_nodes::<T, M>(chunk), len, self.channel, ptr::null_mut()) })
    }

    /// Frees the chunks this sender grew into whose nodes are all at home (not sent, or sent and received), and
    /// returns how many nodes that freed. The slab is never freed. Costs a walk of the nodes at home; call it when a
    /// burst is over, not on the hot path.
    pub fn shrink(&mut self) -> usize {
        // Safety: the channel lives at least as long as this sender, and only the owner changes its chunk list.
        let chunks = unsafe { &(*self.channel).chunks };
        let first = chunks.load(Ordering::Relaxed);
        // BRANCH: nothing was grown
        if first.is_null() {
            return 0;
        }
        // Every node at home goes to `local`, so the count per chunk below is complete.
        // Safety: the channel lives at least as long as this sender; the swapped chain is ours.
        let returned = unsafe { (*self.channel).inbox.swap(ptr::null_mut(), Ordering::Acquire) };
        let mut node = returned;
        while !node.is_null() {
            // Safety: the swapped chain is ours.
            let next = unsafe { (*node).next.load(Ordering::Relaxed) };
            unsafe { (*node).next.store(self.local, Ordering::Relaxed) };
            self.local = node;
            node = next;
        }
        // Safety (this block): the chunk list and the local chain belong to this sender.
        unsafe {
            let mut chunk = first;
            while !chunk.is_null() {
                (*chunk).home = 0;
                chunk = (*chunk).next;
            }
            let mut hint = ptr::null_mut();
            let mut node = self.local;
            while !node.is_null() {
                hint = Self::chunk_of(first, hint, node);
                // BRANCH: slab nodes belong to no chunk
                if !hint.is_null() {
                    (*hint).home += 1;
                }
                node = (*node).next.load(Ordering::Relaxed);
            }
            // Keep only the nodes of chunks that stay.
            let (mut kept, mut node) = (ptr::null_mut(), self.local);
            hint = ptr::null_mut();
            while !node.is_null() {
                let next = (*node).next.load(Ordering::Relaxed);
                hint = Self::chunk_of(first, hint, node);
                // BRANCH: drop nodes of chunks that are entirely at home
                if hint.is_null() || (*hint).home != (*hint).len {
                    (*node).next.store(kept, Ordering::Relaxed);
                    kept = node;
                }
                node = next;
            }
            self.local = kept;
            // Unlink and free the chunks that are entirely at home.
            let (mut freed, mut link, mut chunk) = (0, chunks.as_ptr(), first);
            while !chunk.is_null() {
                let next = (*chunk).next;
                // BRANCH: free a chunk with every node at home, keep the others
                if (*chunk).home == (*chunk).len {
                    *link = next;
                    freed += (*chunk).len;
                    free_chunk::<T, M>(&self.shared.memory, chunk, (*chunk).len);
                } else {
                    link = &raw mut (*chunk).next;
                }
                chunk = next;
            }
            self.allocated -= freed;
            freed
        }
    }

    /// The chunk `node` belongs to (null for a slab node), trying `hint` first: neighbours in the list are usually
    /// neighbours in memory.
    ///
    /// # Safety
    /// `first` must be the sender's chunk list and `hint` null or one of its chunks.
    unsafe fn chunk_of(first: *mut GkChunk, hint: *mut GkChunk, node: *mut GkJobNode<T, M>) -> *mut GkChunk {
        let contains = |chunk: *mut GkChunk| {
            let start = chunk_nodes::<T, M>(chunk);
            // Safety: forwarded from this function's contract.
            let end = start.wrapping_add(unsafe { (*chunk).len });
            start <= node && node < end
        };
        // BRANCH: same chunk as the previous node is the common case
        if !hint.is_null() && contains(hint) {
            return hint;
        }
        let mut chunk = first;
        while !chunk.is_null() {
            if contains(chunk) {
                return chunk;
            }
            // Safety: forwarded from this function's contract.
            chunk = unsafe { (*chunk).next };
        }
        ptr::null_mut()
    }

    /// Moves the inbox into `local` (which is empty). Order does not matter, so the swapped chain simply becomes `local`.
    #[inline]
    fn refill_local(&mut self) {
        // Safety: the channel lives at least as long as this sender.
        let inbox = unsafe { &(*self.channel).inbox };
        // BRANCH: an empty inbox is only read, so the receiver keeps the cache line
        if inbox.load(Ordering::Relaxed).is_null() {
            return;
        }
        // Acquire pairs with the receiver's Release CAS in `return_chain`.
        self.local = inbox.swap(ptr::null_mut(), Ordering::Acquire);
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> Drop for GkSender<T, W, M> {
    fn drop(&mut self) {
        let channel = self.channel;
        // Count the nodes that are home (they are freed with the channel, slab and chunks alike).
        // Safety: the local chain is ours.
        let mut home = unsafe { count_chain(self.local) };
        // Closing the inbox: from now on a returning batch sees `orphan()` and releases counts instead.
        // Safety: the channel is valid; this sender is its owner, so the swapped chain is ours.
        home += unsafe { count_chain((*channel).inbox.swap(orphan(), Ordering::AcqRel)) };
        // Release: a receiver that sees the flag also sees every push before it.
        // Safety: the channel is valid.
        let outbox = unsafe { &(*channel).outbox };
        outbox.gone.store(true, Ordering::Release);
        // If the receiver is gone nobody takes the outbox: drop its jobs here. They see `orphan()` and release counts.
        // The fences pair with the receiver's: either it sees our last push, or we see `closed`.
        fence(Ordering::SeqCst);
        // BRANCH: the receiver is gone, so the outbox would never be taken
        if self.shared.closed.load(Ordering::Relaxed) {
            // Safety: the channel is valid; this sender is its owner.
            unsafe { drain_outbox(channel) };
        }
        // Nodes away from home each owe one count, and so does the receiver's registration.
        let owed = (self.allocated - home) as isize + 1;
        // Safety: the channel is valid until the count reaches zero.
        let now = unsafe { (*channel).pending.fetch_add(owed, Ordering::AcqRel) } + owed;
        // BRANCH: everything was already returned
        if now == 0 {
            // Safety: nothing else can reach the channel.
            unsafe { free_channel(channel) };
        }
        // Let the receiver notice the disconnect. SeqCst: `wake_if_parked` relies on a SeqCst publish before it.
        self.shared.senders.fetch_sub(1, Ordering::SeqCst);
        self.shared.wake_if_parked();
    }
}

// ==== Receiver Definition ====

/// Emptied nodes of one thread channel, waiting to go home together: `first -> ... -> last`, newest first.
struct GkReturnBatch<T, M: GkChannelMemory> {
    channel: *const GkThreadChannel<T, M>,
    first: *mut GkJobNode<T, M>,
    last: *mut GkJobNode<T, M>,
    count: usize,
}

impl<T, M: GkChannelMemory> GkReturnBatch<T, M> {
    const EMPTY: Self = Self {
        channel: ptr::null(),
        first: ptr::null_mut(),
        last: ptr::null_mut(),
        count: 0,
    };
}

/// The consuming end. Takes jobs from every thread channel; there is one per channel.
///
/// Blocking receive ([`recv`](Self::recv), [`recv_timeout`](Self::recv_timeout)) sleeps with the waker `W`. With a
/// [`GkAsyncWaker`] such as [`GkTaskWaker`](crate::GkTaskWaker), [`poll_recv`](Self::poll_recv) and
/// [`recv_async`](Self::recv_async) receive from async code on any executor.
pub struct GkReceiver<T, W: GkWaker = GkThreadWaker, M: GkChannelMemory = GkHeapMemory> {
    /// Jobs taken from the outboxes and not handed out yet, oldest first: `head` is next, `tail` is last. A poll appends
    /// its chains after `tail`, so a wakeup that arrives while jobs are pending is just chained on.
    head: *mut GkJobNode<T, M>,
    tail: *mut GkJobNode<T, M>,
    /// Emptied nodes on their way home. Never holds anything while `head` is null, so an idle receiver strands no node.
    returns: GkReturnBatch<T, M>,
    /// Thread channels seen so far, linked through `registry_next` (no allocation to track them). In a
    /// `single_producer` channel it holds at most the one sender's channel, polled without walking or re-checking the
    /// registry.
    channels: *mut GkThreadChannel<T, M>,
    /// The channel is `single_producer`.
    single: bool,
    /// `poll_recv` returned `Pending` with the waker still published; taken back on the next receive.
    published: bool,
    return_batch: usize,
    spin_polls: usize,
    shared: GkSharedRef<T, W, M>,
    _not_sync: PhantomData<std::cell::Cell<()>>,
}

// Safety: the receiver moves as a whole; the raw pointers are thread channels and nodes it owns or that are safe to
// use from any thread.
unsafe impl<T: Send, W: GkWaker, M: GkChannelMemory> Send for GkReceiver<T, W, M> {}

impl<T: Send, W: GkWaker, M: GkChannelMemory> GkReceiver<T, W, M> {
    /// Waits for the next job and takes ownership of its value. Fails with `DISCONNECTED` once every sender is gone
    /// and nothing is left.
    pub fn recv(&mut self) -> Result<T, GkTreiberChannelError> {
        self.recv_until(None)
    }

    /// Like [`recv`](Self::recv) but gives up with `TIMEOUT` after `timeout`.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, GkTreiberChannelError> {
        self.recv_until(Some(Instant::now() + timeout))
    }

    /// Takes whatever the producers have submitted since the last poll and chains it after the jobs still pending, without
    /// waiting. Lets a consumer that is busy with a batch pick up new arrivals early; order stays FIFO per producer.
    /// Returns true if new jobs were found.
    pub fn collect(&mut self) -> bool {
        self.poll()
    }

    /// The next job's value if one is ready.
    #[inline]
    pub fn try_recv(&mut self) -> Option<T> {
        // BRANCH: take from the current chain first, then poll the outboxes once
        if let Some(value) = self.pop() {
            return Some(value);
        }
        self.poll();
        self.pop()
    }

    fn recv_until(&mut self, deadline: Option<Instant>) -> Result<T, GkTreiberChannelError> {
        self.unpublish();
        loop {
            // BRANCH: a job is ready
            if let Some(value) = self.pop() {
                return Ok(value);
            }
            // BRANCH: the outboxes had new jobs
            if self.poll() {
                continue;
            }
            // BRANCH: no senders left; the poll below sees everything they pushed before dropping
            if self.shared.senders.load(Ordering::Acquire) == 0 {
                self.poll();
                return self.pop().ok_or(GkTreiberChannelError::DISCONNECTED);
            }
            // Spin first: a producer that is about to send is answered without the cost of a sleep and a wakeup.
            // BRANCH: one look per iteration; `Backoff::spin` waits longer after each look (up to 64 spin hints)
            let backoff = Backoff::new();
            let mut found = false;
            for _ in 0..self.spin_polls {
                backoff.spin();
                if self.poll() {
                    found = true;
                    break;
                }
            }
            // BRANCH: the spin found jobs
            if found {
                continue;
            }
            // BRANCH: out of time (only `recv_timeout` has a deadline)
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(GkTreiberChannelError::TIMEOUT);
            }
            // Park: publish the waker, then look once more, so a producer either sees the waker or we see its job.
            // BRANCH: jobs arrived while waiting for the waker to come back
            let Some(waker) = self.announce(W::prepare) else {
                continue;
            };
            fence(Ordering::SeqCst);
            // BRANCH: a job or a disconnect arrived while announcing
            if self.poll() || self.shared.senders.load(Ordering::Acquire) == 0 {
                self.retract();
                continue;
            }
            let timeout = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            // Safety: the waker lives as long as the shared state; only this receiver changes it, and it is not
            // changing it now. A producer may be calling `unpark` on it at the same time, which `&self` allows.
            unsafe { (*waker).park(timeout) };
            self.retract();
        }
    }

    /// Publishes the receiver's waker after `prepare` updated it. Returns `None`, without publishing, if jobs arrived
    /// while the waker was still with the producer that last woke us.
    #[cold]
    fn announce(&mut self, prepare: impl FnOnce(&mut W)) -> Option<*mut W> {
        let backoff = Backoff::new();
        let waker = loop {
            // Acquire pairs with `stash`: the producer that held the waker is done with it.
            let waker = self.shared.spare.swap(ptr::null_mut(), Ordering::Acquire);
            // BRANCH: the waker is home, which is almost always
            if !waker.is_null() {
                break waker;
            }
            // The producer that claimed it is between its unpark and `stash`; look for jobs while it finishes.
            // BRANCH: jobs arrived, no need to park
            if self.poll() {
                return None;
            }
            backoff.snooze();
        };
        // Safety: the swap gave this receiver the only pointer to the waker.
        prepare(unsafe { &mut *waker });
        // SeqCst: part of the waiter / job handshake (see `GkShared::wake_if_parked`).
        self.shared.waiter.store(waker, Ordering::SeqCst);
        Some(waker)
    }
}

impl<T: Send, W: GkAsyncWaker, M: GkChannelMemory> GkReceiver<T, W, M> {
    /// Async receive: the next value, `Pending` (waking the task of `cx` when a sender pushes) if there is none yet, or
    /// `DISCONNECTED` once every sender is gone and nothing is left. Never blocks the thread.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, GkTreiberChannelError>> {
        self.unpublish();
        loop {
            // BRANCH: a job is ready
            if let Some(value) = self.pop() {
                return Poll::Ready(Ok(value));
            }
            // BRANCH: the outboxes had new jobs
            if self.poll() {
                continue;
            }
            // BRANCH: no senders left
            if self.shared.senders.load(Ordering::Acquire) == 0 {
                self.poll();
                return Poll::Ready(self.pop().ok_or(GkTreiberChannelError::DISCONNECTED));
            }
            // Register the task, publish, look once more: a producer either sees the waker or we see its job.
            // BRANCH: jobs arrived while waiting for the waker to come back
            if self.announce(|waker| waker.register(cx.waker())).is_none() {
                continue;
            }
            fence(Ordering::SeqCst);
            // BRANCH: a job or a disconnect arrived while registering
            if self.poll() || self.shared.senders.load(Ordering::Acquire) == 0 {
                self.retract();
                continue;
            }
            self.published = true;
            return Poll::Pending;
        }
    }

    /// Async receive as a future: `receiver.recv_async().await`.
    pub fn recv_async(&mut self) -> GkRecvFuture<'_, T, W, M> {
        GkRecvFuture { receiver: self }
    }
}

/// The future of [`GkReceiver::recv_async`].
pub struct GkRecvFuture<'a, T, W: GkAsyncWaker, M: GkChannelMemory> {
    receiver: &'a mut GkReceiver<T, W, M>,
}

impl<T: Send, W: GkAsyncWaker, M: GkChannelMemory> Future for GkRecvFuture<'_, T, W, M> {
    type Output = Result<T, GkTreiberChannelError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().receiver.poll_recv(cx)
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> GkReceiver<T, W, M> {
    /// Takes the waker back if no producer claimed it. If one did, its wakeup already happened (or is in flight).
    #[cold]
    fn retract(&self) {
        let waker = self.shared.waiter.swap(ptr::null_mut(), Ordering::Acquire);
        // BRANCH: nobody claimed it, so keep it for the next park
        if !waker.is_null() {
            self.shared.stash(waker);
        }
    }

    /// Takes back a waker left published by a `Pending` `poll_recv`.
    #[inline(always)]
    fn unpublish(&mut self) {
        // BRANCH: only after `poll_recv` returned `Pending`
        if self.published {
            self.published = false;
            self.retract();
        }
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> GkReceiver<T, W, M> {
    /// Moves the next value out of its node and sends the node home (in a batch).
    #[inline(always)]
    fn pop(&mut self) -> Option<T> {
        let node = self.head;
        // BRANCH: nothing pending
        if node.is_null() {
            return None;
        }
        // Safety: chain nodes are valid, owned by this receiver and hold a payload, which is read once here.
        let value = unsafe {
            self.head = (*node).next.load(Ordering::Relaxed);
            ptr::read((*node).payload.as_ptr())
        };
        // Safety: the node is empty now and ours.
        unsafe { self.recycle(node) };
        // BRANCH: the chain ran out: `tail` must not point at the node that left, and no node may wait idle
        if self.head.is_null() {
            self.tail = ptr::null_mut();
            self.flush_returns();
        }
        Some(value)
    }

    /// Adds an empty node to the return batch; sends the batch home first if the node goes elsewhere or it is full.
    ///
    /// # Safety
    /// `node` must be valid, owned by the receiver and empty.
    #[inline(always)]
    unsafe fn recycle(&mut self, node: *mut GkJobNode<T, M>) {
        // Safety: forwarded from this function's contract.
        let channel = unsafe { (*node).channel() };
        // BRANCH: same producer is the common case (a poll groups each producer's jobs)
        if channel == self.returns.channel {
            // Safety: the node is ours; it becomes the batch's newest.
            unsafe { (*node).next.store(self.returns.first, Ordering::Relaxed) };
        } else {
            self.flush_returns();
            self.returns.channel = channel;
            self.returns.last = node;
        }
        self.returns.first = node;
        self.returns.count += 1;
        // BRANCH: a full batch goes home at once
        if self.returns.count >= self.return_batch {
            self.flush_returns();
        }
    }

    /// Sends the return batch home with one compare-and-swap.
    #[inline]
    fn flush_returns(&mut self) {
        // BRANCH: nothing to send
        if self.returns.count == 0 {
            return;
        }
        let batch = &self.returns;
        // Safety: the batch is a valid chain of empty nodes of `batch.channel`, owned by this receiver.
        unsafe { return_chain(batch.channel, batch.first, batch.last, batch.count) };
        self.returns = GkReturnBatch::EMPTY;
    }

    /// Takes every outbox with one swap each, reverses each chain (FIFO per producer), links them and appends the result
    /// after the jobs still pending. Releases the thread channels whose owner is gone. Returns true if it found new jobs.
    fn poll(&mut self) -> bool {
        // BRANCH: a single-producer channel stops looking at the registry once it has its one sender
        if !(self.single && !self.channels.is_null()) {
            self.absorb_registry();
        }
        let mut first_new: *mut GkJobNode<T, M> = ptr::null_mut();
        let mut last_new: *mut GkJobNode<T, M> = ptr::null_mut();
        let mut prev: *mut GkThreadChannel<T, M> = ptr::null_mut();
        let mut channel = self.channels;
        // BRANCH: one pass over the registered thread channels
        while !channel.is_null() {
            // Safety: registered channels are valid until their registration is released below.
            let (next, taken, retired) = unsafe {
                let next = (*channel).registry_next.load(Ordering::Relaxed);
                let outbox = &(*channel).outbox;
                let mut taken = outbox.head.reap_all().into_inner();
                // Only an empty outbox needs the owner check (same cache line as the head). If the owner is gone, its
                // last push happened before the flag, so one more reap takes everything that will ever come.
                // BRANCH: the owner is gone and the outbox is empty for good
                let retired = taken.is_null() && outbox.gone.load(Ordering::Acquire);
                if retired {
                    taken = outbox.head.reap_all().into_inner();
                }
                (next, taken, retired)
            };
            // BRANCH: this channel had jobs; append them after the previous ones
            if !taken.is_null() {
                let (first, last, _count) = GkJobNode::reverse_in_place(taken);
                if last_new.is_null() {
                    first_new = first;
                } else {
                    // Safety: both chains are owned by this thread now.
                    unsafe { (*last_new).next.store(first, Ordering::Relaxed) };
                }
                last_new = last;
            }
            if retired {
                // BRANCH: unlink the channel from the private list
                if prev.is_null() {
                    self.channels = next;
                } else {
                    // Safety: `prev` is registered and owned by this list.
                    unsafe { (*prev).registry_next.store(next, Ordering::Relaxed) };
                }
                // Safety: released once; the jobs just taken keep the channel alive through `pending`.
                unsafe { release(channel, 1) };
            } else {
                prev = channel;
            }
            channel = next;
        }
        // BRANCH: nothing new
        if first_new.is_null() {
            return false;
        }
        // BRANCH: chain after the pending jobs, or start the chain
        if self.tail.is_null() {
            self.head = first_new;
        } else {
            // Safety: the pending chain is owned by this receiver.
            unsafe { (*self.tail).next.store(first_new, Ordering::Relaxed) };
        }
        self.tail = last_new;
        true
    }

    /// Moves newly registered thread channels to the front of the private list.
    #[inline]
    fn absorb_registry(&mut self) {
        // BRANCH: nothing new is the common case, and costs one load
        if self.shared.registry.is_empty() {
            return;
        }
        let fresh = self.shared.registry.reap_all().into_inner();
        let mut last = fresh;
        // Safety: the reaped chain is non-empty, valid and owned by this receiver now.
        unsafe {
            loop {
                let next = (*last).registry_next.load(Ordering::Relaxed);
                // BRANCH: walk to the end of the fresh chain
                if next.is_null() {
                    break;
                }
                last = next;
            }
            (*last).registry_next.store(self.channels, Ordering::Relaxed);
        }
        self.channels = fresh;
    }
}

impl<T, W: GkWaker, M: GkChannelMemory> Drop for GkReceiver<T, W, M> {
    fn drop(&mut self) {
        // A task waker left published by `poll_recv` must not be woken for a receiver that is gone.
        self.unpublish();
        // Senders now refuse new jobs. The fence pairs with the one in `GkSender::drop`.
        self.shared.closed.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        // Values taken but not handed out are dropped here; their nodes go home (the last pop flushes the batch).
        while self.pop().is_some() {}
        // Absorb what was registered meanwhile, then retire every thread channel.
        self.absorb_registry();
        let mut channel = self.channels;
        self.channels = ptr::null_mut();
        while !channel.is_null() {
            // Safety: each registered channel is retired exactly once; read the link before releasing it.
            let next = unsafe { (*channel).registry_next.load(Ordering::Relaxed) };
            unsafe { retire(channel) };
            channel = next;
        }
    }
}

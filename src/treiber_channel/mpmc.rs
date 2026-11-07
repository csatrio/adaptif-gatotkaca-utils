//! Many-producer, many-consumer job queue for a thread pool: one lane per producing runtime, any number of workers
//! each reaping a whole lane at once.
//!
//! - [`GkTreiberMpmc`]: the queue. Workers call [`try_reap`](GkTreiberMpmc::try_reap) on it directly; they do not
//!   register and hold no state of their own.
//! - [`GkMpmcLane`]: a producer's lane, taken with [`GkTreiberMpmc::lane`]. Its owner
//!   [`push`](GkMpmcLane::push)es jobs and can [`drain`](GkMpmcLane::drain) them back.
//!
//! Jobs are intrusive: the job type implements [`TreiberNode`] and links through its own `next` field. The queue moves
//! pointers only, so it never allocates per job and has no nodes to send home.
//!
//! # A lane
//!
//! One cache line: a Treiber stack of pushed jobs and two counters. The producer pushes with one `SeqCst`
//! compare-and-swap. A worker takes the whole stack with one swap (an empty lane costs only a load), reverses it and
//! gets the chain oldest first, as a [`TreiberStackIterator`]. The chain is the worker's: it runs the jobs itself, and
//! jobs it drops unrun are freed with [`TreiberNode::drop_node`].
//!
//! Order is FIFO within a chain. Chains of one lane reaped by different workers run side by side, so a later chain can
//! start before an earlier one finishes. A worker that blocks holds the rest of its chain. There is no order between
//! lanes.
//!
//! # Picking a lane
//!
//! Each push stores the lane's push count (one `Relaxed` store; the lane has one producer), and each reap adds the
//! chain length to the lane's taken count, so `pushed - taken` is the lane's backlog.
//! [`try_reap`](GkTreiberMpmc::try_reap) tries the lane with the largest backlog first, then every lane, from a
//! starting lane that rotates per call so ties spread. The counts are only a hint: `None` always comes from a look at
//! every lane.
//!
//! # Waking workers
//!
//! The queue never parks a worker; the pool decides who sleeps and who wakes. [`GkMpmcLane::push`] publishes the job
//! with a `SeqCst` compare-and-swap before it returns. When the producer follows with a `SeqCst` read-modify-write on
//! the pool's state word, and a worker about to park does its own `SeqCst` read-modify-write on that word before
//! calling [`has_work`](GkTreiberMpmc::has_work) (one load per lane), either the producer sees the worker going to
//! sleep or the worker's `has_work` sees the job.
//!
//! # Lanes are never freed
//!
//! The lane table is allocated with the queue and freed with it, so a worker can scan it while producers come and go.
//! Dropping a [`GkMpmcLane`] releases the lane at once, even if it still holds jobs: workers still reap them, and the
//! next producer to take the lane queues after them. When the last handle to the queue is dropped, jobs still queued
//! are freed with [`TreiberNode::drop_node`].

// ==== Import Definition ====

use super::{
    GkTreiberChannelError, alloc_value,
    channel_memory::{GkChannelMemory, GkHeapMemory},
    free_value,
};
use crate::treiber_stack::{TreiberNode, TreiberStackIterator};
use crossbeam_utils::CachePadded;

use std::{
    alloc::Layout,
    cell::Cell,
    fmt,
    marker::PhantomData,
    ops::Deref,
    ptr::{self, NonNull},
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence},
};

// ==== Constants ====

/// Default for [`GkTreiberMpmcConfig::max_lanes`].
pub const GK_MPMC_MAX_LANES: usize = 32;

// ==== Config Definition ====

/// Settings of one queue, fixed when it is created. Every field defaults to the matching `GK_MPMC_*` constant.
///
/// ```
/// use adaptif_gatotkaca_utils::GkTreiberMpmcConfig;
///
/// let config = GkTreiberMpmcConfig::DEFAULT.with_max_lanes(8);
/// assert!(config.validate().is_ok());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GkTreiberMpmcConfig {
    /// Most lanes held at once (one per producing runtime). All of them are allocated with the queue, one cache line
    /// each. Must be at least 1.
    pub max_lanes: usize,
}

impl GkTreiberMpmcConfig {
    /// Every field at its `GK_MPMC_*` default.
    pub const DEFAULT: Self = Self {
        max_lanes: GK_MPMC_MAX_LANES,
    };

    /// Sets [`max_lanes`](Self::max_lanes).
    pub const fn with_max_lanes(mut self, max_lanes: usize) -> Self {
        self.max_lanes = max_lanes;
        self
    }

    /// Checks the rules listed on the fields.
    pub const fn validate(&self) -> Result<(), GkTreiberChannelError> {
        // BRANCH: a queue without a lane could never hold a job
        if self.max_lanes == 0 {
            return Err(GkTreiberChannelError::INVALID_CONFIG);
        }
        Ok(())
    }
}

impl Default for GkTreiberMpmcConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// ==== Lane Definition ====

/// One producer's queue, one cache line. Lives in the lane table for the queue's whole life.
struct GkLane<N> {
    /// Jobs pushed and not yet reaped, newest first.
    pending: AtomicPtr<N>,
    /// Jobs ever pushed to this lane, by all its owners. Only the owner writes it.
    pushed: AtomicU64,
    /// Jobs ever reaped from this lane. Workers add to it right after their swap, while they hold the line anyway.
    taken: AtomicU64,
    /// A [`GkMpmcLane`] holds the lane.
    owned: AtomicBool,
}

impl<N: TreiberNode> GkLane<N> {
    /// Takes every job in the lane, oldest first, or `None` if it is empty.
    #[inline]
    fn reap(&self) -> Option<TreiberStackIterator<N>> {
        // BRANCH: an empty lane is only read, so the producer keeps its cache line
        if self.pending.load(Ordering::Relaxed).is_null() {
            return None;
        }
        // Acquire pairs with the pushes' SeqCst CAS: every job in the chain, and its link, is visible.
        let head = self.pending.swap(ptr::null_mut(), Ordering::Acquire);
        // BRANCH: another worker reaped it between the load and the swap
        if head.is_null() {
            return None;
        }
        let (first, _, count) = N::reverse_in_place(head);
        // Relaxed: a ranking hint.
        self.taken.fetch_add(count as u64, Ordering::Relaxed);
        // Safety: the swap gave this thread the whole chain; its jobs are valid until taken.
        Some(unsafe { TreiberStackIterator::from_head(first) })
    }

    /// Whether the lane holds a job. One load.
    #[inline(always)]
    fn has_work(&self) -> bool {
        !self.pending.load(Ordering::Acquire).is_null()
    }

    /// Jobs pushed and not yet reaped, as far as the counts tell. A hint only.
    #[inline(always)]
    fn backlog_hint(&self) -> u64 {
        let pushed = self.pushed.load(Ordering::Relaxed);
        // Saturating: the two counts are read without ordering and may briefly cross.
        pushed.saturating_sub(self.taken.load(Ordering::Relaxed))
    }

    /// Reaps the lane and hands each job to `f`, oldest first. Returns how many there were.
    fn drain(&self, f: &mut impl FnMut(NonNull<N>)) -> usize {
        let Some(chain) = self.reap() else {
            return 0;
        };
        let mut count = 0;
        for job in chain {
            // Safety: chain jobs are non-null.
            f(unsafe { NonNull::new_unchecked(job) });
            count += 1;
        }
        count
    }
}

// ==== Shared State Definition ====

/// The lane table and its bookkeeping. Allocated in the queue's memory and counted by [`GkMpmcRef`].
struct GkMpmcShared<N: TreiberNode, M: GkChannelMemory> {
    /// `config.max_lanes` lanes.
    lanes: *mut CachePadded<GkLane<N>>,
    /// One past the highest lane ever taken; scans stop there.
    used: AtomicUsize,
    config: GkTreiberMpmcConfig,
    memory: M,
    /// Handles to this state: queue handles and lanes.
    refs: CachePadded<AtomicUsize>,
}

impl<N: TreiberNode, M: GkChannelMemory> GkMpmcShared<N, M> {
    #[inline(always)]
    fn lane(&self, index: usize) -> &GkLane<N> {
        debug_assert!(index < self.config.max_lanes);
        // Safety: the table holds `max_lanes` initialised lanes for the shared state's life.
        unsafe { &*self.lanes.add(index) }
    }
}

/// Layout of the lane table.
fn table_layout<N>(config: &GkTreiberMpmcConfig) -> Option<Layout> {
    Layout::array::<CachePadded<GkLane<N>>>(config.max_lanes).ok()
}

impl<N: TreiberNode, M: GkChannelMemory> Drop for GkMpmcShared<N, M> {
    fn drop(&mut self) {
        // Nothing else can reach the lanes: dropping each reaped chain frees its jobs with `drop_node`.
        for index in 0..*self.used.get_mut() {
            drop(self.lane(index).reap());
        }
        let layout = table_layout::<N>(&self.config).expect("the layout was valid when allocated");
        // Safety: the table came from this memory with this layout and nothing uses it any more. Lanes hold atomics
        // only, so there is nothing to drop.
        unsafe {
            self.memory
                .deallocate(NonNull::new_unchecked(self.lanes.cast()), layout)
        };
    }
}

/// A counted handle to the shared state, like `Arc` but allocated in, and freed to, the queue's memory.
struct GkMpmcRef<N: TreiberNode, M: GkChannelMemory> {
    ptr: NonNull<GkMpmcShared<N, M>>,
}

impl<N: TreiberNode, M: GkChannelMemory> Clone for GkMpmcRef<N, M> {
    #[inline]
    fn clone(&self) -> Self {
        // Relaxed: a new handle is made from a live one, which keeps the state alive.
        self.refs.fetch_add(1, Ordering::Relaxed);
        Self { ptr: self.ptr }
    }
}

impl<N: TreiberNode, M: GkChannelMemory> Deref for GkMpmcRef<N, M> {
    type Target = GkMpmcShared<N, M>;

    #[inline(always)]
    fn deref(&self) -> &GkMpmcShared<N, M> {
        // Safety: the state lives while a handle exists.
        unsafe { self.ptr.as_ref() }
    }
}

impl<N: TreiberNode, M: GkChannelMemory> Drop for GkMpmcRef<N, M> {
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

/// A starting lane for this call: a per-thread turn, offset by the thread so threads do not start in step.
#[inline]
fn next_start() -> usize {
    thread_local! {
        static TURN: Cell<usize> = const { Cell::new(0) };
    }
    TURN.with(|turn| {
        let value = turn.get().wrapping_add(1);
        turn.set(value);
        value.wrapping_add(ptr::from_ref(turn).addr() >> 6)
    })
}

// ==== Queue Definition ====

/// A many-producer, many-consumer job queue for a thread pool. See the [module docs](self).
///
/// - `N`: the job type. It links through its own `next` field ([`TreiberNode`]); jobs never run are freed with
///   [`TreiberNode::drop_node`].
/// - `M`: where the lane table is allocated ([`GkHeapMemory`] by default). Jobs are never allocated by the queue.
///
/// Cloning makes another handle to the same queue. Workers need only a shared reference.
///
/// ```
/// use adaptif_gatotkaca_utils::{GkTreiberMpmc, TreiberNode};
/// use std::{ptr::NonNull, sync::atomic::AtomicPtr};
///
/// struct Job {
///     next: AtomicPtr<Job>,
///     id: u64,
/// }
///
/// impl TreiberNode for Job {
///     type Value = u64;
///     fn next(&self) -> &AtomicPtr<Self> {
///         &self.next
///     }
///     fn value(&self) -> &u64 {
///         &self.id
///     }
/// }
///
/// let queue = GkTreiberMpmc::<Job>::new();
/// let mut lane = queue.lane().unwrap(); // one per producing runtime
/// for id in 0..3 {
///     let job = Box::into_raw(Box::new(Job { next: AtomicPtr::default(), id }));
///     // Safety: a fresh job, in no other queue; `drop_node` frees it with `Box` if it is never run.
///     unsafe { lane.push(NonNull::new(job).unwrap()) };
/// }
/// // Any worker thread, no registration: one reap takes the whole lane, oldest first.
/// let mut ids = Vec::new();
/// while let Some(chain) = queue.try_reap() {
///     for job in chain {
///         // Safety: the job is ours now.
///         let job = unsafe { Box::from_raw(job) };
///         ids.push(job.id);
///     }
/// }
/// assert_eq!(ids, [0, 1, 2]);
/// assert!(!queue.has_work());
/// ```
pub struct GkTreiberMpmc<N: TreiberNode, M: GkChannelMemory = GkHeapMemory> {
    shared: GkMpmcRef<N, M>,
}

// Safety: the queue moves jobs between threads (hence `N: Send`); its own state is atomics and a fixed pointer, and `M`
// is `Send + Sync`.
unsafe impl<N: TreiberNode + Send, M: GkChannelMemory> Send for GkTreiberMpmc<N, M> {}
unsafe impl<N: TreiberNode + Send, M: GkChannelMemory> Sync for GkTreiberMpmc<N, M> {}

impl<N: TreiberNode, M: GkChannelMemory> Clone for GkTreiberMpmc<N, M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<N: TreiberNode, M: GkChannelMemory> fmt::Debug for GkTreiberMpmc<N, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkTreiberMpmc")
            .field("config", &self.shared.config)
            .field("lanes_used", &self.shared.used.load(Ordering::Relaxed))
            .finish()
    }
}

impl<N: TreiberNode, M: GkChannelMemory + Default> Default for GkTreiberMpmc<N, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N: TreiberNode, M: GkChannelMemory + Default> GkTreiberMpmc<N, M> {
    /// Creates a queue with the default configuration.
    pub fn new() -> Self {
        Self::with_memory(GkTreiberMpmcConfig::DEFAULT, M::default())
            .expect("default memory allocates")
    }

    /// Creates a queue with `config`. Fails with `INVALID_CONFIG` (allocating nothing) if
    /// [`GkTreiberMpmcConfig::validate`] rejects it.
    pub fn with_config(config: GkTreiberMpmcConfig) -> Result<Self, GkTreiberChannelError> {
        Self::with_memory(config, M::default())
    }
}

impl<N: TreiberNode, M: GkChannelMemory> GkTreiberMpmc<N, M> {
    /// Creates a queue whose lane table comes from `memory`. Fails with `INVALID_CONFIG` if `config` is rejected and
    /// with `OUT_OF_MEMORY` if `memory` has no room; neither error leaves anything allocated.
    pub fn with_memory(
        config: GkTreiberMpmcConfig,
        memory: M,
    ) -> Result<Self, GkTreiberChannelError> {
        config.validate()?;
        let layout = table_layout::<N>(&config).ok_or(GkTreiberChannelError::INVALID_CONFIG)?;
        let lanes = memory
            .allocate(layout)
            .ok_or(GkTreiberChannelError::OUT_OF_MEMORY)?
            .cast::<CachePadded<GkLane<N>>>()
            .as_ptr();
        // Safety: fresh memory for `max_lanes` lanes.
        unsafe {
            for lane in 0..config.max_lanes {
                lanes.add(lane).write(CachePadded::new(GkLane {
                    pending: AtomicPtr::new(ptr::null_mut()),
                    pushed: AtomicU64::new(0),
                    taken: AtomicU64::new(0),
                    owned: AtomicBool::new(false),
                }));
            }
        }
        let shared = alloc_value(
            &memory,
            GkMpmcShared {
                lanes,
                used: AtomicUsize::new(0),
                config,
                memory: memory.clone(),
                refs: CachePadded::new(AtomicUsize::new(1)),
            },
        );
        let Some(shared) = shared else {
            // `alloc_value` dropped the state it could not place, and with it the table (no jobs are queued yet).
            return Err(GkTreiberChannelError::OUT_OF_MEMORY);
        };
        Ok(Self {
            // Safety: `alloc_value` never returns null.
            shared: GkMpmcRef {
                ptr: unsafe { NonNull::new_unchecked(shared) },
            },
        })
    }

    /// The configuration the queue was created with.
    pub fn config(&self) -> GkTreiberMpmcConfig {
        self.shared.config
    }

    /// The memory the lane table was allocated from.
    pub fn memory(&self) -> &M {
        &self.shared.memory
    }

    /// Takes a free lane for a producer. Lanes released by dropped producers are reused, lowest first. Fails with
    /// `TOO_MANY_LANES` when all [`max_lanes`](GkTreiberMpmcConfig::max_lanes) are held. Never allocates.
    pub fn lane(&self) -> Result<GkMpmcLane<N, M>, GkTreiberChannelError> {
        let shared = &*self.shared;
        for index in 0..shared.config.max_lanes {
            let lane = shared.lane(index);
            // BRANCH: skip held lanes with a load; claim a free one with a CAS
            if lane.owned.load(Ordering::Relaxed)
                || lane
                    .owned
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
            {
                continue;
            }
            // Before the first push, so a scan that sees the push sees the lane.
            shared.used.fetch_max(index + 1, Ordering::Release);
            // The Acquire claim pairs with the previous owner's Release: its last count is visible.
            let pushed = lane.pushed.load(Ordering::Relaxed);
            return Ok(GkMpmcLane {
                lane,
                index,
                pushed,
                _shared: self.shared.clone(),
                _not_sync: PhantomData,
            });
        }
        Err(GkTreiberChannelError::TOO_MANY_LANES)
    }

    /// Takes every job of one lane, oldest first: the lane with the largest backlog, else the first non-empty lane
    /// from a rotating start. `None` only if every lane was empty. The caller runs the chain; jobs it drops unrun are
    /// freed with [`TreiberNode::drop_node`]. Never waits and never allocates.
    pub fn try_reap(&self) -> Option<TreiberStackIterator<N>> {
        let shared = &*self.shared;
        // Acquire pairs with `lane`'s Release: lanes below `used` are visible.
        let used = shared.used.load(Ordering::Acquire);
        // BRANCH: no lane, or one lane, needs no ranking
        match used {
            0 => return None,
            1 => return shared.lane(0).reap(),
            _ => {}
        }
        let start = next_start() % used;
        // Rank: the lane with the largest backlog, by the counts.
        let (mut best, mut best_backlog) = (usize::MAX, 0);
        let mut index = start;
        for _ in 0..used {
            let backlog = shared.lane(index).backlog_hint();
            // BRANCH: strictly larger, so ties go to the lane met first from the rotating start
            if backlog > best_backlog {
                best = index;
                best_backlog = backlog;
            }
            index += 1;
            // BRANCH: wrap around
            if index == used {
                index = 0;
            }
        }
        // BRANCH: the counts point at a lane
        if best != usize::MAX {
            // BRANCH: it still has jobs
            if let Some(chain) = shared.lane(best).reap() {
                return Some(chain);
            }
        }
        // The counts are hints: look at every lane before saying `None`.
        let mut index = start;
        for _ in 0..used {
            // BRANCH: the ranked lane was just tried
            if index != best {
                // BRANCH: the first lane with jobs
                if let Some(chain) = shared.lane(index).reap() {
                    return Some(chain);
                }
            }
            index += 1;
            if index == used {
                index = 0;
            }
        }
        None
    }

    /// Whether any lane holds a job: one load per lane in use, nothing taken. Use it for the last re-check before
    /// parking a worker (see the [module docs](self)) and for draining at shutdown.
    pub fn has_work(&self) -> bool {
        let shared = &*self.shared;
        let used = shared.used.load(Ordering::Acquire);
        (0..used).any(|index| shared.lane(index).has_work())
    }

    /// Reaps every lane, handing each job to `f` (oldest first within a lane), and returns how many there were. Jobs
    /// already reaped by workers are theirs.
    pub fn drain(&self, mut f: impl FnMut(NonNull<N>)) -> usize {
        let shared = &*self.shared;
        let used = shared.used.load(Ordering::Acquire);
        (0..used)
            .map(|index| shared.lane(index).drain(&mut f))
            .sum()
    }
}

// ==== Lane Handle Definition ====

/// A producer's lane of a [`GkTreiberMpmc`]. Dropping it releases the lane; jobs still in it stay queued.
///
/// Meant for one thread at a time; it can be moved to another thread.
pub struct GkMpmcLane<N: TreiberNode, M: GkChannelMemory = GkHeapMemory> {
    lane: *const GkLane<N>,
    index: usize,
    /// This lane's push count; mirrored into the shared `pushed` on every push.
    pushed: u64,
    /// Keeps the lane table alive.
    _shared: GkMpmcRef<N, M>,
    _not_sync: PhantomData<Cell<()>>,
}

// Safety: the lane hands jobs to other threads (hence `N: Send`); `Cell` in the marker keeps it !Sync, so one thread
// pushes at a time.
unsafe impl<N: TreiberNode + Send, M: GkChannelMemory> Send for GkMpmcLane<N, M> {}

impl<N: TreiberNode, M: GkChannelMemory> GkMpmcLane<N, M> {
    #[inline(always)]
    fn lane(&self) -> &GkLane<N> {
        // Safety: the lane table lives as long as the shared state, which this handle keeps alive.
        unsafe { &*self.lane }
    }

    /// Queues `job`. Never fails, waits or allocates. The job is published by a `SeqCst` compare-and-swap before this
    /// returns (see the [module docs](self) for the wake-up handshake).
    ///
    /// # Safety
    /// `job` must be valid, in no other queue or stack, and must not be used by the caller until a worker or a drain
    /// takes it back. If it is never run, it is freed with [`TreiberNode::drop_node`].
    #[inline]
    pub unsafe fn push(&mut self, job: NonNull<N>) {
        self.pushed += 1;
        let lane = self.lane();
        // Relaxed: a ranking hint; this handle is the only writer.
        lane.pushed.store(self.pushed, Ordering::Relaxed);
        let job = job.as_ptr();
        let mut head = lane.pending.load(Ordering::Relaxed);
        loop {
            // Safety: the job is ours until the CAS publishes it.
            unsafe { (*job).next().store(head, Ordering::Relaxed) };
            // SeqCst: publishes the job and orders it before the pool's SeqCst state update.
            match lane
                .pending
                .compare_exchange_weak(head, job, Ordering::SeqCst, Ordering::Relaxed)
            {
                // BRANCH: published
                Ok(_) => return,
                // BRANCH: a worker reaped the lane meanwhile (or a spurious failure); retry on the new head
                Err(current) => head = current,
            }
        }
    }

    /// Takes back every job still in this lane (not yet reaped by a worker), handing each to `f` in push order, and
    /// returns how many there were. For a producer that sees the pool shut down after pushing.
    pub fn drain(&mut self, mut f: impl FnMut(NonNull<N>)) -> usize {
        self.lane().drain(&mut f)
    }

    /// Position of this lane in the queue's lane table.
    pub fn index(&self) -> usize {
        self.index
    }
}

impl<N: TreiberNode, M: GkChannelMemory> fmt::Debug for GkMpmcLane<N, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkMpmcLane")
            .field("index", &self.index)
            .field("pushed", &self.pushed)
            .finish()
    }
}

impl<N: TreiberNode, M: GkChannelMemory> Drop for GkMpmcLane<N, M> {
    fn drop(&mut self) {
        // Release: the next owner sees our last push count.
        self.lane().owned.store(false, Ordering::Release);
    }
}

//! Many-producer, many-consumer job queue for a thread pool: one lane per producing runtime, any number of workers
//! each reaping a whole lane at once, and taking over the rest of a chain whose worker is blocked.
//!
//! - [`GkTreiberMpmc`]: the queue. Workers call [`try_reap`](GkTreiberMpmc::try_reap) on it directly; they do not
//!   register and hold no state of their own.
//! - [`GkMpmcLane`]: a producer's lane, taken with [`GkTreiberMpmc::lane`]. Its owner
//!   [`push`](GkMpmcLane::push)es jobs and can [`drain`](GkMpmcLane::drain) them back.
//! - [`GkMpmcChain`]: what a worker gets from `try_reap`, an iterator over the jobs it runs.
//!
//! Jobs are intrusive: the job type implements [`TreiberNode`] and links through its own `next` field. The queue moves
//! pointers only, so it never allocates per job and has no nodes to send home.
//!
//! # A lane
//!
//! One cache line: a Treiber stack of pushed jobs and two counters. The producer pushes with one `SeqCst`
//! compare-and-swap. A worker takes the whole stack with one swap (an empty lane costs only a load) and reverses it,
//! so the chain runs oldest first.
//!
//! # Chain slots
//!
//! A worker that blocks in the middle of a chain (in `fsync`, a DNS lookup) must not hold the rest of it. So after a
//! reap the worker keeps the first job and publishes the rest in a free chain slot
//! ([`GkTreiberMpmcConfig::max_chains`]), then takes its jobs from that slot one at a time. A take is a
//! compare-and-swap that puts a `HELD` marker in the slot, a read of the first job's link and a store of the rest:
//! a few instructions on a cache line nobody else touches while its owner is busy. A worker never reads a job before
//! its compare-and-swap made the chain its own, so there is no ABA problem: a slot that holds the same address again
//! holds a chain starting there, whoever published it.
//!
//! # Rescuing a stalled chain
//!
//! A worker with nothing else to do takes over a shared chain only once it has stalled: when the chain's first job has
//! not changed for [`GkTreiberMpmcConfig::rescue_after`] (50 µs by default). Every take changes the first job, so the
//! owner pays nothing for this; idle workers remember, per thread, what they last saw in each slot (a small table, so
//! still no registration) and look at the slots at most every `rescue_after / 2`, so they leave the owner's cache line
//! alone while it works. A rescue takes the chain over: it gives the chain a new generation, so the old owner stops at
//! its next take (and goes back to `try_reap` once its blocking job returns) while the rescuer carries on alone, with no
//! two workers fighting over one slot. If the rescuer blocks in turn, another idle worker rescues it the same way. A
//! chain whose worker dropped it (exited midway) has no owner and is rescued the same way.
//!
//! `try_reap` reaps a lane first and looks at the chain slots when every lane is empty. Every 16th call per thread it
//! looks at the slots first, so a stalled chain waits at most a few calls even when the lanes never run dry. When every
//! slot is taken, the reaping worker keeps its chain to itself (the behaviour of `max_chains = 0`).
//!
//! Order is FIFO within a chain. Chains reaped by different workers run side by side, so a later chain can start
//! before an earlier one finishes. There is no order between lanes.
//!
//! # Picking a lane
//!
//! Each push stores the lane's push count (one `Relaxed` store; the lane has one producer), and each reap adds the
//! chain length to the lane's taken count, so `pushed - taken` is the lane's backlog. `try_reap` tries the lane with
//! the largest backlog first, then every lane, from a starting lane that rotates per call so ties spread. The counts
//! are only a hint: `None` always comes from a look at every lane and slot.
//!
//! # Waking workers
//!
//! The queue never parks a worker; the pool decides who sleeps and who wakes. [`GkMpmcLane::push`] publishes the job
//! with a `SeqCst` compare-and-swap before it returns. When the producer follows with a `SeqCst` read-modify-write on
//! the pool's state word, and a worker about to park does its own `SeqCst` read-modify-write on that word before
//! calling [`has_work`](GkTreiberMpmc::has_work), either the producer sees the worker going to sleep or the worker's
//! `has_work` sees the job.
//!
//! `has_work` looks at the lanes only: a shared chain has an owner working on it, so idle workers may park. But nothing
//! signals when that owner blocks, so while [`shared_chains`](GkTreiberMpmc::shared_chains) is above 0 a worker must
//! park with a timeout of about `rescue_after` (or keep one worker searching), or a stalled chain waits for its owner.
//!
//! # Lanes are never freed
//!
//! The lane table and the chain slots are allocated with the queue and freed with it, so a worker can scan them while
//! producers come and go. Dropping a [`GkMpmcLane`] releases the lane at once, even if it still holds jobs: workers
//! still reap them, and the next producer to take the lane queues after them. When the last handle to the queue is
//! dropped, jobs still queued are freed with [`TreiberNode::drop_node`].

// ==== Import Definition ====

use super::{
    GkTreiberChannelError, alloc_value,
    channel_memory::{GkChannelMemory, GkHeapMemory},
    free_value,
};
use crate::treiber_stack::TreiberNode;
use crossbeam_utils::{Backoff, CachePadded};

use std::{
    alloc::Layout,
    cell::{Cell, RefCell},
    fmt,
    marker::PhantomData,
    ops::Deref,
    ptr::{self, NonNull},
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence},
    time::{Duration, Instant},
};

// ==== Constants ====

/// Default for [`GkTreiberMpmcConfig::max_lanes`].
pub const GK_MPMC_MAX_LANES: usize = 32;
/// Default for [`GkTreiberMpmcConfig::max_chains`].
pub const GK_MPMC_MAX_CHAINS: usize = 64;
/// Default for [`GkTreiberMpmcConfig::rescue_after`].
pub const GK_MPMC_RESCUE_AFTER: Duration = Duration::from_micros(50);

/// Every this many `try_reap` calls per thread, the chain slots are looked at before the lanes.
const FAIR_TICK: usize = 16;
/// `GkMpmcChain::slot` when the chain takes from no slot.
const NO_SLOT: usize = usize::MAX;

/// The marker a slot holds while a worker takes a job from it. Never a job: jobs hold an `AtomicPtr`, so they are
/// aligned to at least 2.
#[inline(always)]
fn held<N>() -> *mut N {
    ptr::without_provenance_mut(1)
}

// ==== Config Definition ====

/// Settings of one queue, fixed when it is created. Every field defaults to the matching `GK_MPMC_*` constant.
///
/// ```
/// use adaptif_gatotkaca_utils::GkTreiberMpmcConfig;
///
/// let config = GkTreiberMpmcConfig::DEFAULT.with_max_lanes(8).with_max_chains(128);
/// assert!(config.validate().is_ok());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GkTreiberMpmcConfig {
    /// Most lanes held at once (one per producing runtime). All of them are allocated with the queue, one cache line
    /// each. Must be at least 1.
    pub max_lanes: usize,
    /// Chain slots: how many reaped chains can be shared with other workers at once, one cache line each. Set it to at
    /// least the number of workers; a worker that finds every slot taken keeps its chain to itself, so a block in it
    /// holds the rest. 0 never shares a chain.
    pub max_chains: usize,
    /// How long a shared chain's first job must stay the same before an idle worker takes over: longer than your
    /// typical non-blocking job, shorter than the delay you accept for the jobs behind a blocked one. A rescue takes the
    /// chain over from its owner. Zero instead lets idle workers share every chain at once, which costs throughput on
    /// short jobs.
    pub rescue_after: Duration,
}

impl GkTreiberMpmcConfig {
    /// Every field at its `GK_MPMC_*` default.
    pub const DEFAULT: Self = Self {
        max_lanes: GK_MPMC_MAX_LANES,
        max_chains: GK_MPMC_MAX_CHAINS,
        rescue_after: GK_MPMC_RESCUE_AFTER,
    };

    /// Sets [`max_lanes`](Self::max_lanes).
    pub const fn with_max_lanes(mut self, max_lanes: usize) -> Self {
        self.max_lanes = max_lanes;
        self
    }

    /// Sets [`max_chains`](Self::max_chains).
    pub const fn with_max_chains(mut self, max_chains: usize) -> Self {
        self.max_chains = max_chains;
        self
    }

    /// Sets [`rescue_after`](Self::rescue_after).
    pub const fn with_rescue_after(mut self, rescue_after: Duration) -> Self {
        self.rescue_after = rescue_after;
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
    /// Takes every job in the lane: the head of the chain, oldest first, or null if the lane is empty.
    #[inline]
    fn reap(&self) -> *mut N {
        // BRANCH: an empty lane is only read, so the producer keeps its cache line
        if self.pending.load(Ordering::Relaxed).is_null() {
            return ptr::null_mut();
        }
        // Acquire pairs with the pushes' SeqCst CAS: every job in the chain, and its link, is visible.
        let head = self.pending.swap(ptr::null_mut(), Ordering::Acquire);
        // BRANCH: another worker reaped it between the load and the swap
        if head.is_null() {
            return ptr::null_mut();
        }
        let (first, _, count) = N::reverse_in_place(head);
        // Relaxed: a ranking hint.
        self.taken.fetch_add(count as u64, Ordering::Relaxed);
        first
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
}

/// One chain slot, one cache line.
struct GkChainSlot<N> {
    /// Null (free), `held()` (a worker is taking a job, or publishing) or the head of a shared chain.
    head: AtomicPtr<N>,
    /// Bumped by every publish. A chain remembers it, so it stops at the end of its own jobs instead of going on with a
    /// chain published later in the same slot (which can be older: reaped earlier, published later).
    generation: AtomicU64,
}

/// Hands every job of a private chain to `f`, in chain order, and returns how many there were.
///
/// # Safety
/// The chain must be null-terminated, valid and owned by the caller.
unsafe fn for_each_job<N: TreiberNode>(mut job: *mut N, f: &mut impl FnMut(NonNull<N>)) -> usize {
    let mut count = 0;
    while !job.is_null() {
        // Safety: forwarded from this function's contract; the link is read before the job is handed out.
        unsafe {
            let next = (*job).next().load(Ordering::Relaxed);
            (*job).next().store(ptr::null_mut(), Ordering::Relaxed);
            f(NonNull::new_unchecked(job));
            job = next;
        }
        count += 1;
    }
    count
}

// ==== Shared State Definition ====

/// The lane table, the chain slots and their bookkeeping. Allocated in the queue's memory and counted by
/// [`GkMpmcRef`].
struct GkMpmcShared<N: TreiberNode, M: GkChannelMemory> {
    /// `config.max_lanes` lanes.
    lanes: *mut CachePadded<GkLane<N>>,
    /// `config.max_chains` slots.
    chains: *mut CachePadded<GkChainSlot<N>>,
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

    #[inline(always)]
    fn slot(&self, index: usize) -> &GkChainSlot<N> {
        debug_assert!(index < self.config.max_chains);
        // Safety: the table holds `max_chains` initialised slots for the shared state's life.
        unsafe { &*self.chains.add(index) }
    }

    /// Reaps one lane: the one with the largest backlog, else the first non-empty one from `start`. Returns the chain's
    /// head (null if every lane is empty) and the lane.
    fn reap_lanes(&self, start: usize) -> (*mut N, usize) {
        // Acquire pairs with `lane`'s Release: lanes below `used` are visible.
        let used = self.used.load(Ordering::Acquire);
        // BRANCH: no lane, or one lane, needs no ranking
        match used {
            0 => return (ptr::null_mut(), 0),
            1 => return (self.lane(0).reap(), 0),
            _ => {}
        }
        let start = start % used;
        // Rank: the lane with the largest backlog, by the counts.
        let (mut best, mut best_backlog) = (usize::MAX, 0);
        let mut index = start;
        for _ in 0..used {
            let backlog = self.lane(index).backlog_hint();
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
            let head = self.lane(best).reap();
            // BRANCH: it still has jobs
            if !head.is_null() {
                return (head, best);
            }
        }
        // The counts are hints: look at every lane before giving up.
        let mut index = start;
        for _ in 0..used {
            // BRANCH: the ranked lane was just tried
            if index != best {
                let head = self.lane(index).reap();
                // BRANCH: the first lane with jobs
                if !head.is_null() {
                    return (head, index);
                }
            }
            index += 1;
            if index == used {
                index = 0;
            }
        }
        (ptr::null_mut(), 0)
    }

    /// Puts a private chain (oldest first) back on `lane`, for when no chain slot is free. It goes on top of the lane's
    /// stack, reversed, so a later reap hands it out in order, after the jobs pushed meanwhile. Published with a
    /// `SeqCst` CAS, like a push.
    fn give_back(&self, chain: *mut N, lane: usize) {
        let lane = self.lane(lane);
        let (newest, oldest, count) = N::reverse_in_place(chain);
        // Relaxed: a ranking hint; these jobs are pending again.
        lane.taken.fetch_sub(count as u64, Ordering::Relaxed);
        let mut head = lane.pending.load(Ordering::Relaxed);
        loop {
            // Safety: the chain is ours until the CAS publishes it.
            unsafe { (*oldest).next().store(head, Ordering::Relaxed) };
            match lane.pending.compare_exchange_weak(
                head,
                newest,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                // BRANCH: published
                Ok(_) => return,
                // BRANCH: the producer pushed or a worker reaped meanwhile; retry on the new head
                Err(current) => head = current,
            }
        }
    }

    /// Publishes a private chain in a free slot, looking from `start`. Returns the slot and the chain's generation, or
    /// `None` (the chain stays the caller's) if every slot is taken.
    fn publish(&self, chain: *mut N, start: usize) -> Option<(usize, u64)> {
        let slots = self.config.max_chains;
        // BRANCH: sharing is off
        if slots == 0 {
            return None;
        }
        let mut index = start % slots;
        for _ in 0..slots {
            let slot = self.slot(index);
            // BRANCH: a free slot is ours if the CAS wins it; `held()` keeps others out while the generation changes
            if slot.head.load(Ordering::Relaxed).is_null()
                && slot
                    .head
                    .compare_exchange(
                        ptr::null_mut(),
                        held(),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                // Relaxed: only the holder of the slot writes it, and the Release below publishes it.
                let generation = slot.generation.load(Ordering::Relaxed).wrapping_add(1);
                slot.generation.store(generation, Ordering::Relaxed);
                // Release: the chain's links and the generation.
                slot.head.store(chain, Ordering::Release);
                return Some((index, generation));
            }
            index += 1;
            if index == slots {
                index = 0;
            }
        }
        // BRANCH: every slot is taken
        None
    }

    /// Takes the first job of the chain in slot `index`, with the chain's generation. `None` if the slot is free, holds
    /// a chain of another generation than `generation` (when given), or is held by another worker and `patient` is
    /// false (a patient caller waits for the holder's store).
    ///
    /// With `claim`, the chain gets a new generation: its previous taker stops at its next take, and the caller's chain
    /// carries on alone (a rescue takes a stalled chain over instead of sharing it).
    fn take_from_slot(
        &self,
        index: usize,
        patient: bool,
        generation: Option<u64>,
        claim: bool,
    ) -> Option<(NonNull<N>, u64)> {
        let slot = self.slot(index);
        let backoff = Backoff::new();
        loop {
            let head = slot.head.load(Ordering::Relaxed);
            // BRANCH: nothing shared here
            if head.is_null() {
                return None;
            }
            // BRANCH: another worker is between its CAS and its store (a few instructions); wait for it if patient,
            // yielding in case it was descheduled there
            if head == held() {
                if !patient {
                    return None;
                }
                backoff.snooze();
                continue;
            }
            // Acquire pairs with the Release that put `head` here (a publish or the previous taker's store): the
            // chain's links are visible. Only now is the chain ours; nothing was read through `head` before.
            // BRANCH: lost to another worker (or a spurious failure); look again
            if slot
                .head
                .compare_exchange_weak(head, held(), Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }
            // Relaxed: the publish (or a claim) wrote it before the Release we acquired, and nobody changes it while we
            // hold the slot.
            let mut current = slot.generation.load(Ordering::Relaxed);
            // BRANCH: a later chain in this slot, or this chain was taken over; put it back untouched
            if let Some(generation) = generation {
                if current != generation {
                    slot.head.store(head, Ordering::Release);
                    return None;
                }
            }
            // BRANCH: a rescue makes the chain its own; the Release store below publishes the new generation
            if claim {
                current = current.wrapping_add(1);
                slot.generation.store(current, Ordering::Relaxed);
            }
            // Safety: the CAS made the whole chain ours until the store below.
            let next = unsafe {
                let next = (*head).next().load(Ordering::Relaxed);
                (*head).next().store(ptr::null_mut(), Ordering::Relaxed);
                next
            };
            // BRANCH: that was the last job; free the slot
            if next.is_null() {
                slot.head.store(ptr::null_mut(), Ordering::Release);
            } else {
                // Release: the rest of the chain, for the next taker.
                slot.head.store(next, Ordering::Release);
            }
            // Safety: jobs are non-null.
            return Some((unsafe { NonNull::new_unchecked(head) }, current));
        }
    }

    /// Takes one job from a shared chain that has stalled (see the module docs), looking from `start`. Returns the
    /// job, its slot and the chain's generation.
    fn steal(&self, start: usize) -> Option<(NonNull<N>, usize, u64)> {
        let slots = self.config.max_chains;
        // BRANCH: sharing is off
        if slots == 0 {
            return None;
        }
        let delay = self.config.rescue_after;
        // BRANCH: no delay: take from the first chain found
        if delay.is_zero() {
            let mut index = start % slots;
            for _ in 0..slots {
                // BRANCH: the first slot with a job
                if let Some((job, generation)) = self.take_from_slot(index, false, None, false) {
                    return Some((job, index, generation));
                }
                index += 1;
                if index == slots {
                    index = 0;
                }
            }
            return None;
        }
        let now = Instant::now();
        RESCUE.with_borrow_mut(|rescue| {
            // BRANCH: looked at the slots less than `delay / 2` ago; leave the owners' cache lines alone
            if rescue.next_look.is_some_and(|next| now < next) {
                return None;
            }
            rescue.next_look = Some(now + delay / 2);
            let mut index = start % slots;
            for _ in 0..slots {
                let slot = self.slot(index);
                let head = slot.head.load(Ordering::Relaxed);
                // BRANCH: a shared chain (not free, not mid-take)
                if !head.is_null() && head != held() {
                    let generation = slot.generation.load(Ordering::Relaxed);
                    let seen = GkSeen {
                        slot: ptr::from_ref(slot).addr(),
                        generation,
                        head: head.addr(),
                        since: Some(now),
                    };
                    let entry = &mut rescue.seen[(seen.slot >> 7) % SEEN_ENTRIES];
                    // BRANCH: unchanged since this thread's earlier look, long enough ago: stalled
                    if entry.same_as(&seen) {
                        if entry
                            .since
                            .is_some_and(|since| now.duration_since(since) >= delay)
                        {
                            // BRANCH: still that chain (a take here moves its head, and the next look starts over)
                            if let Some((job, generation)) =
                                self.take_from_slot(index, false, Some(generation), true)
                            {
                                *entry = GkSeen::EMPTY;
                                return Some((job, index, generation));
                            }
                        }
                    } else {
                        *entry = seen;
                    }
                }
                index += 1;
                if index == slots {
                    index = 0;
                }
            }
            None
        })
    }

    /// Takes every job of slot `index`, waiting out other takers, and hands each to `f`.
    fn drain_slot(&self, index: usize, f: &mut impl FnMut(NonNull<N>)) -> usize {
        let backoff = Backoff::new();
        let mut count = 0;
        loop {
            match self.take_from_slot(index, true, None, false) {
                // BRANCH: one more job
                Some((job, _)) => {
                    f(job);
                    count += 1;
                }
                // BRANCH: still held by a taker; wait for its store
                None if self.slot(index).head.load(Ordering::Relaxed) == held() => backoff.snooze(),
                // BRANCH: free
                None => return count,
            }
        }
    }
}

/// Layouts of the lane table and of the chain slots (`None` for no slots).
fn table_layouts<N>(config: &GkTreiberMpmcConfig) -> Option<(Layout, Option<Layout>)> {
    let lanes = Layout::array::<CachePadded<GkLane<N>>>(config.max_lanes).ok()?;
    // BRANCH: no slots take no memory
    let chains = if config.max_chains == 0 {
        None
    } else {
        Some(Layout::array::<CachePadded<GkChainSlot<N>>>(config.max_chains).ok()?)
    };
    Some((lanes, chains))
}

impl<N: TreiberNode, M: GkChannelMemory> Drop for GkMpmcShared<N, M> {
    fn drop(&mut self) {
        // Nothing else can reach the lanes and slots: free the jobs still queued.
        let mut free = |job: NonNull<N>| unsafe { N::drop_node(job.as_ptr()) };
        for index in 0..*self.used.get_mut() {
            let head = self.lane(index).reap();
            // Safety: a reaped chain is ours.
            unsafe { for_each_job(head, &mut free) };
        }
        for index in 0..self.config.max_chains {
            self.drain_slot(index, &mut free);
        }
        let (lanes, chains) =
            table_layouts::<N>(&self.config).expect("the layouts were valid when allocated");
        // Safety: both blocks came from this memory with these layouts and nothing uses them any more. They hold
        // atomics only, so there is nothing to drop.
        unsafe {
            self.memory
                .deallocate(NonNull::new_unchecked(self.lanes.cast()), lanes);
            // BRANCH: only allocated when there are slots
            if let Some(chains) = chains {
                self.memory
                    .deallocate(NonNull::new_unchecked(self.chains.cast()), chains);
            }
        }
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

/// This thread's turn: counts calls (for the fairness tick) and, offset by the thread, gives a starting lane and slot
/// so threads do not start in step.
#[inline]
fn next_turn() -> (usize, usize) {
    thread_local! {
        static TURN: Cell<usize> = const { Cell::new(0) };
    }
    TURN.with(|turn| {
        let value = turn.get().wrapping_add(1);
        turn.set(value);
        (value, value.wrapping_add(ptr::from_ref(turn).addr() >> 6))
    })
}

/// What this thread last saw in one chain slot, to tell a stalled chain from a busy one.
#[derive(Clone, Copy)]
struct GkSeen {
    /// The slot's address (so the table serves every queue the thread uses).
    slot: usize,
    generation: u64,
    /// The chain's first job.
    head: usize,
    /// When this thread first saw that first job; `None` for an empty entry.
    since: Option<Instant>,
}

impl GkSeen {
    const EMPTY: Self = Self {
        slot: 0,
        generation: 0,
        head: 0,
        since: None,
    };

    /// Same slot, same chain, same first job: nobody took a job from it in between.
    #[inline(always)]
    fn same_as(&self, other: &Self) -> bool {
        self.since.is_some()
            && self.slot == other.slot
            && self.generation == other.generation
            && self.head == other.head
    }
}

/// Entries in a thread's table of seen slots (direct-mapped by slot address; a collision only delays a rescue).
const SEEN_ENTRIES: usize = 64;

/// One thread's view of the chain slots, for the stall check.
struct GkRescue {
    seen: [GkSeen; SEEN_ENTRIES],
    /// No look at the slots before this.
    next_look: Option<Instant>,
}

thread_local! {
    static RESCUE: RefCell<GkRescue> = const {
        RefCell::new(GkRescue {
            seen: [GkSeen::EMPTY; SEEN_ENTRIES],
            next_look: None,
        })
    };
}

// ==== Queue Definition ====

/// A many-producer, many-consumer job queue for a thread pool. See the [module docs](self).
///
/// - `N`: the job type. It links through its own `next` field ([`TreiberNode`]); jobs never run are freed with
///   [`TreiberNode::drop_node`].
/// - `M`: where the lane table and the chain slots are allocated ([`GkHeapMemory`] by default). Jobs are never
///   allocated by the queue.
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
///         let job = unsafe { Box::from_raw(job.as_ptr()) };
///         ids.push(job.id);
///     }
/// }
/// assert_eq!(ids, [0, 1, 2]);
/// assert!(!queue.has_work());
/// ```
pub struct GkTreiberMpmc<N: TreiberNode, M: GkChannelMemory = GkHeapMemory> {
    shared: GkMpmcRef<N, M>,
}

// Safety: the queue moves jobs between threads (hence `N: Send`); its own state is atomics and fixed pointers, and `M`
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
            .field("shared_chains", &self.shared_chains())
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
    /// Creates a queue whose lane table and chain slots come from `memory`. Fails with `INVALID_CONFIG` if `config` is
    /// rejected and with `OUT_OF_MEMORY` if `memory` has no room; neither error leaves anything allocated.
    pub fn with_memory(
        config: GkTreiberMpmcConfig,
        memory: M,
    ) -> Result<Self, GkTreiberChannelError> {
        const {
            assert!(
                align_of::<N>() >= 2,
                "a job must be aligned to at least 2 (it holds an AtomicPtr)"
            )
        };
        config.validate()?;
        let (lanes_layout, chains_layout) =
            table_layouts::<N>(&config).ok_or(GkTreiberChannelError::INVALID_CONFIG)?;
        let lanes = memory
            .allocate(lanes_layout)
            .ok_or(GkTreiberChannelError::OUT_OF_MEMORY)?
            .cast::<CachePadded<GkLane<N>>>()
            .as_ptr();
        // BRANCH: slots only when sharing is on
        let chains = match chains_layout {
            None => NonNull::dangling().as_ptr(),
            Some(layout) => {
                let Some(chains) = memory.allocate(layout) else {
                    // Safety: just allocated with this layout, holds nothing.
                    unsafe {
                        memory.deallocate(NonNull::new_unchecked(lanes.cast()), lanes_layout)
                    };
                    return Err(GkTreiberChannelError::OUT_OF_MEMORY);
                };
                chains.cast::<CachePadded<GkChainSlot<N>>>().as_ptr()
            }
        };
        // Safety: fresh memory for `max_lanes` lanes and `max_chains` slots.
        unsafe {
            for lane in 0..config.max_lanes {
                lanes.add(lane).write(CachePadded::new(GkLane {
                    pending: AtomicPtr::new(ptr::null_mut()),
                    pushed: AtomicU64::new(0),
                    taken: AtomicU64::new(0),
                    owned: AtomicBool::new(false),
                }));
            }
            for slot in 0..config.max_chains {
                chains.add(slot).write(CachePadded::new(GkChainSlot {
                    head: AtomicPtr::new(ptr::null_mut()),
                    generation: AtomicU64::new(0),
                }));
            }
        }
        let shared = alloc_value(
            &memory,
            GkMpmcShared {
                lanes,
                chains,
                used: AtomicUsize::new(0),
                config,
                memory: memory.clone(),
                refs: CachePadded::new(AtomicUsize::new(1)),
            },
        );
        let Some(shared) = shared else {
            // `alloc_value` dropped the state it could not place, and with it the tables (no jobs are queued yet).
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

    /// The memory the lane table and the chain slots were allocated from.
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

    /// Takes work: reaps one lane (the largest backlog first), keeps its first job and publishes the rest in a chain
    /// slot; or, when every lane is empty, takes over a shared chain that has stalled (see the [module docs](self))
    /// and keeps taking from it. Every 16th call per thread looks at stalled chains first. `None` if every lane was
    /// empty and no shared chain has stalled. Never waits and never allocates.
    ///
    /// The returned [`GkMpmcChain`] holds at least one job.
    pub fn try_reap(&self) -> Option<GkMpmcChain<'_, N, M>> {
        let shared = &*self.shared;
        let (turn, start) = next_turn();
        let fair = turn % FAIR_TICK == 0;
        // BRANCH: the fairness tick looks at jobs left by other (maybe blocked) workers first
        if fair {
            if let Some((job, slot, generation)) = shared.steal(start) {
                return Some(GkMpmcChain::stolen(shared, job, slot, generation));
            }
        }
        let (head, lane) = shared.reap_lanes(start);
        // BRANCH: a fresh chain; share all but its first job
        if !head.is_null() {
            // Safety: the reaped chain is ours.
            let rest = unsafe {
                let rest = (*head).next().load(Ordering::Relaxed);
                (*head).next().store(ptr::null_mut(), Ordering::Relaxed);
                rest
            };
            let shared_at = if rest.is_null() {
                None
            } else {
                shared.publish(rest, start)
            };
            let (slot, generation) = shared_at.unwrap_or((NO_SLOT, 0));
            return Some(GkMpmcChain {
                shared,
                next: head,
                private: if shared_at.is_none() {
                    rest
                } else {
                    ptr::null_mut()
                },
                slot,
                generation,
                lane,
                published: shared_at.is_some(),
            });
        }
        // BRANCH: every lane is empty; help with a shared chain (already tried on the fairness tick)
        if fair {
            return None;
        }
        let (job, slot, generation) = shared.steal(start)?;
        Some(GkMpmcChain::stolen(shared, job, slot, generation))
    }

    /// Whether any lane holds a job: one load per lane in use, nothing taken. Use it for the last re-check before
    /// parking a worker (see the [module docs](self)). Shared chains are not counted: their owners are on them; see
    /// [`shared_chains`](Self::shared_chains).
    pub fn has_work(&self) -> bool {
        let shared = &*self.shared;
        let used = shared.used.load(Ordering::Acquire);
        (0..used).any(|index| shared.lane(index).has_work())
    }

    /// How many chains are shared in chain slots: one load per slot, so call it when parking, not per job. While it
    /// is above 0, a worker that parks should wake within about [`rescue_after`](GkTreiberMpmcConfig::rescue_after) to
    /// look for a stalled chain; at shutdown, [`drain`](Self::drain) takes them.
    pub fn shared_chains(&self) -> usize {
        let shared = &*self.shared;
        (0..shared.config.max_chains)
            .filter(|&index| !shared.slot(index).head.load(Ordering::Acquire).is_null())
            .count()
    }

    /// Takes every job not yet taken by a worker, from every lane and every chain slot, handing each to `f` (oldest
    /// first within a lane or chain), and returns how many there were. For the pool's shutdown.
    pub fn drain(&self, mut f: impl FnMut(NonNull<N>)) -> usize {
        let shared = &*self.shared;
        let used = shared.used.load(Ordering::Acquire);
        let mut count = 0;
        for index in 0..used {
            let head = shared.lane(index).reap();
            // Safety: a reaped chain is ours.
            count += unsafe { for_each_job(head, &mut f) };
        }
        for index in 0..shared.config.max_chains {
            count += shared.drain_slot(index, &mut f);
        }
        count
    }
}

// ==== Chain Definition ====

/// Jobs for one worker, from [`GkTreiberMpmc::try_reap`]: the first job of a fresh reap and then the rest of it, or
/// jobs taken from a chain another worker shared. Iterate it and run each job; between jobs the rest stays in its chain
/// slot, where idle workers can take from it, so a job that blocks does not hold the others.
///
/// A chain yields the jobs of one reap only, oldest first: it stops when its jobs in the slot run out, even if the slot
/// was reused meanwhile. Dropping the chain never loses a job: the rest stays in its slot, and
/// jobs it holds to itself (every slot was taken) are published in a slot that came free, or put back on their lane.
pub struct GkMpmcChain<'a, N: TreiberNode, M: GkChannelMemory = GkHeapMemory> {
    shared: &'a GkMpmcShared<N, M>,
    /// A job already ours, handed out by the next call.
    next: *mut N,
    /// The rest of the reap when it could not be shared.
    private: *mut N,
    /// The slot this chain takes from, or `NO_SLOT`.
    slot: usize,
    /// The generation of this chain in `slot`.
    generation: u64,
    /// Where held jobs go back if no slot is free: the reaped lane (lane 0 for a stolen job, which can come from any).
    lane: usize,
    published: bool,
}

impl<'a, N: TreiberNode, M: GkChannelMemory> GkMpmcChain<'a, N, M> {
    fn stolen(
        shared: &'a GkMpmcShared<N, M>,
        job: NonNull<N>,
        slot: usize,
        generation: u64,
    ) -> Self {
        Self {
            shared,
            next: job.as_ptr(),
            private: ptr::null_mut(),
            slot,
            generation,
            lane: 0,
            published: false,
        }
    }

    /// True if this chain comes from a fresh reap whose rest was published in a chain slot. Other workers take those
    /// jobs only if this worker stalls (see [`GkTreiberMpmcConfig::rescue_after`]), so there is no need to wake one now;
    /// parked workers should time out instead (see [`GkTreiberMpmc::shared_chains`]).
    pub fn published(&self) -> bool {
        self.published
    }
}

impl<N: TreiberNode, M: GkChannelMemory> Iterator for GkMpmcChain<'_, N, M> {
    type Item = NonNull<N>;

    #[inline]
    fn next(&mut self) -> Option<NonNull<N>> {
        let job = self.next;
        // BRANCH: the job taken when the chain was made
        if !job.is_null() {
            self.next = ptr::null_mut();
            // Safety: jobs are non-null.
            return Some(unsafe { NonNull::new_unchecked(job) });
        }
        // BRANCH: take the next job from the shared chain; stop once its jobs run out or a rescuer took it over
        if self.slot != NO_SLOT {
            let job = self
                .shared
                .take_from_slot(self.slot, true, Some(self.generation), false);
            if job.is_none() {
                self.slot = NO_SLOT;
            }
            return job.map(|(job, _)| job);
        }
        let job = self.private;
        // BRANCH: the rest of a reap that every slot was too full to share
        if job.is_null() {
            return None;
        }
        // Safety: the private chain is ours.
        unsafe {
            self.private = (*job).next().load(Ordering::Relaxed);
            (*job).next().store(ptr::null_mut(), Ordering::Relaxed);
            Some(NonNull::new_unchecked(job))
        }
    }
}

impl<N: TreiberNode, M: GkChannelMemory> fmt::Debug for GkMpmcChain<'_, N, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkMpmcChain")
            .field("slot", &(self.slot != NO_SLOT).then_some(self.slot))
            .field("published", &self.published)
            .finish()
    }
}

impl<N: TreiberNode, M: GkChannelMemory> Drop for GkMpmcChain<'_, N, M> {
    fn drop(&mut self) {
        // Jobs this chain holds to itself: the untaken first job, then the private rest.
        let mut rest = self.private;
        // BRANCH: the first job was never taken; put it back in front
        if !self.next.is_null() {
            // Safety: the job is ours.
            unsafe { (*self.next).next().store(rest, Ordering::Relaxed) };
            rest = self.next;
        }
        // BRANCH: nothing held (the usual case: the rest is in a slot, or all was run)
        if rest.is_null() {
            return;
        }
        let (_, start) = next_turn();
        // BRANCH: share them in a free slot if one came free; else back on the lane, so they are never lost
        if self.shared.publish(rest, start).is_none() {
            self.shared.give_back(rest, self.lane);
        }
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
    /// returns how many there were. For a producer that sees the pool shut down after pushing. Jobs already reaped,
    /// including those shared in chain slots, are the workers' (or [`GkTreiberMpmc::drain`]'s).
    pub fn drain(&mut self, mut f: impl FnMut(NonNull<N>)) -> usize {
        let head = self.lane().reap();
        // Safety: a reaped chain is ours.
        unsafe { for_each_job(head, &mut f) }
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

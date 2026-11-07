//! GkTreiberMpmc: chains in push order, shared chains taken over from a blocked worker, ranking, lane limits and
//! reuse, drains, unrun jobs freed, and stress runs with short-lived workers and a pool-style parking handshake.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::{
    GkChannelMemory, GkRegionMemory, GkTreiberChannelError, GkTreiberMpmc, GkTreiberMpmcConfig,
    TreiberNode,
};

use std::{
    ptr::NonNull,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

// ==== Helpers ====

struct Job {
    next: AtomicPtr<Job>,
    lane: usize,
    seq: u64,
}

impl TreiberNode for Job {
    type Value = u64;

    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &u64 {
        &self.seq
    }
}

fn job(lane: usize, seq: u64) -> NonNull<Job> {
    NonNull::new(Box::into_raw(Box::new(Job {
        next: AtomicPtr::default(),
        lane,
        seq,
    })))
    .unwrap()
}

fn take(job: NonNull<Job>) -> Box<Job> {
    // Safety: every job in these tests comes from `job` and is taken once.
    unsafe { Box::from_raw(job.as_ptr()) }
}

fn seq(job: NonNull<Job>) -> u64 {
    take(job).seq
}

fn within<R: Send + 'static>(mut seconds: u64, f: impl FnOnce() -> R + Send + 'static) -> R {
    // Miri runs far slower than native code.
    if cfg!(miri) {
        seconds *= 100;
    }
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(seconds))
        .expect("test timed out")
}

/// One `try_reap`, run to the end: the (lane, seq) of each job, in order.
fn reap_one<M: GkChannelMemory>(queue: &GkTreiberMpmc<Job, M>) -> Option<Vec<(usize, u64)>> {
    let chain = queue.try_reap()?;
    Some(
        chain
            .map(|job| {
                let job = take(job);
                (job.lane, job.seq)
            })
            .collect(),
    )
}

/// `try_reap` until `None`, returning the sequence numbers in the order they came.
fn reap_all<M: GkChannelMemory>(queue: &GkTreiberMpmc<Job, M>) -> Vec<u64> {
    let mut seqs = Vec::new();
    while let Some(chain) = reap_one(queue) {
        seqs.extend(chain.into_iter().map(|(_, seq)| seq));
    }
    seqs
}

fn push_range(lane: &mut adaptif_gatotkaca_utils::GkMpmcLane<Job>, seqs: std::ops::Range<u64>) {
    let index = lane.index();
    for seq in seqs {
        unsafe { lane.push(job(index, seq)) };
    }
}

// ==== Single thread ====

#[test]
fn one_reap_takes_the_whole_lane_in_push_order() {
    let queue = GkTreiberMpmc::<Job>::new();
    assert!(queue.try_reap().is_none());
    assert!(!queue.has_work());
    let mut lane = queue.lane().unwrap();
    push_range(&mut lane, 0..1000);
    assert!(queue.has_work());
    let chain = queue.try_reap().unwrap();
    assert!(chain.published());
    assert_eq!(
        chain.map(seq).collect::<Vec<_>>(),
        (0..1000).collect::<Vec<_>>()
    );
    assert!(queue.try_reap().is_none());
    assert!(!queue.has_work());
    // A single job has nothing to share.
    push_range(&mut lane, 1000..1001);
    let chain = queue.try_reap().unwrap();
    assert!(!chain.published());
    assert_eq!(chain.map(seq).collect::<Vec<_>>(), [1000]);
}

#[test]
fn chains_between_pushes_stay_in_order() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut lane = queue.lane().unwrap();
    let mut seen = Vec::new();
    let mut next = 0;
    for burst in [10, 1, 7, 3, 20, 4] {
        push_range(&mut lane, next..next + burst);
        next += burst;
        let chain = reap_one(&queue).unwrap();
        assert_eq!(chain.len() as u64, burst);
        seen.extend(chain.into_iter().map(|(_, seq)| seq));
    }
    assert_eq!(seen, (0..next).collect::<Vec<_>>());
}

#[test]
fn a_blocked_chain_is_taken_over() {
    let delay = Duration::from_millis(2);
    let queue =
        GkTreiberMpmc::<Job>::with_config(GkTreiberMpmcConfig::DEFAULT.with_rescue_after(delay))
            .unwrap();
    let mut lane = queue.lane().unwrap();
    push_range(&mut lane, 0..100);
    // Worker A reaps and "blocks" in its first job.
    let mut a = queue.try_reap().unwrap();
    assert_eq!(seq(a.next().unwrap()), 0);
    assert!(a.published());
    // The lanes are empty; the rest is shared.
    assert!(!queue.has_work());
    assert_eq!(queue.shared_chains(), 1);
    // Worker B finds the chain stalled once `delay` has passed, and takes it over.
    let started = std::time::Instant::now();
    let mut b = loop {
        if let Some(chain) = queue.try_reap() {
            break chain;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "never rescued");
        thread::sleep(delay / 4);
    };
    assert!(!b.published());
    assert_eq!(seq(b.next().unwrap()), 1);
    // B took the chain over: A, back from its block, gets nothing more, and B carries on alone.
    assert!(a.next().is_none());
    assert_eq!(b.map(seq).collect::<Vec<_>>(), (2..100).collect::<Vec<_>>());
    assert_eq!(queue.shared_chains(), 0);
}

#[test]
fn a_busy_chain_is_not_taken_over() {
    let queue = GkTreiberMpmc::<Job>::with_config(
        GkTreiberMpmcConfig::DEFAULT.with_rescue_after(Duration::from_secs(60)),
    )
    .unwrap();
    let mut lane = queue.lane().unwrap();
    push_range(&mut lane, 0..10);
    let mut a = queue.try_reap().unwrap();
    assert_eq!(seq(a.next().unwrap()), 0);
    // Idle workers look, but the chain has not stalled for `rescue_after`: it stays with its owner.
    for _ in 0..3 {
        assert!(queue.try_reap().is_none());
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(a.map(seq).collect::<Vec<_>>(), (1..10).collect::<Vec<_>>());
}

#[test]
fn zero_chain_slots_keep_the_chain_private() {
    let queue =
        GkTreiberMpmc::<Job>::with_config(GkTreiberMpmcConfig::DEFAULT.with_max_chains(0)).unwrap();
    let mut lane = queue.lane().unwrap();
    push_range(&mut lane, 0..100);
    let mut a = queue.try_reap().unwrap();
    assert_eq!(seq(a.next().unwrap()), 0);
    assert!(!a.published());
    // Nobody else can see the rest.
    assert!(queue.try_reap().is_none());
    assert!(!queue.has_work());
    assert_eq!(a.map(seq).collect::<Vec<_>>(), (1..100).collect::<Vec<_>>());
}

#[test]
fn a_dropped_chain_gives_its_jobs_back() {
    // No rescue delay: the dropped chain has no owner, and this test takes it over at once.
    let queue = GkTreiberMpmc::<Job>::with_config(
        GkTreiberMpmcConfig::DEFAULT.with_rescue_after(Duration::ZERO),
    )
    .unwrap();
    let mut lane = queue.lane().unwrap();
    push_range(&mut lane, 0..10);
    // Dropped before taking anything: the first job is shared again, the rest already is.
    drop(queue.try_reap().unwrap());
    let mut seen = reap_all(&queue);
    seen.sort_unstable();
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
}

#[test]
fn ranks_the_largest_backlog_first() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut a = queue.lane().unwrap();
    let mut b = queue.lane().unwrap();
    push_range(&mut a, 0..3);
    push_range(&mut b, 0..50);
    let first = reap_one(&queue).unwrap();
    assert_eq!(first.len(), 50);
    assert!(first.iter().all(|&(lane, _)| lane == b.index()));
    // The reap counted as taken: `b` now has a smaller backlog than `a`.
    push_range(&mut b, 50..52);
    let second = reap_one(&queue).unwrap();
    assert_eq!(second, [(a.index(), 0), (a.index(), 1), (a.index(), 2)]);
    assert_eq!(reap_one(&queue).unwrap().len(), 2);
    assert!(queue.try_reap().is_none());
}

#[test]
fn lane_limit_and_reuse() {
    let queue =
        GkTreiberMpmc::<Job>::with_config(GkTreiberMpmcConfig::DEFAULT.with_max_lanes(2)).unwrap();
    let mut first = queue.lane().unwrap();
    let second = queue.lane().unwrap();
    assert_eq!(
        queue.lane().unwrap_err(),
        GkTreiberChannelError::TOO_MANY_LANES
    );
    assert_eq!((first.index(), second.index()), (0, 1));
    push_range(&mut first, 0..6);
    // Released with jobs still queued: they stay, and the next owner's jobs come after them.
    drop(first);
    let mut reused = queue.lane().unwrap();
    assert_eq!(reused.index(), 0);
    push_range(&mut reused, 6..12);
    assert_eq!(reap_all(&queue), (0..12).collect::<Vec<_>>());
    drop(second);
}

#[test]
fn lane_drain_takes_what_workers_have_not_reaped() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut lane = queue.lane().unwrap();
    push_range(&mut lane, 0..10);
    // A worker reaped these: job 0 is its own, 1..10 wait in a chain slot.
    let mut held = queue.try_reap().unwrap();
    assert_eq!(seq(held.next().unwrap()), 0);
    push_range(&mut lane, 10..110);
    let mut drained = Vec::new();
    assert_eq!(lane.drain(|job| drained.push(seq(job))), 100);
    assert_eq!(drained, (10..110).collect::<Vec<_>>());
    // The queue's drain also takes the shared chain.
    let mut rest = Vec::new();
    assert_eq!(queue.drain(|job| rest.push(seq(job))), 9);
    assert_eq!(rest, (1..10).collect::<Vec<_>>());
    assert!(held.next().is_none());
    assert!(!queue.has_work());
}

#[test]
fn queue_drain_takes_every_lane() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut lanes: Vec<_> = (0..3).map(|_| queue.lane().unwrap()).collect();
    for (index, lane) in lanes.iter_mut().enumerate() {
        push_range(lane, 0..(10 * (index as u64 + 1)));
    }
    let mut per_lane = [0u64; 3];
    assert_eq!(queue.drain(|job| per_lane[take(job).lane] += 1), 60);
    assert_eq!(per_lane, [10, 20, 30]);
    assert!(!queue.has_work());
}

#[test]
fn invalid_config() {
    for config in [
        GkTreiberMpmcConfig::DEFAULT.with_max_lanes(0),
        GkTreiberMpmcConfig::DEFAULT.with_max_lanes(usize::MAX),
        GkTreiberMpmcConfig::DEFAULT.with_max_chains(usize::MAX),
    ] {
        assert_eq!(
            GkTreiberMpmc::<Job>::with_config(config).unwrap_err(),
            GkTreiberChannelError::INVALID_CONFIG
        );
    }
}

// ==== Clean-up ====

static DROPPED: AtomicUsize = AtomicUsize::new(0);

/// A job that counts its drops (only used by `unrun_jobs_are_freed`).
struct Counted {
    next: AtomicPtr<Counted>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

impl TreiberNode for Counted {
    type Value = ();

    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &() {
        &()
    }
}

#[test]
fn unrun_jobs_are_freed() {
    let counted = || {
        NonNull::new(Box::into_raw(Box::new(Counted {
            next: AtomicPtr::default(),
        })))
        .unwrap()
    };
    let run = |job: NonNull<Counted>| drop(unsafe { Box::from_raw(job.as_ptr()) });
    // Without chain slots, a chain dropped half-run puts the rest back on its lane; the last handle frees it.
    let private =
        GkTreiberMpmc::<Counted>::with_config(GkTreiberMpmcConfig::DEFAULT.with_max_chains(0))
            .unwrap();
    let mut lane = private.lane().unwrap();
    for _ in 0..50 {
        unsafe { lane.push(counted()) };
    }
    let mut chain = private.try_reap().unwrap();
    for _ in 0..2 {
        run(chain.next().unwrap());
    }
    drop(chain);
    assert_eq!(DROPPED.load(Ordering::Relaxed), 2);
    assert!(private.has_work());
    drop((lane, private));
    assert_eq!(DROPPED.load(Ordering::Relaxed), 50);
    // With chain slots, the rest stays shared; the last handle frees it, and what is still in the lanes.
    let queue = GkTreiberMpmc::<Counted>::new();
    let worker = queue.clone();
    let mut lane = queue.lane().unwrap();
    for _ in 0..50 {
        unsafe { lane.push(counted()) };
    }
    let mut chain = queue.try_reap().unwrap();
    for _ in 0..2 {
        run(chain.next().unwrap());
    }
    drop(chain);
    for _ in 0..10 {
        unsafe { lane.push(counted()) };
    }
    drop((lane, queue));
    assert_eq!(DROPPED.load(Ordering::Relaxed), 52);
    drop(worker);
    assert_eq!(DROPPED.load(Ordering::Relaxed), 110);
}

#[test]
fn region_memory_is_given_back() {
    let mut buffer = vec![0u64; 16 * 1024];
    let base = NonNull::new(buffer.as_mut_ptr().cast::<u8>()).unwrap();
    // Safety: the buffer outlives the queue and nothing else uses it.
    let region = unsafe { GkRegionMemory::new(base, buffer.len() * 8) }.unwrap();
    let queue = GkTreiberMpmc::<Job, _>::with_memory(
        GkTreiberMpmcConfig::DEFAULT
            .with_max_lanes(4)
            .with_max_chains(8),
        region,
    )
    .unwrap();
    assert!(region.in_use() > 0);
    let mut lane = queue.lane().unwrap();
    let index = lane.index();
    for seq in 0..40 {
        unsafe { lane.push(job(index, seq)) };
    }
    assert_eq!(reap_all(&queue), (0..40).collect::<Vec<_>>());
    drop((lane, queue));
    assert_eq!(region.in_use(), 0);
}

// ==== Threads ====

/// A worker blocks in the first job of a long chain until every other job has run. Other workers must take the rest
/// from its chain slot; if the chain were held, this would never finish.
#[test]
fn other_workers_finish_a_blocked_chain() {
    const JOBS: u64 = if cfg!(miri) { 200 } else { 20_000 };
    within(60, || {
        let queue = GkTreiberMpmc::<Job>::new();
        let mut lane = queue.lane().unwrap();
        push_range(&mut lane, 0..JOBS);
        let mut blocked = queue.try_reap().unwrap();
        assert!(blocked.published());
        let first = blocked.next().unwrap();
        let done = Arc::new(AtomicU64::new(0));
        let helpers: Vec<_> = (0..3)
            .map(|_| {
                let (queue, done) = (queue.clone(), done.clone());
                thread::spawn(move || {
                    while done.load(Ordering::Relaxed) < JOBS - 1 {
                        let Some(chain) = queue.try_reap() else {
                            thread::yield_now();
                            continue;
                        };
                        let mut last = None;
                        for job in chain {
                            let seq = seq(job);
                            assert!(last.is_none_or(|last| last < seq));
                            last = Some(seq);
                            done.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        // The "blocked" job waits for everything else.
        while done.load(Ordering::Relaxed) < JOBS - 1 {
            thread::yield_now();
        }
        assert_eq!(seq(first), 0);
        assert!(blocked.next().is_none());
        for handle in helpers {
            handle.join().unwrap();
        }
        assert!(!queue.has_work());
    });
}

/// Producers push into their lanes while workers come and go (each runs a few chains and exits). Every job is taken
/// exactly once, and each chain yields one lane's jobs in push order.
#[test]
fn exactly_once_with_short_lived_workers() {
    const PRODUCERS: usize = 4;
    const PER: u64 = if cfg!(miri) { 300 } else { 100_000 };
    const WORKERS: usize = 8;
    within(120, || {
        let queue = GkTreiberMpmc::<Job>::new();
        let seen: Arc<Vec<AtomicBool>> = Arc::new(
            (0..PRODUCERS as u64 * PER)
                .map(|_| AtomicBool::new(false))
                .collect(),
        );
        let taken = Arc::new(AtomicU64::new(0));
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|_| {
                let mut lane = queue.lane().unwrap();
                thread::spawn(move || {
                    let index = lane.index();
                    for seq in 0..PER {
                        unsafe { lane.push(job(index, seq)) };
                        if seq % 1024 == 0 {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        let total = PRODUCERS as u64 * PER;
        let spawn_worker = |chains: u64| {
            let (queue, seen, taken) = (queue.clone(), seen.clone(), taken.clone());
            thread::spawn(move || {
                let mut done = 0;
                while done < chains && taken.load(Ordering::Relaxed) < total {
                    let Some(chain) = queue.try_reap() else {
                        thread::yield_now();
                        continue;
                    };
                    // One reap per chain: one lane, oldest first (other workers may take some in between).
                    let mut last = None::<(usize, u64)>;
                    for job in chain {
                        let job = take(job);
                        assert!(last.is_none_or(|(lane, seq)| lane == job.lane && seq < job.seq));
                        last = Some((job.lane, job.seq));
                        let slot = job.lane as u64 * PER + job.seq;
                        assert!(!seen[slot as usize].swap(true, Ordering::Relaxed));
                        taken.fetch_add(1, Ordering::Relaxed);
                    }
                    done += 1;
                }
            })
        };
        let mut workers: Vec<_> = (0..WORKERS).map(|i| spawn_worker(5 + i as u64)).collect();
        let mut round = 0u64;
        while taken.load(Ordering::Relaxed) < total {
            // Replace workers that ran their chains, like a pool's keep-alive exits.
            for worker in workers.iter_mut() {
                if worker.is_finished() {
                    round += 1;
                    let fresh = spawn_worker(1 + round % 20);
                    std::mem::replace(worker, fresh).join().unwrap();
                }
            }
            thread::yield_now();
        }
        for handle in producers.into_iter().chain(workers) {
            handle.join().unwrap();
        }
        assert_eq!(taken.load(Ordering::Relaxed), total);
        assert!(seen.iter().all(|seen| seen.load(Ordering::Relaxed)));
        assert!(!queue.has_work());
    });
}

/// A pool-style handshake: a worker announces itself asleep with a `SeqCst` RMW on the state word, re-checks
/// `has_work` and only then waits; a producer pushes, then does a `SeqCst` RMW on the same word and wakes a worker if
/// any is asleep. While chains are shared, a worker parks with a timeout, to rescue one that stalls. Small bursts keep the workers parking all
/// the time. A lost wake-up hangs, and the test times out.
#[test]
fn no_lost_wakeups_with_pool_handshake() {
    const PRODUCERS: usize = 3;
    const PER: u64 = if cfg!(miri) { 200 } else { 30_000 };
    const WORKERS: usize = 6;
    within(120, || {
        struct Pool {
            queue: GkTreiberMpmc<Job>,
            sleepers: AtomicUsize,
            lock: Mutex<()>,
            wake: Condvar,
            done: AtomicBool,
            taken: AtomicU64,
        }
        impl Pool {
            fn notify(&self) {
                // BRANCH: someone is (about to be) asleep
                if self.sleepers.fetch_add(0, Ordering::SeqCst) > 0 {
                    let _guard = self.lock.lock().unwrap();
                    self.wake.notify_one();
                }
            }
        }
        let pool = Arc::new(Pool {
            queue: GkTreiberMpmc::new(),
            sleepers: AtomicUsize::new(0),
            lock: Mutex::new(()),
            wake: Condvar::new(),
            done: AtomicBool::new(false),
            taken: AtomicU64::new(0),
        });
        let workers: Vec<_> = (0..WORKERS)
            .map(|_| {
                let pool = pool.clone();
                thread::spawn(move || {
                    loop {
                        if let Some(chain) = pool.queue.try_reap() {
                            if chain.published() {
                                pool.notify();
                            }
                            let count = chain.map(|job| drop(take(job))).count();
                            pool.taken.fetch_add(count as u64, Ordering::Relaxed);
                            continue;
                        }
                        let guard = pool.lock.lock().unwrap();
                        pool.sleepers.fetch_add(1, Ordering::SeqCst);
                        // BRANCH: the last re-check before parking
                        if pool.queue.has_work() {
                            pool.sleepers.fetch_sub(1, Ordering::SeqCst);
                            continue;
                        }
                        let shared = pool.queue.shared_chains() > 0;
                        if pool.done.load(Ordering::SeqCst) && !shared {
                            pool.sleepers.fetch_sub(1, Ordering::SeqCst);
                            return;
                        }
                        // BRANCH: a shared chain may stall with nobody to signal it; wake in time to rescue it
                        let guard = if shared {
                            pool.wake
                                .wait_timeout(
                                    guard,
                                    2 * adaptif_gatotkaca_utils::GK_MPMC_RESCUE_AFTER,
                                )
                                .unwrap()
                                .0
                        } else {
                            pool.wake.wait(guard).unwrap()
                        };
                        pool.sleepers.fetch_sub(1, Ordering::SeqCst);
                        drop(guard);
                    }
                })
            })
            .collect();
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|_| {
                let pool = pool.clone();
                let mut lane = pool.queue.lane().unwrap();
                thread::spawn(move || {
                    let index = lane.index();
                    for seq in 0..PER {
                        unsafe { lane.push(job(index, seq)) };
                        pool.notify();
                        if seq % 64 == 0 {
                            thread::sleep(Duration::from_micros(20));
                        }
                    }
                })
            })
            .collect();
        for handle in producers {
            handle.join().unwrap();
        }
        // Wait for the workers to take everything (a lost wake-up would leave jobs here and time out).
        while pool.taken.load(Ordering::Relaxed) < PRODUCERS as u64 * PER {
            thread::sleep(Duration::from_millis(1));
        }
        pool.done.store(true, Ordering::SeqCst);
        {
            let _guard = pool.lock.lock().unwrap();
            pool.wake.notify_all();
        }
        for handle in workers {
            handle.join().unwrap();
        }
        assert!(!pool.queue.has_work());
    });
}

/// A producer drains its own lane while it pushes and workers reap it; the queue's drain takes what the workers left
/// in chain slots. Every job is taken once.
#[test]
fn producer_drain_races_workers() {
    const PER: u64 = if cfg!(miri) { 300 } else { 50_000 };
    within(60, || {
        for _ in 0..if cfg!(miri) { 2 } else { 20 } {
            let queue = GkTreiberMpmc::<Job>::new();
            let mut lane = queue.lane().unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let workers: Vec<_> = (0..4)
                .map(|_| {
                    let (queue, stop) = (queue.clone(), stop.clone());
                    thread::spawn(move || {
                        let mut seqs = Vec::new();
                        while !stop.load(Ordering::Relaxed) {
                            if let Some(mut chain) = queue.try_reap() {
                                // Run part of the chain, as a worker that exits midway would.
                                seqs.extend(chain.by_ref().take(7).map(seq));
                            }
                        }
                        seqs
                    })
                })
                .collect();
            let mut drained = Vec::new();
            for seq in 0..PER {
                unsafe { lane.push(job(0, seq)) };
                if seq % 100 == 99 {
                    lane.drain(|job| drained.push(self::seq(job)));
                }
            }
            lane.drain(|job| drained.push(seq(job)));
            stop.store(true, Ordering::Relaxed);
            // Not checked for order: a worker that drops its chain while every slot is taken puts the older jobs back
            // on top of the lane, so a drain can return them after newer ones.
            let mut all = drained;
            for handle in workers {
                all.extend(handle.join().unwrap());
            }
            queue.drain(|job| all.push(seq(job)));
            all.sort_unstable();
            assert_eq!(all, (0..PER).collect::<Vec<_>>());
        }
    });
}

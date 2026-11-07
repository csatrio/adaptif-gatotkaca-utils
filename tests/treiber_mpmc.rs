//! GkTreiberMpmc: chains in push order, ranking, lane limits and reuse, drains, unrun jobs freed, and stress runs with
//! short-lived workers and a pool-style parking handshake.

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

fn take(job: *mut Job) -> Box<Job> {
    // Safety: every job in these tests comes from `job` and is taken once.
    unsafe { Box::from_raw(job) }
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

/// One reap: the (lane, seq) of each job, in chain order.
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

/// Reaps until `None`, returning the sequence numbers in reap order.
fn reap_all<M: GkChannelMemory>(queue: &GkTreiberMpmc<Job, M>) -> Vec<u64> {
    let mut seqs = Vec::new();
    while let Some(chain) = reap_one(queue) {
        seqs.extend(chain.into_iter().map(|(_, seq)| seq));
    }
    seqs
}

// ==== Single thread ====

#[test]
fn one_reap_takes_the_whole_lane_in_push_order() {
    let queue = GkTreiberMpmc::<Job>::new();
    assert!(queue.try_reap().is_none());
    assert!(!queue.has_work());
    let mut lane = queue.lane().unwrap();
    for seq in 0..1000 {
        unsafe { lane.push(job(0, seq)) };
    }
    assert!(queue.has_work());
    let chain = reap_one(&queue).unwrap();
    assert_eq!(
        chain.iter().map(|&(_, seq)| seq).collect::<Vec<_>>(),
        (0..1000).collect::<Vec<_>>()
    );
    assert!(queue.try_reap().is_none());
    assert!(!queue.has_work());
}

#[test]
fn chains_between_pushes_stay_in_order() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut lane = queue.lane().unwrap();
    let mut seen = Vec::new();
    let mut next = 0;
    for burst in [10, 1, 7, 3, 20, 4] {
        for _ in 0..burst {
            unsafe { lane.push(job(0, next)) };
            next += 1;
        }
        let chain = reap_one(&queue).unwrap();
        assert_eq!(chain.len(), burst);
        seen.extend(chain.into_iter().map(|(_, seq)| seq));
    }
    assert_eq!(seen, (0..next).collect::<Vec<_>>());
}

#[test]
fn ranks_the_largest_backlog_first() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut a = queue.lane().unwrap();
    let mut b = queue.lane().unwrap();
    for seq in 0..3 {
        unsafe { a.push(job(a.index(), seq)) };
    }
    for seq in 0..50 {
        unsafe { b.push(job(b.index(), seq)) };
    }
    let first = reap_one(&queue).unwrap();
    assert_eq!(first.len(), 50);
    assert!(first.iter().all(|&(lane, _)| lane == b.index()));
    // The reap counted as taken: `b` now has a smaller backlog than `a`.
    for seq in 50..52 {
        unsafe { b.push(job(b.index(), seq)) };
    }
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
    for seq in 0..6 {
        unsafe { first.push(job(0, seq)) };
    }
    // Released with jobs still queued: they stay, and the next owner's jobs come after them.
    drop(first);
    let mut reused = queue.lane().unwrap();
    assert_eq!(reused.index(), 0);
    for seq in 6..12 {
        unsafe { reused.push(job(0, seq)) };
    }
    assert_eq!(reap_all(&queue), (0..12).collect::<Vec<_>>());
    drop(second);
}

#[test]
fn lane_drain_takes_what_workers_have_not_reaped() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut lane = queue.lane().unwrap();
    for seq in 0..10 {
        unsafe { lane.push(job(0, seq)) };
    }
    // A worker holds this chain; the drain does not see it.
    let held = queue.try_reap().unwrap();
    for seq in 10..110 {
        unsafe { lane.push(job(0, seq)) };
    }
    let mut drained = Vec::new();
    assert_eq!(lane.drain(|job| drained.push(take(job.as_ptr()).seq)), 100);
    assert_eq!(drained, (10..110).collect::<Vec<_>>());
    assert!(!queue.has_work());
    assert_eq!(
        held.map(|job| take(job).seq).collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
}

#[test]
fn queue_drain_takes_every_lane() {
    let queue = GkTreiberMpmc::<Job>::new();
    let mut lanes: Vec<_> = (0..3).map(|_| queue.lane().unwrap()).collect();
    for (index, lane) in lanes.iter_mut().enumerate() {
        for seq in 0..(10 * (index as u64 + 1)) {
            unsafe { lane.push(job(index, seq)) };
        }
    }
    let mut per_lane = [0u64; 3];
    assert_eq!(
        queue.drain(|job| per_lane[take(job.as_ptr()).lane] += 1),
        60
    );
    assert_eq!(per_lane, [10, 20, 30]);
    assert!(!queue.has_work());
}

#[test]
fn invalid_config() {
    for config in [
        GkTreiberMpmcConfig::DEFAULT.with_max_lanes(0),
        GkTreiberMpmcConfig::DEFAULT.with_max_lanes(usize::MAX),
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
    let queue = GkTreiberMpmc::<Counted>::new();
    let worker = queue.clone();
    let mut lane = queue.lane().unwrap();
    for _ in 0..50 {
        unsafe { lane.push(counted()) };
    }
    // A worker runs two jobs of its chain and drops the rest.
    let mut chain = queue.try_reap().unwrap();
    for _ in 0..2 {
        drop(unsafe { Box::from_raw(chain.next().unwrap()) });
    }
    drop(chain);
    assert_eq!(DROPPED.load(Ordering::Relaxed), 50);
    for _ in 0..10 {
        unsafe { lane.push(counted()) };
    }
    // Jobs still queued are freed by the last handle.
    drop((lane, queue));
    assert_eq!(DROPPED.load(Ordering::Relaxed), 50);
    drop(worker);
    assert_eq!(DROPPED.load(Ordering::Relaxed), 60);
}

#[test]
fn region_memory_is_given_back() {
    let mut buffer = vec![0u64; 16 * 1024];
    let base = NonNull::new(buffer.as_mut_ptr().cast::<u8>()).unwrap();
    // Safety: the buffer outlives the queue and nothing else uses it.
    let region = unsafe { GkRegionMemory::new(base, buffer.len() * 8) }.unwrap();
    let queue = GkTreiberMpmc::<Job, _>::with_memory(
        GkTreiberMpmcConfig::DEFAULT.with_max_lanes(4),
        region,
    )
    .unwrap();
    assert!(region.in_use() > 0);
    let mut lane = queue.lane().unwrap();
    for seq in 0..40 {
        unsafe { lane.push(job(0, seq)) };
    }
    assert_eq!(reap_all(&queue), (0..40).collect::<Vec<_>>());
    drop((lane, queue));
    assert_eq!(region.in_use(), 0);
}

// ==== Threads ====

/// Producers push into their lanes while workers come and go (each runs a few chains and exits). Every job is taken
/// exactly once, each chain is a run of consecutive jobs of one lane, and each worker sees each lane's jobs in push
/// order.
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
                let mut last = [None::<u64>; PRODUCERS];
                let mut done = 0;
                while done < chains && taken.load(Ordering::Relaxed) < total {
                    let Some(chain) = queue.try_reap() else {
                        thread::yield_now();
                        continue;
                    };
                    let mut chain_lane = None;
                    let mut count = 0;
                    for job in chain {
                        let job = take(job);
                        // One lane per chain, consecutive jobs.
                        assert_eq!(*chain_lane.get_or_insert(job.lane), job.lane);
                        if let Some(last) = last[job.lane] {
                            assert!(last < job.seq);
                            if count > 0 {
                                assert_eq!(last + 1, job.seq);
                            }
                        }
                        last[job.lane] = Some(job.seq);
                        let slot = job.lane as u64 * PER + job.seq;
                        assert!(!seen[slot as usize].swap(true, Ordering::Relaxed));
                        count += 1;
                    }
                    taken.fetch_add(count, Ordering::Relaxed);
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
/// any is asleep. Small bursts keep the workers parking all the time. A lost wake-up hangs, and the test times out.
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
                        if pool.done.load(Ordering::SeqCst) {
                            pool.sleepers.fetch_sub(1, Ordering::SeqCst);
                            return;
                        }
                        let guard = pool.wake.wait(guard).unwrap();
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
                        // BRANCH: someone is (about to be) asleep
                        if pool.sleepers.fetch_add(0, Ordering::SeqCst) > 0 {
                            let _guard = pool.lock.lock().unwrap();
                            pool.wake.notify_one();
                        }
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

/// A producer drains its own lane while it pushes and workers reap it: every job is taken once, by one side or the
/// other.
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
                            if let Some(chain) = queue.try_reap() {
                                seqs.extend(chain.map(|job| take(job).seq));
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
                    lane.drain(|job| drained.push(take(job.as_ptr()).seq));
                }
            }
            lane.drain(|job| drained.push(take(job.as_ptr()).seq));
            stop.store(true, Ordering::Relaxed);
            assert!(drained.is_sorted());
            let mut all = drained;
            for handle in workers {
                let seqs = handle.join().unwrap();
                assert!(seqs.is_sorted());
                all.extend(seqs);
            }
            all.sort_unstable();
            assert_eq!(all, (0..PER).collect::<Vec<_>>());
        }
    });
}

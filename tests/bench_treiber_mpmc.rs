//! GkTreiberMpmc against crossbeam's MPMC channel and crossbeam-deque (measurement, `--ignored`):
//! `cargo test --release --test bench_treiber_mpmc -- --ignored --nocapture`
//!
//! P producer threads each queue boxed jobs (allocated before the clock starts); C worker threads take them, run them
//! and free them. Contenders:
//!
//! - `treiber`: `GkTreiberMpmc`, workers spin on `try_reap` and run each chain (the pool decides parking, not the
//!   queue). The rest of each reap is shared in a chain slot, so idle workers take over from a blocked one.
//! - `reap-only`: the same with `max_chains(0)`: a worker keeps its whole reap, so a block holds the rest.
//! - `cb channel`: `crossbeam_channel::unbounded`, workers use `recv` (spins before it parks).
//! - `cb injector`: `crossbeam_deque::Injector`, workers spin on `steal`, one job at a time: a shared FIFO, no
//!   per-worker state.
//! - `cb deque`: tokio's model, the injector plus a FIFO `Worker` deque per worker, filled with `steal_batch_and_pop`
//!   and stolen from by the others.
//!
//! Two runs:
//!
//! - throughput: empty jobs, `THROUGHPUT_JOBS` per producer, millions of jobs per second.
//! - blocking: `BLOCKING_JOBS` per producer, every `BLOCK_EVERY`th job sleeps `BLOCK_FOR` (an `fsync` stand-in);
//!   wall time in milliseconds. The ideal is the total sleep divided by the workers.
//!
//! A third test, `bench_mpmc_parking`, runs the queue in a pool whose workers **park** when idle (the runs above
//! spin), to measure what the module docs' park/wake handshake costs a producer on every push:
//!
//! - `rmw`: after each push, a `SeqCst` read-modify-write on the pool's idle count (the module docs' alternative); a
//!   worker about to park does its own `SeqCst` read-modify-write on it, then `has_work`.
//! - `fence`: after each push, a `SeqCst` fence and a load of the idle count (the module docs' recipe, tokio's
//!   pattern); the worker fences between announcing itself and `has_work`.
//! - `cb channel`: `crossbeam_channel::unbounded`, whose `recv` parks by itself (reference).
//!
//! It reports throughput with continuous producers (empty jobs), and push-to-run latency with bursty producers
//! (`BURSTS` bursts of `BURST` jobs, `BURST_GAP` apart), where workers park between bursts and every burst needs a
//! wake-up.
//!
//! Median of `ROUNDS` runs, each bounded by `RUN_LIMIT`.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::{GkTreiberMpmc, GkTreiberMpmcConfig, TreiberNode};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::{Backoff, CachePadded};

use std::{
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence},
        mpsc,
    },
    thread::{self, Thread},
    time::{Duration, Instant},
};

// ==== Parameters ====

const THROUGHPUT_JOBS: u64 = 500_000;
const PRODUCER_COUNTS: [usize; 3] = [1, 2, 4];
const WORKER_COUNTS: [usize; 3] = [2, 4, 8];
const BLOCKING_JOBS: u64 = 20_000;
const BLOCK_EVERY: u64 = 500;
const BLOCK_FOR: Duration = Duration::from_millis(1);
const BLOCKING_SHAPES: [(usize, usize); 3] = [(1, 4), (2, 8), (4, 8)];
const ROUNDS: usize = 5;
const RUN_LIMIT: Duration = Duration::from_secs(120);
const PARKING_SHAPES: [(usize, usize); 4] = [(1, 2), (1, 4), (2, 4), (4, 4)];
const BURSTS: u64 = 400;
const BURST: u64 = 16;
const BURST_GAP: Duration = Duration::from_micros(50);

/// What the producers queue.
#[derive(Clone, Copy)]
struct Load {
    producers: usize,
    workers: usize,
    per_producer: u64,
    /// Every this many jobs, one sleeps `BLOCK_FOR`; 0 for none.
    block_every: u64,
}

impl Load {
    fn total(&self) -> u64 {
        self.per_producer * self.producers as u64
    }
}

/// The contenders.
#[derive(Clone, Copy)]
enum Kind {
    Treiber,
    ReapOnly,
    Channel,
    Injector,
    Deque,
}

const KINDS: [Kind; 5] = [Kind::Treiber, Kind::ReapOnly, Kind::Channel, Kind::Injector, Kind::Deque];

impl Kind {
    fn name(self) -> &'static str {
        // BRANCH: one name per contender
        match self {
            Kind::Treiber => "treiber",
            Kind::ReapOnly => "reap-only",
            Kind::Channel => "cb channel",
            Kind::Injector => "cb injector",
            Kind::Deque => "cb deque",
        }
    }

    /// Seconds for one run of `load`.
    fn run(self, load: Load) -> f64 {
        // BRANCH: one runner per contender
        match self {
            Kind::Treiber => treiber(load, GkTreiberMpmcConfig::DEFAULT),
            Kind::ReapOnly => treiber(load, GkTreiberMpmcConfig::DEFAULT.with_max_chains(0)),
            Kind::Channel => crossbeam(load),
            Kind::Injector => deque(load, false),
            Kind::Deque => deque(load, true),
        }
    }
}

// ==== Helpers ====

struct Job {
    next: AtomicPtr<Job>,
    block: bool,
}

impl TreiberNode for Job {
    type Value = bool;

    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &bool {
        &self.block
    }
}

fn boxed(id: u64, block_every: u64) -> Box<Job> {
    Box::new(Job { next: AtomicPtr::default(), block: block_every != 0 && id % block_every == 0 })
}

/// Runs a job: sleeps if it is a blocking one, then frees it.
#[allow(clippy::boxed_local, reason = "each job is its own allocation, as in a pool")]
fn run(job: Box<Job>) {
    // BRANCH: an `fsync` stand-in
    if job.block {
        thread::sleep(BLOCK_FOR);
    }
}

/// Raw jobs that can cross threads (they are owned by whoever holds the list).
struct Jobs(Vec<NonNull<Job>>);

// Safety: the jobs are plain boxed data owned by this list.
unsafe impl Send for Jobs {}

fn within_limit<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(RUN_LIMIT).expect("benchmark run timed out")
}

fn median(mut runs: Vec<f64>) -> f64 {
    runs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    runs[runs.len() / 2]
}

#[allow(clippy::vec_box, reason = "each job is its own allocation, as in a pool")]
fn batches(load: Load) -> Vec<Vec<Box<Job>>> {
    (0..load.producers).map(|_| (0..load.per_producer).map(|id| boxed(id, load.block_every)).collect()).collect()
}

// ==== Runs ====

/// Seconds through a `GkTreiberMpmc` with `config`.
fn treiber(load: Load, config: GkTreiberMpmcConfig) -> f64 {
    within_limit(move || {
        let queue = GkTreiberMpmc::<Job>::with_config(config).unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let jobs: Vec<_> =
            batches(load).into_iter().map(|jobs| Jobs(jobs.into_iter().map(|job| NonNull::from(Box::leak(job))).collect())).collect();
        let lanes: Vec<_> = (0..load.producers).map(|_| queue.lane().unwrap()).collect();
        let start = Instant::now();
        let producer_handles: Vec<_> = lanes
            .into_iter()
            .zip(jobs)
            .map(|(mut lane, jobs)| {
                thread::spawn(move || {
                    // Move the whole list in (a closure would otherwise capture only the non-`Send` field).
                    let jobs = jobs;
                    for job in jobs.0 {
                        // Safety: a fresh job in no other queue.
                        unsafe { lane.push(job) };
                    }
                })
            })
            .collect();
        let worker_handles: Vec<_> = (0..load.workers)
            .map(|_| {
                let (queue, done) = (queue.clone(), done.clone());
                thread::spawn(move || {
                    let mut taken = 0u64;
                    loop {
                        // BRANCH: a chain, run here; otherwise stop once the producers are finished and nothing is
                        // left
                        if let Some(chain) = queue.try_reap() {
                            for job in chain {
                                // Safety: the job is ours now.
                                run(unsafe { Box::from_raw(job.as_ptr()) });
                                taken += 1;
                            }
                        } else if done.load(Ordering::Acquire) && !queue.has_work() && queue.shared_chains() == 0 {
                            return taken;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                })
            })
            .collect();
        for handle in producer_handles {
            handle.join().unwrap();
        }
        done.store(true, Ordering::Release);
        let taken: u64 = worker_handles.into_iter().map(|h| h.join().unwrap()).sum();
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(taken, load.total());
        secs
    })
}

/// Seconds through crossbeam's unbounded channel.
fn crossbeam(load: Load) -> f64 {
    within_limit(move || {
        let (tx, rx) = crossbeam_channel::unbounded::<Box<Job>>();
        let jobs = batches(load);
        let start = Instant::now();
        let producer_handles: Vec<_> = jobs
            .into_iter()
            .map(|jobs| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for job in jobs {
                        tx.send(job).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        let worker_handles: Vec<_> = (0..load.workers)
            .map(|_| {
                let rx = rx.clone();
                thread::spawn(move || {
                    let mut taken = 0u64;
                    while let Ok(job) = rx.recv() {
                        run(job);
                        taken += 1;
                    }
                    taken
                })
            })
            .collect();
        for handle in producer_handles {
            handle.join().unwrap();
        }
        let taken: u64 = worker_handles.into_iter().map(|h| h.join().unwrap()).sum();
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(taken, load.total());
        secs
    })
}

/// Seconds through crossbeam-deque: the injector alone (`local_deques` false, one job per steal), or the injector plus
/// per-worker deques filled in batches (`local_deques` true, tokio's model).
fn deque(load: Load, local_deques: bool) -> f64 {
    within_limit(move || {
        let injector = Arc::new(Injector::<Box<Job>>::new());
        let done = Arc::new(AtomicBool::new(false));
        let jobs = batches(load);
        let locals: Vec<Worker<Box<Job>>> = (0..load.workers).map(|_| Worker::new_fifo()).collect();
        let stealers: Arc<Vec<Stealer<Box<Job>>>> = Arc::new(locals.iter().map(Worker::stealer).collect());
        let start = Instant::now();
        let producer_handles: Vec<_> = jobs
            .into_iter()
            .map(|jobs| {
                let injector = injector.clone();
                thread::spawn(move || {
                    for job in jobs {
                        injector.push(job);
                    }
                })
            })
            .collect();
        let worker_handles: Vec<_> = locals
            .into_iter()
            .enumerate()
            .map(|(me, local)| {
                let (injector, stealers, done) = (injector.clone(), stealers.clone(), done.clone());
                thread::spawn(move || {
                    let mut taken = 0u64;
                    loop {
                        // BRANCH: one job from the injector, or tokio's local-then-batch-then-steal search
                        let step = if local_deques {
                            match local.pop() {
                                Some(job) => Steal::Success(job),
                                None => injector.steal_batch_and_pop(&local).or_else(|| {
                                    stealers
                                        .iter()
                                        .enumerate()
                                        .filter(|(other, _)| *other != me)
                                        .map(|(_, stealer)| stealer.steal())
                                        .collect()
                                }),
                            }
                        } else {
                            injector.steal()
                        };
                        match step {
                            Steal::Success(job) => {
                                run(job);
                                taken += 1;
                            }
                            Steal::Retry => std::hint::spin_loop(),
                            Steal::Empty => {
                                // BRANCH: the producers are finished and nothing is left (other workers finish their
                                // own local jobs)
                                if done.load(Ordering::Acquire) && injector.is_empty() && local.is_empty() {
                                    return taken;
                                }
                                std::hint::spin_loop();
                            }
                        }
                    }
                })
            })
            .collect();
        for handle in producer_handles {
            handle.join().unwrap();
        }
        done.store(true, Ordering::Release);
        let taken: u64 = worker_handles.into_iter().map(|h| h.join().unwrap()).sum();
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(taken, load.total());
        secs
    })
}

// ==== Report ====

fn header(unit: &str) {
    print!("{:>4} {:>4}", "P", "C");
    for kind in KINDS {
        print!(" {:>12}", kind.name());
    }
    println!("   ({unit})");
}

#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn bench_mpmc_throughput() {
    println!("\nthroughput, empty jobs: millions of jobs per second, median of {ROUNDS}");
    header("M jobs/s");
    for producers in PRODUCER_COUNTS {
        for workers in WORKER_COUNTS {
            let load = Load { producers, workers, per_producer: THROUGHPUT_JOBS, block_every: 0 };
            print!("{producers:>4} {workers:>4}");
            for kind in KINDS {
                let secs = median((0..ROUNDS).map(|_| kind.run(load)).collect());
                print!(" {:>12.1}", load.total() as f64 / secs / 1e6);
            }
            println!();
        }
    }
}

#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn bench_mpmc_blocking() {
    println!(
        "\nblocking: {BLOCKING_JOBS} jobs per producer, one in {BLOCK_EVERY} sleeps {BLOCK_FOR:?}; wall time, median of \
         {ROUNDS}"
    );
    header("ms");
    for (producers, workers) in BLOCKING_SHAPES {
        let load = Load { producers, workers, per_producer: BLOCKING_JOBS, block_every: BLOCK_EVERY };
        print!("{producers:>4} {workers:>4}");
        for kind in KINDS {
            let secs = median((0..ROUNDS).map(|_| kind.run(load)).collect());
            print!(" {:>12.1}", secs * 1e3);
        }
        let ideal = (load.total() / BLOCK_EVERY) as f64 * BLOCK_FOR.as_secs_f64() / workers as f64;
        println!("   ideal {:.1}", ideal * 1e3);
    }
}

// ==== Parking pool ====

/// How a producer tells the pool about a job it pushed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Notify {
    Rmw,
    Fence,
}

/// The parking contenders.
#[derive(Clone, Copy)]
enum Parking {
    Treiber(Notify),
    Channel,
}

const PARKING_KINDS: [Parking; 3] = [Parking::Treiber(Notify::Rmw), Parking::Treiber(Notify::Fence), Parking::Channel];

impl Parking {
    fn name(self) -> &'static str {
        // BRANCH: one name per contender
        match self {
            Parking::Treiber(Notify::Rmw) => "rmw",
            Parking::Treiber(Notify::Fence) => "fence",
            Parking::Channel => "cb channel",
        }
    }
}

/// A job that remembers when it was pushed (nanoseconds since the run's start; 0 when not measured).
struct TimedJob {
    next: AtomicPtr<TimedJob>,
    pushed: u64,
}

impl TreiberNode for TimedJob {
    type Value = u64;

    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &u64 {
        &self.pushed
    }
}

// Safety: plain data, owned by whoever holds the box.
unsafe impl Send for TimedJob {}

/// The pool's sleep state: how many workers are announced asleep (the state word of the handshake) and who they are.
#[derive(Default)]
struct Pool {
    idle: CachePadded<AtomicUsize>,
    sleepers: Mutex<Vec<Thread>>,
}

impl Pool {
    /// Producer side, after each push.
    #[inline]
    fn notify(&self, how: Notify) {
        // BRANCH: the handshake under test
        let idle = match how {
            Notify::Rmw => self.idle.fetch_add(0, Ordering::SeqCst),
            Notify::Fence => {
                fence(Ordering::SeqCst);
                self.idle.load(Ordering::Relaxed)
            }
        };
        // BRANCH: a worker is (about to be) asleep
        if idle > 0 {
            self.wake_one();
        }
    }

    #[cold]
    fn wake_one(&self) {
        let sleeper = self.sleepers.lock().unwrap().pop();
        // BRANCH: another producer woke the last sleeper first
        if let Some(sleeper) = sleeper {
            self.idle.fetch_sub(1, Ordering::Relaxed);
            sleeper.unpark();
        }
    }

    fn wake_all(&self) {
        let sleepers: Vec<Thread> = self.sleepers.lock().unwrap().drain(..).collect();
        self.idle.fetch_sub(sleepers.len(), Ordering::Relaxed);
        sleepers.iter().for_each(Thread::unpark);
    }

    /// Worker side, before the last `has_work`: from here on a producer sees the worker or the worker sees the job.
    fn announce(&self, how: Notify) {
        let mut sleepers = self.sleepers.lock().unwrap();
        sleepers.push(thread::current());
        self.idle.fetch_add(1, Ordering::SeqCst);
        drop(sleepers);
        // BRANCH: the fence variant pairs fences; the RMW variant pairs the two read-modify-writes
        if how == Notify::Fence {
            fence(Ordering::SeqCst);
        }
    }

    /// Takes the calling worker off the list unless a producer already did (and unparked it).
    fn withdraw(&self) {
        let me = thread::current().id();
        let mut sleepers = self.sleepers.lock().unwrap();
        // BRANCH: still listed: nobody woke it (it found work, timed out or is leaving)
        if let Some(index) = sleepers.iter().position(|t| t.id() == me) {
            sleepers.swap_remove(index);
            self.idle.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Frees a job taken by a worker and records its push-to-run latency if it was stamped.
fn take_timed(job: NonNull<TimedJob>, start: Instant, latencies: &mut Vec<u64>) {
    // Safety: the job is ours now.
    let job = unsafe { Box::from_raw(job.as_ptr()) };
    // BRANCH: stamped (bursty load)
    if job.pushed != 0 {
        latencies.push(start.elapsed().as_nanos() as u64 - job.pushed);
    }
}

/// Pushes a producer's jobs: all at once (`bursty` false) or in bursts with a gap, stamping each job.
fn produce(jobs: u64, bursty: bool, start: Instant, mut push: impl FnMut(Box<TimedJob>)) {
    let (bursts, burst) = if bursty { (BURSTS, BURST) } else { (1, jobs) };
    for _ in 0..bursts {
        for _ in 0..burst {
            let pushed = if bursty { start.elapsed().as_nanos() as u64 } else { 0 };
            push(Box::new(TimedJob { next: AtomicPtr::default(), pushed }));
        }
        // BRANCH: let the workers run dry and park
        if bursty {
            let until = Instant::now() + BURST_GAP;
            while Instant::now() < until {
                std::hint::spin_loop();
            }
        }
    }
}

/// One run through a parking pool. Returns (seconds, sorted push-to-run latencies in nanoseconds).
fn parking_run(kind: Parking, producers: usize, workers: usize, bursty: bool) -> (f64, Vec<u64>) {
    within_limit(move || {
        let per = if bursty { BURSTS * BURST } else { THROUGHPUT_JOBS };
        let total = per * producers as u64;
        let taken = Arc::new(AtomicU64::new(0));
        let start = Instant::now();
        let latencies: Vec<Vec<u64>> = match kind {
            Parking::Treiber(how) => {
                let queue = GkTreiberMpmc::<TimedJob>::new();
                let rescue_after = queue.config().rescue_after;
                let pool = Arc::new(Pool::default());
                let done = Arc::new(AtomicBool::new(false));
                let lanes: Vec<_> = (0..producers).map(|_| queue.lane().unwrap()).collect();
                let worker_handles: Vec<_> = (0..workers)
                    .map(|_| {
                        let (queue, pool, done, taken) = (queue.clone(), pool.clone(), done.clone(), taken.clone());
                        thread::spawn(move || {
                            let mut latencies = Vec::new();
                            let backoff = Backoff::new();
                            loop {
                                // BRANCH: a chain to run
                                if let Some(chain) = queue.try_reap() {
                                    for job in chain {
                                        take_timed(job, start, &mut latencies);
                                        taken.fetch_add(1, Ordering::Relaxed);
                                    }
                                    backoff.reset();
                                    continue;
                                }
                                // BRANCH: spin and yield a little before parking
                                if !backoff.is_completed() {
                                    backoff.snooze();
                                    continue;
                                }
                                pool.announce(how);
                                // BRANCH: a job arrived meanwhile
                                if queue.has_work() {
                                    pool.withdraw();
                                    backoff.reset();
                                    continue;
                                }
                                let stalled = queue.shared_chains() > 0;
                                // BRANCH: the producers are finished and nothing is left
                                if done.load(Ordering::Acquire) && !stalled {
                                    pool.withdraw();
                                    return latencies;
                                }
                                // BRANCH: a shared chain may stall, so look again within `rescue_after`
                                if stalled {
                                    thread::park_timeout(rescue_after);
                                } else {
                                    thread::park();
                                }
                                pool.withdraw();
                                backoff.reset();
                            }
                        })
                    })
                    .collect();
                let producer_handles: Vec<_> = lanes
                    .into_iter()
                    .map(|mut lane| {
                        let pool = pool.clone();
                        thread::spawn(move || {
                            produce(per, bursty, start, |job| {
                                // Safety: a fresh job in no other queue.
                                unsafe { lane.push(NonNull::from(Box::leak(job))) };
                                pool.notify(how);
                            })
                        })
                    })
                    .collect();
                for handle in producer_handles {
                    handle.join().unwrap();
                }
                done.store(true, Ordering::Release);
                pool.wake_all();
                worker_handles.into_iter().map(|h| h.join().unwrap()).collect()
            }
            Parking::Channel => {
                let (tx, rx) = crossbeam_channel::unbounded::<Box<TimedJob>>();
                let worker_handles: Vec<_> = (0..workers)
                    .map(|_| {
                        let (rx, taken) = (rx.clone(), taken.clone());
                        thread::spawn(move || {
                            let mut latencies = Vec::new();
                            while let Ok(job) = rx.recv() {
                                take_timed(NonNull::from(Box::leak(job)), start, &mut latencies);
                                taken.fetch_add(1, Ordering::Relaxed);
                            }
                            latencies
                        })
                    })
                    .collect();
                let producer_handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || produce(per, bursty, start, |job| tx.send(job).unwrap()))
                    })
                    .collect();
                drop(tx);
                for handle in producer_handles {
                    handle.join().unwrap();
                }
                worker_handles.into_iter().map(|h| h.join().unwrap()).collect()
            }
        };
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(taken.load(Ordering::Relaxed), total);
        let mut latencies: Vec<u64> = latencies.into_iter().flatten().collect();
        latencies.sort_unstable();
        (secs, latencies)
    })
}

fn percentile_us(sorted: &[u64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p) as usize] as f64 / 1e3
}

#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn bench_mpmc_parking() {
    println!("\nparking pool, continuous producers, empty jobs: millions of jobs per second, median of {ROUNDS}");
    print!("{:>4} {:>4}", "P", "C");
    for kind in PARKING_KINDS {
        print!(" {:>12}", kind.name());
    }
    println!();
    for (producers, workers) in PARKING_SHAPES {
        print!("{producers:>4} {workers:>4}");
        for kind in PARKING_KINDS {
            let secs = median((0..ROUNDS).map(|_| parking_run(kind, producers, workers, false).0).collect());
            print!(" {:>12.1}", (THROUGHPUT_JOBS * producers as u64) as f64 / secs / 1e6);
        }
        println!();
    }
    println!(
        "\nparking pool, bursty producers ({BURSTS} bursts of {BURST}, {BURST_GAP:?} apart): push-to-run latency p50 / p99 in \
         microseconds, run with the median p50 of {ROUNDS}"
    );
    print!("{:>4} {:>4}", "P", "C");
    for kind in PARKING_KINDS {
        print!(" {:>16}", kind.name());
    }
    println!();
    for (producers, workers) in PARKING_SHAPES {
        print!("{producers:>4} {workers:>4}");
        for kind in PARKING_KINDS {
            let mut runs: Vec<Vec<u64>> = (0..ROUNDS).map(|_| parking_run(kind, producers, workers, true).1).collect();
            runs.sort_by_key(|run| run[run.len() / 2]);
            let run = &runs[ROUNDS / 2];
            print!(" {:>16}", format!("{:.1} / {:.1}", percentile_us(run, 0.5), percentile_us(run, 0.99)));
        }
        println!();
    }
}

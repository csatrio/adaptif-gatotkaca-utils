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
//! Median of `ROUNDS` runs, each bounded by `RUN_LIMIT`.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::{GkTreiberMpmc, GkTreiberMpmcConfig, TreiberNode};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};

use std::{
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, Ordering},
        mpsc,
    },
    thread,
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

const KINDS: [Kind; 5] = [
    Kind::Treiber,
    Kind::ReapOnly,
    Kind::Channel,
    Kind::Injector,
    Kind::Deque,
];

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
    Box::new(Job {
        next: AtomicPtr::default(),
        block: block_every != 0 && id % block_every == 0,
    })
}

/// Runs a job: sleeps if it is a blocking one, then frees it.
#[allow(
    clippy::boxed_local,
    reason = "each job is its own allocation, as in a pool"
)]
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

#[allow(
    clippy::vec_box,
    reason = "each job is its own allocation, as in a pool"
)]
fn batches(load: Load) -> Vec<Vec<Box<Job>>> {
    (0..load.producers)
        .map(|_| {
            (0..load.per_producer)
                .map(|id| boxed(id, load.block_every))
                .collect()
        })
        .collect()
}

// ==== Runs ====

/// Seconds through a `GkTreiberMpmc` with `config`.
fn treiber(load: Load, config: GkTreiberMpmcConfig) -> f64 {
    within_limit(move || {
        let queue = GkTreiberMpmc::<Job>::with_config(config).unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let jobs: Vec<_> = batches(load)
            .into_iter()
            .map(|jobs| {
                Jobs(
                    jobs.into_iter()
                        .map(|job| NonNull::from(Box::leak(job)))
                        .collect(),
                )
            })
            .collect();
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
                        } else if done.load(Ordering::Acquire)
                            && !queue.has_work()
                            && queue.shared_chains() == 0
                        {
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
        let stealers: Arc<Vec<Stealer<Box<Job>>>> =
            Arc::new(locals.iter().map(Worker::stealer).collect());
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
                                if done.load(Ordering::Acquire)
                                    && injector.is_empty()
                                    && local.is_empty()
                                {
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
            let load = Load {
                producers,
                workers,
                per_producer: THROUGHPUT_JOBS,
                block_every: 0,
            };
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
        let load = Load {
            producers,
            workers,
            per_producer: BLOCKING_JOBS,
            block_every: BLOCK_EVERY,
        };
        print!("{producers:>4} {workers:>4}");
        for kind in KINDS {
            let secs = median((0..ROUNDS).map(|_| kind.run(load)).collect());
            print!(" {:>12.1}", secs * 1e3);
        }
        let ideal = (load.total() / BLOCK_EVERY) as f64 * BLOCK_FOR.as_secs_f64() / workers as f64;
        println!("   ideal {:.1}", ideal * 1e3);
    }
}

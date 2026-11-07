//! GkTreiberMpmc against crossbeam's MPMC channel and crossbeam-deque (measurement, `--ignored`):
//! `cargo test --release --test bench_treiber_mpmc -- --ignored --nocapture`
//!
//! P producer threads each queue `PER_PRODUCER` boxed jobs (allocated before the clock starts); C worker threads take
//! them and free them. Contenders:
//!
//! - `treiber`: `GkTreiberMpmc`, workers spin on `try_reap` and run each reaped lane chain themselves (the pool
//!   decides parking, not the queue).
//! - `cb channel`: `crossbeam_channel::unbounded`, workers use `recv` (spins before it parks).
//! - `cb injector`: `crossbeam_deque::Injector`, workers spin on `steal`, one job at a time: a shared FIFO, no
//!   per-worker state.
//! - `cb deque`: tokio's model, the injector plus a FIFO `Worker` deque per worker, filled with `steal_batch_and_pop`
//!   and stolen from by the others. Like `treiber`, workers take jobs in batches.
//!
//! Median of `ROUNDS` runs, each bounded by `RUN_LIMIT`.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::{GkTreiberMpmc, TreiberNode};
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

const PER_PRODUCER: u64 = 500_000;
const PRODUCER_COUNTS: [usize; 3] = [1, 2, 4];
const WORKER_COUNTS: [usize; 3] = [2, 4, 8];
const ROUNDS: usize = 5;
const RUN_LIMIT: Duration = Duration::from_secs(120);

// ==== Helpers ====

struct Job {
    next: AtomicPtr<Job>,
    id: u64,
}

impl TreiberNode for Job {
    type Value = u64;

    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    fn value(&self) -> &u64 {
        &self.id
    }
}

fn boxed(id: u64) -> Box<Job> {
    Box::new(Job {
        next: AtomicPtr::default(),
        id,
    })
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

// ==== Runs ====

/// Jobs per second through a `GkTreiberMpmc`.
fn treiber(producers: usize, workers: usize) -> f64 {
    within_limit(move || {
        let queue = GkTreiberMpmc::<Job>::new();
        let done = Arc::new(AtomicBool::new(false));
        let batches: Vec<_> = (0..producers)
            .map(|_| {
                Jobs(
                    (0..PER_PRODUCER)
                        .map(|id| NonNull::from(Box::leak(boxed(id))))
                        .collect(),
                )
            })
            .collect();
        let lanes: Vec<_> = (0..producers).map(|_| queue.lane().unwrap()).collect();
        let start = Instant::now();
        let producer_handles: Vec<_> = lanes
            .into_iter()
            .zip(batches)
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
        let worker_handles: Vec<_> = (0..workers)
            .map(|_| {
                let (queue, done) = (queue.clone(), done.clone());
                thread::spawn(move || {
                    let mut taken = 0u64;
                    loop {
                        // BRANCH: a whole lane, run here; otherwise stop once the producers are finished and
                        // nothing is left
                        if let Some(chain) = queue.try_reap() {
                            for job in chain {
                                // Safety: the job is ours now.
                                drop(unsafe { Box::from_raw(job) });
                                taken += 1;
                            }
                        } else if done.load(Ordering::Acquire) && !queue.has_work() {
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
        assert_eq!(taken, PER_PRODUCER * producers as u64);
        taken as f64 / secs
    })
}

/// Jobs per second through crossbeam's unbounded channel.
fn crossbeam(producers: usize, workers: usize) -> f64 {
    within_limit(move || {
        let (tx, rx) = crossbeam_channel::unbounded::<Box<Job>>();
        let batches: Vec<Vec<Box<Job>>> = (0..producers)
            .map(|_| (0..PER_PRODUCER).map(boxed).collect())
            .collect();
        let start = Instant::now();
        let producer_handles: Vec<_> = batches
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
        let worker_handles: Vec<_> = (0..workers)
            .map(|_| {
                let rx = rx.clone();
                thread::spawn(move || {
                    let mut taken = 0u64;
                    while let Ok(job) = rx.recv() {
                        drop(job);
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
        assert_eq!(taken, PER_PRODUCER * producers as u64);
        taken as f64 / secs
    })
}

/// Jobs per second through crossbeam-deque: the injector alone (`local_deques` false, one job per steal), or the
/// injector plus per-worker deques filled in batches (`local_deques` true, tokio's model).
fn deque(producers: usize, workers: usize, local_deques: bool) -> f64 {
    within_limit(move || {
        let injector = Arc::new(Injector::<Box<Job>>::new());
        let done = Arc::new(AtomicBool::new(false));
        let batches: Vec<Vec<Box<Job>>> = (0..producers)
            .map(|_| (0..PER_PRODUCER).map(boxed).collect())
            .collect();
        let locals: Vec<Worker<Box<Job>>> = (0..workers).map(|_| Worker::new_fifo()).collect();
        let stealers: Arc<Vec<Stealer<Box<Job>>>> =
            Arc::new(locals.iter().map(Worker::stealer).collect());
        let start = Instant::now();
        let producer_handles: Vec<_> = batches
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
                                drop(job);
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
        assert_eq!(taken, PER_PRODUCER * producers as u64);
        taken as f64 / secs
    })
}

// ==== Report ====

#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn bench_mpmc_throughput() {
    println!("\njobs per second (millions), median of {ROUNDS}; P producers x C workers");
    println!(
        "{:>4} {:>4} {:>12} {:>12} {:>12} {:>12}",
        "P", "C", "treiber", "cb channel", "cb injector", "cb deque"
    );
    for producers in PRODUCER_COUNTS {
        for workers in WORKER_COUNTS {
            let t = median((0..ROUNDS).map(|_| treiber(producers, workers)).collect());
            let c = median((0..ROUNDS).map(|_| crossbeam(producers, workers)).collect());
            let i = median(
                (0..ROUNDS)
                    .map(|_| deque(producers, workers, false))
                    .collect(),
            );
            let d = median(
                (0..ROUNDS)
                    .map(|_| deque(producers, workers, true))
                    .collect(),
            );
            println!(
                "{producers:>4} {workers:>4} {:>12.1} {:>12.1} {:>12.1} {:>12.1}",
                t / 1e6,
                c / 1e6,
                i / 1e6,
                d / 1e6
            );
        }
    }
}

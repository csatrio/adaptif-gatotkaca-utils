//! GkTreiberChannel against crossbeam_channel and std::sync::mpsc (measurement, `--ignored`):
//! `cargo test --release --test bench_treiber_channel -- --ignored --nocapture`
//!
//! Contenders: treiber (grows past its slab), treiber bounded (4096 jobs per producer, never allocates),
//! crossbeam unbounded, crossbeam bounded(4096) (4096 in total), std mpsc (unbounded `channel()`).
//!
//! - throughput: P producer threads send `PER_PRODUCER` u64 messages each to one consumer that drains until disconnect;
//! - ping-pong: one client and one echo server over two channels of the same kind, latency of a round trip.
//!
//! Median of `ROUNDS` runs. Every run is bounded by `RUN_LIMIT`.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::treiber_channel::{GkTreiberChannel, GkTreiberChannelConfig};

use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

// ==== Parameters ====

const PER_PRODUCER: u64 = 500_000;
const PRODUCER_COUNTS: [usize; 6] = [1, 2, 3, 4, 5, 6];
const ROUNDS: usize = 5;
const PING_PONGS: usize = 30_000;
const BOUND: usize = 4096;
const RUN_LIMIT: Duration = Duration::from_secs(120);

/// The channels under test.
#[derive(Clone, Copy)]
enum Kind {
    Treiber,
    /// `single_producer` (1:1); only measured with one producer.
    TreiberSingle,
    TreiberBounded,
    /// `lifo`: no reverse, no per-producer order.
    TreiberLifo,
    CrossbeamUnbounded,
    CrossbeamBounded,
    StdMpsc,
}

const KINDS: [Kind; 7] = [
    Kind::Treiber,
    Kind::TreiberSingle,
    Kind::TreiberBounded,
    Kind::TreiberLifo,
    Kind::CrossbeamUnbounded,
    Kind::CrossbeamBounded,
    Kind::StdMpsc,
];

impl Kind {
    fn name(self) -> &'static str {
        // BRANCH: one name per contender
        match self {
            Kind::Treiber => "treiber",
            Kind::TreiberSingle => "treiber 1:1",
            Kind::TreiberBounded => "treiber b4096",
            Kind::TreiberLifo => "treiber lifo",
            Kind::CrossbeamUnbounded => "cb unbounded",
            Kind::CrossbeamBounded => "cb b4096",
            Kind::StdMpsc => "std mpsc",
        }
    }
}

// ==== Helpers ====

/// Runs `f` on a thread; the benchmark fails instead of hanging if it takes longer than `RUN_LIMIT`.
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

fn percentile(sorted: &[u64], p: f64) -> u64 {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

/// A treiber channel of the given kind.
fn treiber(kind: Kind) -> GkTreiberChannel<u64> {
    // BRANCH: one config per treiber contender
    match kind {
        Kind::TreiberSingle => GkTreiberChannel::with_config(
            GkTreiberChannelConfig::DEFAULT.with_single_producer(true),
        )
        .unwrap(),
        Kind::TreiberBounded => GkTreiberChannel::new_bounded(),
        Kind::TreiberLifo => {
            GkTreiberChannel::with_config(GkTreiberChannelConfig::DEFAULT.with_lifo(true)).unwrap()
        }
        _ => GkTreiberChannel::new(),
    }
}

// ==== Throughput ====

/// Messages per second for `producers` producer threads and one consumer.
fn throughput(kind: Kind, producers: usize) -> f64 {
    within_limit(move || {
        let total = PER_PRODUCER * producers as u64;
        // BRANCH: one setup per contender; each starts its producers, drains on this thread and returns the elapsed time
        let (received, secs) = match kind {
            Kind::Treiber | Kind::TreiberSingle | Kind::TreiberBounded | Kind::TreiberLifo => {
                let channel = treiber(kind);
                let mut receiver = channel.receiver().unwrap();
                let senders: Vec<_> = (0..producers).map(|_| channel.sender().unwrap()).collect();
                let start = Instant::now();
                let handles: Vec<_> = senders
                    .into_iter()
                    .map(|mut sender| {
                        thread::spawn(move || {
                            for i in 0..PER_PRODUCER {
                                sender.send(i).unwrap();
                            }
                        })
                    })
                    .collect();
                let mut received = 0u64;
                while receiver.recv().is_ok() {
                    received += 1;
                }
                let secs = start.elapsed().as_secs_f64();
                for handle in handles {
                    handle.join().unwrap();
                }
                (received, secs)
            }
            Kind::CrossbeamUnbounded | Kind::CrossbeamBounded => {
                let (tx, rx) = if matches!(kind, Kind::CrossbeamUnbounded) {
                    crossbeam_channel::unbounded::<u64>()
                } else {
                    crossbeam_channel::bounded::<u64>(BOUND)
                };
                let start = Instant::now();
                let handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..PER_PRODUCER {
                                tx.send(i).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);
                let mut received = 0u64;
                while rx.recv().is_ok() {
                    received += 1;
                }
                let secs = start.elapsed().as_secs_f64();
                for handle in handles {
                    handle.join().unwrap();
                }
                (received, secs)
            }
            Kind::StdMpsc => {
                let (tx, rx) = mpsc::channel::<u64>();
                let start = Instant::now();
                let handles: Vec<_> = (0..producers)
                    .map(|_| {
                        let tx = tx.clone();
                        thread::spawn(move || {
                            for i in 0..PER_PRODUCER {
                                tx.send(i).unwrap();
                            }
                        })
                    })
                    .collect();
                drop(tx);
                let mut received = 0u64;
                while rx.recv().is_ok() {
                    received += 1;
                }
                let secs = start.elapsed().as_secs_f64();
                for handle in handles {
                    handle.join().unwrap();
                }
                (received, secs)
            }
        };
        assert_eq!(received, total);
        received as f64 / secs
    })
}

// ==== Ping-pong ====

/// Sorted round-trip times in nanoseconds.
fn ping_pong(kind: Kind) -> Vec<u64> {
    within_limit(move || {
        let mut times = Vec::with_capacity(PING_PONGS);
        // BRANCH: one setup per contender; a client sends i, an echo server sends it back
        match kind {
            Kind::Treiber | Kind::TreiberSingle | Kind::TreiberBounded | Kind::TreiberLifo => {
                let (request, reply) = (treiber(kind), treiber(kind));
                let mut request_tx = request.sender().unwrap();
                let mut request_rx = request.receiver().unwrap();
                let mut reply_tx = reply.sender().unwrap();
                let mut reply_rx = reply.receiver().unwrap();
                let server = thread::spawn(move || {
                    while let Ok(job) = request_rx.recv() {
                        reply_tx.send(job).unwrap();
                    }
                });
                for i in 0..PING_PONGS as u64 {
                    let start = Instant::now();
                    request_tx.send(i).unwrap();
                    assert_eq!(reply_rx.recv().unwrap(), i);
                    times.push(start.elapsed().as_nanos() as u64);
                }
                drop(request_tx);
                server.join().unwrap();
            }
            Kind::CrossbeamUnbounded | Kind::CrossbeamBounded => {
                let make = || {
                    if matches!(kind, Kind::CrossbeamUnbounded) {
                        crossbeam_channel::unbounded::<u64>()
                    } else {
                        crossbeam_channel::bounded::<u64>(BOUND)
                    }
                };
                let ((request_tx, request_rx), (reply_tx, reply_rx)) = (make(), make());
                let server = thread::spawn(move || {
                    while let Ok(value) = request_rx.recv() {
                        reply_tx.send(value).unwrap();
                    }
                });
                for i in 0..PING_PONGS as u64 {
                    let start = Instant::now();
                    request_tx.send(i).unwrap();
                    assert_eq!(reply_rx.recv().unwrap(), i);
                    times.push(start.elapsed().as_nanos() as u64);
                }
                drop(request_tx);
                server.join().unwrap();
            }
            Kind::StdMpsc => {
                let (request_tx, request_rx) = mpsc::channel::<u64>();
                let (reply_tx, reply_rx) = mpsc::channel::<u64>();
                let server = thread::spawn(move || {
                    while let Ok(value) = request_rx.recv() {
                        reply_tx.send(value).unwrap();
                    }
                });
                for i in 0..PING_PONGS as u64 {
                    let start = Instant::now();
                    request_tx.send(i).unwrap();
                    assert_eq!(reply_rx.recv().unwrap(), i);
                    times.push(start.elapsed().as_nanos() as u64);
                }
                drop(request_tx);
                server.join().unwrap();
            }
        }
        times.sort_unstable();
        times
    })
}

// ==== Benchmark ====

#[ignore]
#[test]
fn treiber_channel_vs_the_rest() {
    println!(
        "\nthroughput, M msgs/s (median of {ROUNDS}; {PER_PRODUCER} u64 per producer, 1 consumer)"
    );
    print!("{:>9}", "producers");
    for kind in KINDS {
        print!(" {:>14}", kind.name());
    }
    println!(" {:>16}", "treiber b / cb b");
    for producers in PRODUCER_COUNTS {
        let results: Vec<f64> = KINDS
            .iter()
            .map(|&kind| {
                // BRANCH: the 1:1 channel has exactly one producer
                if matches!(kind, Kind::TreiberSingle) && producers != 1 {
                    return f64::NAN;
                }
                median((0..ROUNDS).map(|_| throughput(kind, producers)).collect())
            })
            .collect();
        print!("{producers:>9}");
        for result in &results {
            // BRANCH: a contender that does not apply to this row
            if result.is_nan() {
                print!(" {:>14}", "-");
            } else {
                print!(" {:>14.2}", result / 1e6);
            }
        }
        println!(" {:>15.2}x", results[2] / results[5]);
    }

    println!(
        "\nping-pong round trip, microseconds ({PING_PONGS} round trips, best of {ROUNDS} by p50)"
    );
    println!("{:>16} {:>9} {:>9} {:>9}", "", "p50", "p99", "max");
    for kind in KINDS {
        let runs: Vec<Vec<u64>> = (0..ROUNDS).map(|_| ping_pong(kind)).collect();
        let best = runs
            .into_iter()
            .min_by_key(|times| percentile(times, 0.5))
            .unwrap();
        println!(
            "{:>16} {:>9.1} {:>9.1} {:>9.1}",
            kind.name(),
            percentile(&best, 0.5) as f64 / 1e3,
            percentile(&best, 0.99) as f64 / 1e3,
            *best.last().unwrap() as f64 / 1e3
        );
    }
}

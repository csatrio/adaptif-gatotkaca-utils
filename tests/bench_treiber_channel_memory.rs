//! Heap memory of GkTreiberChannel against crossbeam_channel and std::sync::mpsc (measurement, `--ignored`):
//! `cargo test --release --test bench_treiber_channel_memory -- --ignored --nocapture`
//!
//! A counting global allocator tracks live heap bytes (so thread stacks are not included).
//! Own test binary: the allocator would slow down the speed benchmark.
//!
//! - idle: producers created, nothing sent;
//! - backlog: every producer sends `PER_PRODUCER` u64 before the consumer takes any (the worst case for a queue);
//! - drained: everything received, producers and receiver still alive (treiber: after `GkSender::shrink`);
//! - closed: everything dropped.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::treiber_channel::{GK_SLAB_NODES, GkTreiberChannel};

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicIsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

// ==== Counting allocator ====

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn add(bytes: isize) {
    let now = LIVE.fetch_add(bytes, Ordering::SeqCst) + bytes;
    PEAK.fetch_max(now, Ordering::SeqCst);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        add(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn live() -> isize {
    LIVE.load(Ordering::SeqCst)
}

// ==== Parameters ====

const PER_PRODUCER: u64 = 200_000;
const PRODUCER_COUNTS: [usize; 3] = [1, 4, 16];
const RUN_LIMIT: Duration = Duration::from_secs(120);

/// Heap bytes at each stage, relative to before the channel existed.
struct Stages {
    idle: isize,
    backlog: isize,
    drained: isize,
    closed: isize,
}

// ==== Scenarios ====

/// Messages queued at the backlog stage, and the heap at each stage.
struct Measured {
    messages: u64,
    stages: Stages,
}

fn treiber(producers: usize, bounded: bool) -> Measured {
    // A bounded channel can only back up to its bound: 4096 jobs per producer.
    let per_producer = if bounded {
        GK_SLAB_NODES as u64
    } else {
        PER_PRODUCER
    };
    let base = live();
    let channel = if bounded {
        GkTreiberChannel::<u64>::new_bounded()
    } else {
        GkTreiberChannel::<u64>::new()
    };
    let mut receiver = channel.receiver().unwrap();
    let senders: Vec<_> = (0..producers).map(|_| channel.sender().unwrap()).collect();
    let idle = live() - base;
    // Producers send everything, then hand their sender back so it stays alive.
    let handles: Vec<_> = senders
        .into_iter()
        .map(|mut sender| {
            thread::spawn(move || {
                for i in 0..per_producer {
                    sender.send(i).unwrap();
                }
                sender
            })
        })
        .collect();
    let senders: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let backlog = live() - base;
    for _ in 0..per_producer * producers as u64 {
        receiver.recv().unwrap();
    }
    // Senders keep what they grew into until `shrink` frees the chunks whose nodes are all home.
    let mut senders = senders;
    for sender in &mut senders {
        sender.shrink();
    }
    let drained = live() - base;
    drop(senders);
    drop(receiver);
    drop(channel);
    Measured {
        messages: per_producer * producers as u64,
        stages: Stages {
            idle,
            backlog,
            drained,
            closed: live() - base,
        },
    }
}

fn crossbeam(producers: usize, capacity: Option<usize>) -> Measured {
    // crossbeam's bound is for the whole channel, so the producers share it.
    let per_producer = match capacity {
        Some(n) => (n / producers) as u64,
        None => PER_PRODUCER,
    };
    let base = live();
    let (tx, rx) = match capacity {
        Some(n) => crossbeam_channel::bounded::<u64>(n),
        None => crossbeam_channel::unbounded::<u64>(),
    };
    let senders: Vec<_> = (0..producers).map(|_| tx.clone()).collect();
    let idle = live() - base;
    let handles: Vec<_> = senders
        .into_iter()
        .map(|sender| {
            thread::spawn(move || {
                for i in 0..per_producer {
                    sender.send(i).unwrap();
                }
                sender
            })
        })
        .collect();
    let senders: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let backlog = live() - base;
    for _ in 0..per_producer * producers as u64 {
        rx.recv().unwrap();
    }
    let drained = live() - base;
    drop(senders);
    drop(tx);
    drop(rx);
    Measured {
        messages: per_producer * producers as u64,
        stages: Stages {
            idle,
            backlog,
            drained,
            closed: live() - base,
        },
    }
}

fn std_mpsc(producers: usize) -> Measured {
    let base = live();
    let (tx, rx) = mpsc::channel::<u64>();
    let senders: Vec<_> = (0..producers).map(|_| tx.clone()).collect();
    let idle = live() - base;
    let handles: Vec<_> = senders
        .into_iter()
        .map(|sender| {
            thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    sender.send(i).unwrap();
                }
                sender
            })
        })
        .collect();
    let senders: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let backlog = live() - base;
    for _ in 0..PER_PRODUCER * producers as u64 {
        rx.recv().unwrap();
    }
    let drained = live() - base;
    drop(senders);
    drop(tx);
    drop(rx);
    Measured {
        messages: PER_PRODUCER * producers as u64,
        stages: Stages {
            idle,
            backlog,
            drained,
            closed: live() - base,
        },
    }
}

/// Runs a scenario on its own thread so a hang fails the benchmark instead of blocking it.
fn run(f: impl FnOnce() -> Measured + Send + 'static) -> Measured {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(RUN_LIMIT).expect("benchmark run timed out")
}

// ==== Benchmark ====

fn kib(bytes: isize) -> f64 {
    bytes as f64 / 1024.0
}

#[ignore]
#[test]
fn treiber_channel_memory_vs_the_rest() {
    // Warm-up so one-time allocations (thread machinery) are not charged to the first row.
    run(|| treiber(1, false));
    run(|| crossbeam(1, None));
    run(|| std_mpsc(1));
    PEAK.store(live(), Ordering::SeqCst);
    println!(
        "\nheap KiB (live bytes above the pre-channel baseline); unbounded channels queue {PER_PRODUCER} u64 per producer,\nbounded ones fill up to their bound ({GK_SLAB_NODES} per producer for treiber, {GK_SLAB_NODES} in total for crossbeam)"
    );
    println!(
        "{:>9} {:<14} {:>10} {:>10} {:>12} {:>10} {:>9} {:>14}",
        "producers", "channel", "queued", "idle", "backlog", "drained", "closed", "backlog B/msg"
    );
    for producers in PRODUCER_COUNTS {
        let rows: [(&str, Measured); 5] = [
            ("treiber", run(move || treiber(producers, false))),
            ("treiber b4096", run(move || treiber(producers, true))),
            ("cb unbounded", run(move || crossbeam(producers, None))),
            (
                "cb b4096",
                run(move || crossbeam(producers, Some(GK_SLAB_NODES))),
            ),
            ("std mpsc", run(move || std_mpsc(producers))),
        ];
        for (name, measured) in rows {
            let stages = measured.stages;
            println!(
                "{producers:>9} {name:<14} {:>10} {:>10.1} {:>12.1} {:>10.1} {:>9.1} {:>14.1}",
                measured.messages,
                kib(stages.idle),
                kib(stages.backlog),
                kib(stages.drained),
                kib(stages.closed),
                (stages.backlog - stages.idle) as f64 / measured.messages as f64
            );
        }
    }
}

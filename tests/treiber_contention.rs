//! Multi-threaded tests for the Treiber stack and queue.

use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode, TreiberQueue};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::thread;

struct Node {
    next: AtomicPtr<Node>,
    producer: u32,
    seq: u32,
}

impl TreiberNode for Node {
    type Value = (u32, u32);
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &(u32, u32) {
        // Not used by these tests.
        unreachable!()
    }
}

fn node(producer: u32, seq: u32) -> *mut Node {
    Box::into_raw(Box::new(Node {
        next: AtomicPtr::new(ptr::null_mut()),
        producer,
        seq,
    }))
}

/// Reaps, frees every node and returns (producer, seq) in yielded order.
fn drain(head: AtomicPtr<Node>) -> Vec<(u32, u32)> {
    head.iterator()
        .map(|p| {
            let n = unsafe { Box::from_raw(p) };
            (n.producer, n.seq)
        })
        .collect()
}

#[test]
fn many_producers_one_reaper_exactly_once_and_in_order() {
    const PRODUCERS: u32 = 8;
    // Debug builds allocate slowly, so use fewer nodes there.
    const PER_PRODUCER: u32 = if cfg!(debug_assertions) {
        100_000
    } else {
        1_000_000
    };

    let stack: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());
    let done = AtomicBool::new(false);
    let mut next_expected = vec![0u32; PRODUCERS as usize];

    thread::scope(|scope| {
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|id| {
                let stack = &stack;
                scope.spawn(move || {
                    for seq in 0..PER_PRODUCER {
                        stack.push_to_stack(node(id, seq));
                    }
                })
            })
            .collect();

        let reaper = scope.spawn(|| {
            let mut seen = vec![0u32; PRODUCERS as usize];
            loop {
                let finished = done.load(Ordering::Acquire);
                for (id, seq) in drain(stack.reap_all()) {
                    // Each node exactly once, and in each producer's push order.
                    assert_eq!(seq, seen[id as usize], "producer {id}");
                    seen[id as usize] += 1;
                }
                if finished {
                    return seen;
                }
            }
        });

        for p in producers {
            p.join().unwrap();
        }
        done.store(true, Ordering::Release);
        next_expected = reaper.join().unwrap();
    });

    assert!(next_expected.iter().all(|&n| n == PER_PRODUCER));
    assert!(stack.is_empty());
}

#[test]
fn push_chain_stays_contiguous_and_ordered() {
    const CHAIN_LEN: u32 = 64;
    const CHAIN_THREADS: u32 = 4;
    const CHAINS_PER_THREAD: u32 = 500;
    const SINGLE_THREADS: u32 = 4;
    const SINGLES_PER_THREAD: u32 = 20_000;

    let stack: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());

    thread::scope(|scope| {
        for t in 0..CHAIN_THREADS {
            let stack = &stack;
            scope.spawn(move || {
                for c in 0..CHAINS_PER_THREAD {
                    // producer = chain id, seq = position in the chain.
                    let chain_id = 1000 + t * CHAINS_PER_THREAD + c;
                    let nodes = (0..CHAIN_LEN).map(|i| node(chain_id, i));
                    let (first, last) = Node::link_chain(nodes).unwrap();
                    stack.push_chain(first, last);
                }
            });
        }
        for t in 0..SINGLE_THREADS {
            let stack = &stack;
            scope.spawn(move || {
                for i in 0..SINGLES_PER_THREAD {
                    stack.push_to_stack(node(t, i));
                }
            });
        }
    });

    let out = drain(stack.reap_all());
    let chains = CHAIN_THREADS * CHAINS_PER_THREAD;
    assert_eq!(
        out.len() as u32,
        chains * CHAIN_LEN + SINGLE_THREADS * SINGLES_PER_THREAD
    );

    // Every chain: 64 consecutive entries, seq 0..64 in order.
    let mut i = 0;
    let mut chains_seen = 0;
    let mut single_next = vec![0u32; SINGLE_THREADS as usize];
    while i < out.len() {
        let (id, seq) = out[i];
        if id >= 1000 {
            assert_eq!(seq, 0, "chain {id} must start at its oldest node");
            for k in 0..CHAIN_LEN {
                assert_eq!(out[i + k as usize], (id, k), "chain {id} not contiguous");
            }
            i += CHAIN_LEN as usize;
            chains_seen += 1;
        } else {
            assert_eq!(seq, single_next[id as usize]);
            single_next[id as usize] += 1;
            i += 1;
        }
    }
    assert_eq!(chains_seen, chains);
}

#[test]
fn link_chain_empty_and_single() {
    assert!(Node::link_chain(std::iter::empty()).is_none());
    let n = node(1, 1);
    let (first, last) = Node::link_chain([n]).unwrap();
    assert_eq!(first, n);
    assert_eq!(last, n);
    drop(unsafe { Box::from_raw(n) });
}

/// Pushes `total` nodes per thread into a limited queue; failed pushes keep
/// the node. Returns (reaped_ids, rejected_ids) for exact accounting.
fn run_queue(limit: usize, threads: u32, per_thread: u32, concurrent_reap: bool) {
    let queue = TreiberQueue::<Node>::new(limit);
    let done = AtomicBool::new(false);
    let mut reaped: Vec<(u32, u32)> = Vec::new();
    let mut rejected: Vec<(u32, u32)> = Vec::new();

    thread::scope(|scope| {
        let reaper = scope.spawn(|| {
            let mut got = Vec::new();
            if concurrent_reap {
                while !done.load(Ordering::Acquire) {
                    got.extend(drain(queue.reap_all()));
                }
            } else {
                while !done.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            }
            got.extend(drain(queue.reap_all()));
            got
        });

        let producers: Vec<_> = (0..threads)
            .map(|id| {
                let queue = &queue;
                scope.spawn(move || {
                    let mut mine = Vec::new();
                    for seq in 0..per_thread {
                        let n = node(id, seq);
                        if !queue.push(n) {
                            // The caller still owns a rejected node.
                            let n = unsafe { Box::from_raw(n) };
                            mine.push((n.producer, n.seq));
                        }
                    }
                    mine
                })
            })
            .collect();

        for p in producers {
            rejected.extend(p.join().unwrap());
        }
        done.store(true, Ordering::Release);
        reaped = reaper.join().unwrap();
    });

    // Every node was either reaped or rejected, never both, never lost.
    let mut all: Vec<_> = reaped.iter().chain(rejected.iter()).copied().collect();
    all.sort_unstable();
    let expected: Vec<_> = (0..threads)
        .flat_map(|id| (0..per_thread).map(move |seq| (id, seq)))
        .collect();
    assert_eq!(all, expected);

    if !concurrent_reap {
        // No reset happened, so the limit is a hard cap.
        assert!(reaped.len() <= limit, "{} > {limit}", reaped.len());
        assert!(!rejected.is_empty());
    }
}

#[test]
fn queue_limit_is_a_cap_without_reaping() {
    run_queue(1000, 8, 5000, false);
}

#[test]
fn queue_accounts_for_every_node_with_concurrent_reaping() {
    run_queue(1000, 8, 20_000, true);
}

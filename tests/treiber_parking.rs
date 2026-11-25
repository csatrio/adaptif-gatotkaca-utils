//! `GkParkingStack` / `GkParkingQueue` behaviour, plus the iterator and `TreiberQueue` additions of 0.16.
//!
//! Small enough for Miri (data races, leaks, use-after-free):
//! `cargo +nightly miri test --test treiber_parking`

use adaptif_gatotkaca_utils::{
    GkParkingQueue, GkParkingStack, GkQueueFull, GkTreiberPtr, GkTreiberStackIterator, TreiberNode, TreiberQueue, TreiberStackIterator,
};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

struct Node {
    next: AtomicPtr<Node>,
    // Written with plain stores before the push: Miri reports a race if the reap does not synchronise with it.
    value: (u64, u64),
    drops: Option<Arc<AtomicUsize>>,
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(d) = &self.drops {
            d.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl TreiberNode for Node {
    type Value = (u64, u64);
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &(u64, u64) {
        &self.value
    }
}

fn node(a: u64, b: u64) -> NonNull<Node> {
    NonNull::from(Box::leak(Box::new(Node { next: AtomicPtr::new(ptr::null_mut()), value: (a, b), drops: None })))
}

fn counted(v: u64, drops: &Arc<AtomicUsize>) -> NonNull<Node> {
    NonNull::from(Box::leak(Box::new(Node { next: AtomicPtr::new(ptr::null_mut()), value: (0, v), drops: Some(drops.clone()) })))
}

/// Reverses a reaped chain (push order) and frees it, returning the values.
fn take_fifo(head: NonNull<Node>) -> Vec<u64> {
    let (first, _, _) = Node::reverse_in_place(head.as_ptr());
    // Safety: the chain was reaped, the test owns it.
    unsafe { TreiberStackIterator::from_head(first) }.map(|n| unsafe { Box::from_raw(n) }.value.1).collect()
}

#[test]
fn starts_parked_and_wakes_once_per_sleep() {
    let s = GkParkingStack::<Node>::new();
    assert!(s.is_parked() && s.is_empty());
    unsafe {
        assert!(s.push(node(0, 1)), "first push must wake the parked consumer");
        assert!(!s.push(node(0, 2)), "consumer already being woken");
    }
    assert!(!s.is_parked() && !s.is_empty());
    assert_eq!(take_fifo(s.reap().unwrap()), [1, 2]);
    assert!(s.reap().is_none());
    assert!(s.try_park());
    assert!(s.is_parked());
    assert!(unsafe { s.push(node(0, 3)) }, "woken again after parking");
    assert_eq!(take_fifo(s.reap().unwrap()), [3]);
    assert!(s.try_park());
}

#[test]
fn park_fails_when_a_node_raced_in() {
    let s = GkParkingStack::<Node>::new();
    assert!(s.reap().is_none(), "a parked stack reaps nothing");
    // The reap woke the consumer: pushes no longer ask for a wake-up.
    assert!(!unsafe { s.push(node(0, 7)) });
    assert!(!s.try_park(), "a node is waiting");
    assert_eq!(take_fifo(s.reap().unwrap()), [7]);
    assert!(s.try_park());
}

#[test]
fn push_chain_keeps_order_and_reports_wake() {
    let s = GkParkingStack::<Node>::new();
    let (first, last) = Node::link_chain([1, 2, 3].map(|v| node(0, v).as_ptr())).unwrap();
    assert!(unsafe { s.push_chain(NonNull::new(first).unwrap(), NonNull::new(last).unwrap()) });
    assert!(!unsafe { s.push(node(0, 4)) });
    assert_eq!(take_fifo(s.reap().unwrap()), [1, 2, 3, 4]);
}

#[test]
fn drop_frees_queued_nodes() {
    let drops = Arc::new(AtomicUsize::new(0));
    {
        let s = GkParkingStack::<Node>::new();
        unsafe {
            let _ = s.push(counted(1, &drops));
            let _ = s.push(counted(2, &drops));
        }
    }
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    // A parked (empty) stack drops nothing.
    drop(GkParkingStack::<Node>::new());
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn queue_admits_up_to_limit_until_released() {
    let q = GkParkingQueue::<Node>::new(2);
    assert_eq!(q.limit(), 2);
    unsafe {
        assert_eq!(q.try_push(node(0, 1)), Ok(true));
        assert_eq!(q.try_push(node(0, 2)), Ok(false));
        let refused = node(0, 3);
        assert_eq!(q.try_push(refused), Err(GkQueueFull));
        assert!(q.is_full());
        // Reaping does not free capacity; releasing does.
        let got = take_fifo(q.reap().unwrap());
        assert_eq!(got, [1, 2]);
        assert_eq!(q.try_push(refused), Err(GkQueueFull));
        q.release(1);
        assert_eq!(q.try_push(refused), Ok(false), "consumer awake: no wake-up");
        q.release(1);
        assert_eq!(q.len(), 1);
        assert_eq!(take_fifo(q.reap().unwrap()), [3]);
        q.release(1);
    }
    assert!(q.is_empty());
    assert!(q.try_park() && q.is_parked());
}

#[test]
fn iterator_into_remaining_resumes_where_it_stopped() {
    let head: AtomicPtr<Node> = GkTreiberPtr::new();
    for v in 1..=5 {
        head.push_to_stack(node(0, v).as_ptr());
    }
    let mut it = head.reap_all().iterator(); // oldest first
    assert_eq!(it.peek().map(|n| unsafe { n.as_ref() }.value.1), Some(1));
    let first_two: Vec<u64> = it.by_ref().take(2).map(|n| unsafe { Box::from_raw(n) }.value.1).collect();
    assert_eq!(first_two, [1, 2]);
    let rest = it.into_remaining(); // nothing freed
    let resumed: Vec<u64> = unsafe { TreiberStackIterator::from_head(rest) }.map(|n| unsafe { Box::from_raw(n) }.value.1).collect();
    assert_eq!(resumed, [3, 4, 5]);
    let empty = unsafe { TreiberStackIterator::<Node>::from_head(ptr::null_mut()) };
    assert!(empty.peek().is_none() && empty.into_remaining().is_null());
}

#[test]
fn treiber_queue_refuses_when_full_without_touching_the_count() {
    let q = TreiberQueue::<Node>::new(1);
    let a = node(0, 1).as_ptr();
    let b = node(0, 2).as_ptr();
    assert!(q.push(a));
    assert!(!q.push(b));
    assert_eq!(q.len(), 1);
    let chain = q.reap_all();
    for n in chain.iterator() {
        drop(unsafe { Box::from_raw(n) });
    }
    drop(unsafe { Box::from_raw(b) });
}

/// Producers push while one consumer drains, clears its doorbell, parks and sleeps. A lost wake-up leaves the
/// consumer asleep with items queued: the timed wait fails the test. Also checks per-producer FIFO and that every
/// payload arrives exactly once.
#[test]
fn no_lost_wakeup_under_contention() {
    const PRODUCERS: u64 = if cfg!(miri) { 3 } else { 6 };
    const PER: u64 = if cfg!(miri) { 25 } else { 50_000 };
    let stack = Arc::new(GkParkingStack::<Node>::new());
    // The doorbell: a counting channel, like an eventfd. `try_recv` until empty is "clear".
    let (bell_tx, bell_rx) = mpsc::channel::<()>();
    let wakes = Arc::new(AtomicUsize::new(0));

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let (stack, bell_tx, wakes) = (stack.clone(), bell_tx.clone(), wakes.clone());
            thread::spawn(move || {
                for seq in 0..PER {
                    if unsafe { stack.push(node(p, seq)) } {
                        wakes.fetch_add(1, Ordering::Relaxed);
                        let _ = bell_tx.send(());
                    }
                    if seq % 64 == 0 {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();
    drop(bell_tx);

    let mut next_seq = vec![0u64; PRODUCERS as usize];
    let mut received = 0u64;
    let mut sleeps = 0u64;
    while received < PRODUCERS * PER {
        while let Some(head) = stack.reap() {
            let (first, _, _) = Node::reverse_in_place(head.as_ptr());
            for n in unsafe { TreiberStackIterator::from_head(first) } {
                let (p, seq) = unsafe { Box::from_raw(n) }.value;
                assert_eq!(seq, next_seq[p as usize], "producer {p} out of order");
                next_seq[p as usize] += 1;
                received += 1;
            }
        }
        if received == PRODUCERS * PER {
            break;
        }
        while bell_rx.try_recv().is_ok() {} // clear BEFORE parking
        if stack.try_park() {
            sleeps += 1;
            bell_rx.recv_timeout(Duration::from_secs(20)).expect("lost wake-up: consumer parked with no notification coming");
        }
    }
    for p in producers {
        p.join().unwrap();
    }
    assert!(stack.reap().is_none());
    // Every successful park is followed by exactly one waking push. The initial parked state is ended either by a
    // waking push or by the consumer's first reap.
    let wakes = wakes.load(Ordering::Relaxed) as u64;
    assert!(wakes == sleeps || wakes == sleeps + 1, "wakes {wakes}, sleeps {sleeps}");
}

/// Producers retry refused pushes; the consumer releases capacity as it drains. Nothing is lost or duplicated and
/// the count never goes beyond `limit` + producers.
#[test]
fn bounded_queue_under_contention() {
    const PRODUCERS: u64 = if cfg!(miri) { 2 } else { 4 };
    const PER: u64 = if cfg!(miri) { 20 } else { 20_000 };
    const LIMIT: usize = 8;
    let q = Arc::new(GkParkingQueue::<Node>::new(LIMIT));
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let q = q.clone();
            thread::spawn(move || {
                for seq in 0..PER {
                    let n = node(p, seq);
                    loop {
                        match unsafe { q.try_push(n) } {
                            Ok(_) => break,
                            Err(GkQueueFull) => thread::yield_now(),
                        }
                    }
                }
            })
        })
        .collect();
    let mut next_seq = vec![0u64; PRODUCERS as usize];
    let mut received = 0u64;
    while received < PRODUCERS * PER {
        assert!(q.len() <= LIMIT + PRODUCERS as usize);
        match q.reap() {
            Some(head) => {
                let (first, _, count) = Node::reverse_in_place(head.as_ptr());
                for n in unsafe { TreiberStackIterator::from_head(first) } {
                    let (p, seq) = unsafe { Box::from_raw(n) }.value;
                    assert_eq!(seq, next_seq[p as usize]);
                    next_seq[p as usize] += 1;
                }
                received += count as u64;
                q.release(count);
            }
            None => thread::yield_now(),
        }
    }
    for p in producers {
        p.join().unwrap();
    }
    assert!(q.is_empty());
}

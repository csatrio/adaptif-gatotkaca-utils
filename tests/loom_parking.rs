//! Loom models of the `GkParkingStack` / `GkParkingQueue` protocol: every interleaving of push, reap, signal clear,
//! park and wake-up is explored. A lost wake-up leaves the consumer waiting forever, which loom reports as a deadlock;
//! a missing happens-before on a payload is reported as a data race on its `loom::cell::UnsafeCell`.
//!
//! ```sh
//! RUSTFLAGS="--cfg gk_loom" cargo test --release --test loom_parking
//! ```
#![cfg(gk_loom)]

use adaptif_gatotkaca_utils::{GkParkingQueue, GkParkingStack, GkQueueFull, TreiberNode, TreiberStackIterator};
use loom::cell::UnsafeCell;
use loom::sync::atomic::AtomicUsize;
use loom::sync::{Arc, Condvar, Mutex};
use loom::thread;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

struct Node {
    next: AtomicPtr<Node>,
    value: UnsafeCell<u64>,
}

impl TreiberNode for Node {
    type Value = UnsafeCell<u64>;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &UnsafeCell<u64> {
        &self.value
    }
}

fn node(v: u64) -> NonNull<Node> {
    let n = Box::new(Node { next: AtomicPtr::new(ptr::null_mut()), value: UnsafeCell::new(0) });
    // The payload is written after allocation, like a worker filling a completion slot.
    n.value.with_mut(|p| unsafe { *p = v });
    NonNull::from(Box::leak(n))
}

/// The eventfd stand-in: a counter that `clear` resets and `wait` blocks on (linearizable, like an eventfd).
#[derive(Default)]
struct Bell {
    count: Mutex<u64>,
    cv: Condvar,
}

impl Bell {
    fn notify(&self) {
        *self.count.lock().unwrap() += 1;
        self.cv.notify_one();
    }
    fn clear(&self) {
        *self.count.lock().unwrap() = 0;
    }
    fn wait(&self) {
        let mut g = self.count.lock().unwrap();
        while *g == 0 {
            g = self.cv.wait(g).unwrap();
        }
    }
}

/// Drains everything currently queued; returns the payload sum and count.
fn drain(stack: &GkParkingStack<Node>) -> (u64, u64) {
    let (mut sum, mut n) = (0, 0);
    while let Some(head) = stack.reap() {
        let (first, _, _) = Node::reverse_in_place(head.as_ptr());
        for p in unsafe { TreiberStackIterator::from_head(first) } {
            let b = unsafe { Box::from_raw(p) };
            sum += b.value.with(|v| unsafe { *v });
            n += 1;
        }
    }
    (sum, n)
}

fn builder() -> loom::model::Builder {
    let mut b = loom::model::Builder::new();
    if b.preemption_bound.is_none() {
        b.preemption_bound = Some(3);
    }
    b
}

/// Two producers (one single push, one two-node chain) and a consumer that sleeps between drains.
fn model(clear_before_park: bool) {
    builder().check(move || {
        let stack = Arc::new(GkParkingStack::<Node>::new());
        let bell = Arc::new(Bell::default());

        let p1 = {
            let (stack, bell) = (stack.clone(), bell.clone());
            thread::spawn(move || {
                if unsafe { stack.push(node(1)) } {
                    bell.notify();
                }
            })
        };
        let p2 = {
            let (stack, bell) = (stack.clone(), bell.clone());
            thread::spawn(move || {
                let (first, last) = Node::link_chain([node(10).as_ptr(), node(100).as_ptr()]).unwrap();
                if unsafe { stack.push_chain(NonNull::new(first).unwrap(), NonNull::new(last).unwrap()) } {
                    bell.notify();
                }
            })
        };

        let (mut sum, mut got) = (0, 0);
        loop {
            let (s, n) = drain(&stack);
            sum += s;
            got += n;
            if got == 3 {
                break;
            }
            if clear_before_park {
                bell.clear();
                if stack.try_park() {
                    bell.wait();
                }
            } else if stack.try_park() {
                // WRONG order, kept to prove the model catches it.
                bell.clear();
                bell.wait();
            }
        }
        assert_eq!(sum, 111);
        p1.join().unwrap();
        p2.join().unwrap();
    });
}

#[test]
fn no_lost_wakeup_and_payloads_visible() {
    model(true);
}

/// Clearing the signal after a successful park can swallow the wake-up of a push that landed in between. Loom must
/// find that deadlock, which shows the model above is able to fail. A loom deadlock aborts the process while
/// unwinding, so the faulty model runs in a child process.
#[test]
fn clearing_after_park_loses_a_wakeup() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "wrong_order_model", "--nocapture", "--test-threads=1"])
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(!out.status.success(), "the faulty model passed: the loom model cannot detect a lost wake-up");
    assert!(text.contains("deadlock"), "expected a loom deadlock, got:\n{text}");
}

#[test]
#[ignore = "faulty on purpose; run by `clearing_after_park_loses_a_wakeup` in a child process"]
fn wrong_order_model() {
    model(false);
}

/// With a limit of 1 and no release, exactly one of two racing pushes is admitted, and the refused node is untouched.
#[test]
fn queue_admits_exactly_up_to_the_limit() {
    builder().check(|| {
        let q = Arc::new(GkParkingQueue::<Node>::new(1));
        let admitted = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..2)
            .map(|i| {
                let (q, admitted) = (q.clone(), admitted.clone());
                thread::spawn(move || {
                    let n = node(i);
                    match unsafe { q.try_push(n) } {
                        Ok(_) => {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(GkQueueFull) => drop(unsafe { Box::from_raw(n.as_ptr()) }),
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(admitted.load(Ordering::Relaxed), 1);
        assert_eq!(q.len(), 1);
        let head = q.reap().unwrap();
        drop(unsafe { Box::from_raw(head.as_ptr()) });
        q.release(1);
        assert!(q.is_empty());
    });
}

/// A consumer that releases capacity while a producer retries: the producer eventually gets in, and its payload is
/// visible to the consumer.
#[test]
fn queue_release_lets_a_refused_producer_in() {
    builder().check(|| {
        let q = Arc::new(GkParkingQueue::<Node>::new(1));
        let _ = unsafe { q.try_push(node(5)) }.unwrap();
        let producer = {
            let q = q.clone();
            thread::spawn(move || {
                let n = node(7);
                while unsafe { q.try_push(n) }.is_err() {
                    thread::yield_now();
                }
            })
        };
        let mut sum = 0;
        while sum != 12 {
            match q.reap() {
                Some(head) => {
                    let (first, _, count) = Node::reverse_in_place(head.as_ptr());
                    for p in unsafe { TreiberStackIterator::from_head(first) } {
                        sum += unsafe { Box::from_raw(p) }.value.with(|v| unsafe { *v });
                    }
                    q.release(count);
                }
                None => thread::yield_now(),
            }
        }
        producer.join().unwrap();
    });
}

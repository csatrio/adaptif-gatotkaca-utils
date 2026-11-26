//! Loom model of how a `GkTreiberChannel` receiver sleeps: parked markers in the outbox heads (see "Waking the
//! receiver" in `treiber_channel`'s module docs). The channel itself uses `std` atomics, so this is a model of its
//! protocol, with the same orderings, not the channel code:
//!
//! - send: write the payload, push with a `Release` compare-and-swap; only a push that replaced the marker runs a
//!   `SeqCst` fence and, if the waiter is published, claims it (compare-and-swap), unparks the receiver and hands the
//!   waker back;
//! - receiver: polls every known outbox (a non-empty one is taken with an `Acquire` swap; a marker stays); to sleep it
//!   takes its waker back, prepares it, publishes it, runs a `SeqCst` fence, absorbs new senders and marks every empty
//!   outbox parked, skipping those still parked from an earlier sleep; then parks and retracts the waker;
//! - a sender registered later (its outbox starts parked) is only seen through the registry.
//!
//! A lost wake-up leaves the receiver parked forever, which loom reports as a deadlock. Payloads and the waker are in
//! `loom::cell::UnsafeCell`s, so a missing happens-before is reported as a data race.
//!
//! ```sh
//! RUSTFLAGS="--cfg gk_loom" cargo test --release --test loom_channel_park
//! ```
#![cfg(gk_loom)]

use loom::cell::UnsafeCell;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use loom::thread::{self, Thread};

/// An outbox head holds the number of jobs queued, or this marker.
const PARKED: usize = usize::MAX;
/// Jobs sent by the first (early) sender; the late sender sends one.
const EARLY_JOBS: usize = 2;

struct Shared {
    /// Outbox heads: 0 is the early sender (known from the start), 1 the late one (known through `registered`).
    outboxes: [AtomicUsize; 2],
    /// The late sender's registration (the registry stack in the channel).
    registered: AtomicBool,
    payloads: [[UnsafeCell<u64>; EARLY_JOBS]; 2],
    /// 1 while the receiver's waker is published.
    waiter: AtomicUsize,
    /// 1 while the waker is home, ready for the receiver's next sleep.
    spare: AtomicUsize,
    waker: UnsafeCell<Option<Thread>>,
}

impl Shared {
    fn new() -> Self {
        let cells = || [UnsafeCell::new(0), UnsafeCell::new(0)];
        Self {
            outboxes: [AtomicUsize::new(PARKED), AtomicUsize::new(PARKED)],
            registered: AtomicBool::new(false),
            payloads: [cells(), cells()],
            waiter: AtomicUsize::new(0),
            spare: AtomicUsize::new(1),
            waker: UnsafeCell::new(None),
        }
    }

    /// `GkSender::try_send`. `seqcst_fence: false` is the faulty variant.
    fn send(&self, sender: usize, job: usize, seqcst_fence: bool) {
        self.payloads[sender][job].with_mut(|p| unsafe { *p = (sender * 10 + job) as u64 });
        let outbox = &self.outboxes[sender];
        let mut head = outbox.load(Ordering::Relaxed);
        loop {
            let jobs = if head == PARKED { 0 } else { head };
            match outbox.compare_exchange_weak(head, jobs + 1, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => head = actual,
            }
        }
        if head == PARKED {
            // `GkShared::wake_parked`
            fence(if seqcst_fence { Ordering::SeqCst } else { Ordering::Acquire });
            if self.waiter.load(Ordering::Relaxed) == 1 && self.waiter.compare_exchange(1, 0, Ordering::Acquire, Ordering::Relaxed).is_ok()
            {
                self.waker.with(|w| unsafe { (*w).as_ref().unwrap().unpark() });
                self.spare.store(1, Ordering::Release);
            }
        }
    }
}

/// The receiver's private state.
struct Receiver {
    shared: Arc<Shared>,
    /// Senders seen so far (the private list).
    known: usize,
    /// Jobs taken per sender, so payloads are read in order.
    taken: [usize; 2],
    sum: u64,
}

impl Receiver {
    fn absorb(&mut self) {
        if self.known == 1 && self.shared.registered.load(Ordering::Acquire) {
            self.known = 2;
        }
    }

    /// `GkReceiver::poll`: one lap; returns how many jobs it took.
    fn poll(&mut self) -> usize {
        self.absorb();
        let mut got = 0;
        for sender in 0..self.known {
            let outbox = &self.shared.outboxes[sender];
            let head = outbox.load(Ordering::Relaxed);
            if head == 0 || head == PARKED {
                continue;
            }
            let jobs = outbox.swap(0, Ordering::Acquire);
            assert!(jobs != PARKED && jobs != 0, "only the receiver marks or empties an outbox");
            for job in self.taken[sender]..self.taken[sender] + jobs {
                self.sum += self.shared.payloads[sender][job].with(|p| unsafe { *p });
            }
            self.taken[sender] += jobs;
            got += jobs;
        }
        got
    }

    /// `GkReceiver::mark_parked`: true if an outbox holds jobs.
    fn mark_parked(&mut self) -> bool {
        self.absorb();
        for outbox in &self.shared.outboxes[..self.known] {
            let head = outbox.load(Ordering::Relaxed);
            if head == 0 {
                if outbox.compare_exchange(0, PARKED, Ordering::Relaxed, Ordering::Relaxed).is_err() {
                    return true;
                }
            } else if head != PARKED {
                return true;
            }
        }
        false
    }

    fn receive_all(&mut self, total: usize) {
        let me = thread::current();
        let mut got = 0;
        loop {
            got += self.poll();
            if got == total {
                return;
            }
            // `announce`: take the waker back; the producer that last woke us may still hold it.
            let mut home = false;
            while !home {
                home = self.shared.spare.swap(0, Ordering::Acquire) == 1;
                if !home {
                    let more = self.poll();
                    if more != 0 {
                        got += more;
                        break;
                    }
                    thread::yield_now();
                }
            }
            if !home {
                continue;
            }
            self.shared.waker.with_mut(|w| unsafe { *w = Some(me.clone()) });
            self.shared.waiter.store(1, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            if !self.mark_parked() {
                thread::park();
            }
            // `retract`
            if self.shared.waiter.swap(0, Ordering::Acquire) == 1 {
                self.shared.spare.store(1, Ordering::Release);
            }
        }
    }
}

fn builder() -> loom::model::Builder {
    let mut b = loom::model::Builder::new();
    if b.preemption_bound.is_none() {
        b.preemption_bound = Some(3);
    }
    b
}

fn model(seqcst_fence: bool) {
    builder().check(move || {
        let shared = Arc::new(Shared::new());
        let early = {
            let shared = shared.clone();
            thread::spawn(move || {
                for job in 0..EARLY_JOBS {
                    shared.send(0, job, seqcst_fence);
                }
            })
        };
        let late = {
            let shared = shared.clone();
            thread::spawn(move || {
                // `GkTreiberChannel::sender`: the registry push (Release) happens before the first send.
                shared.registered.store(true, Ordering::Release);
                shared.send(1, 0, seqcst_fence);
            })
        };
        let mut receiver = Receiver { shared, known: 1, taken: [0; 2], sum: 0 };
        receiver.receive_all(EARLY_JOBS + 1);
        // Payloads are `sender * 10 + job`: 0 and 1 from the early sender, 10 from the late one.
        assert_eq!(receiver.sum, 11);
        early.join().unwrap();
        late.join().unwrap();
    });
}

/// Every interleaving delivers every job, with its payload visible, and never leaves the receiver asleep with a job
/// queued: through fresh markers, markers left from an earlier sleep, and a sender the receiver has not seen yet.
#[test]
fn parked_outboxes_lose_no_wakeup() {
    model(true);
}

/// With only an `Acquire` fence on the waking push, the receiver can skip an outbox it believes still parked while
/// the push that replaced the marker misses the waiter: a lost wake-up. Loom must find it, which shows the model above
/// can fail. A loom deadlock aborts the process while unwinding, so the faulty model runs in a child process.
#[test]
fn acquire_fence_on_the_waking_push_loses_a_wakeup() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "faulty_fence_model", "--nocapture", "--test-threads=1"])
        .output()
        .unwrap();
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(!out.status.success(), "the faulty model passed: the loom model cannot detect a lost wake-up");
    assert!(text.contains("deadlock"), "expected a loom deadlock, got:\n{text}");
}

#[test]
#[ignore = "faulty on purpose; run by `acquire_fence_on_the_waking_push_loses_a_wakeup` in a child process"]
fn faulty_fence_model() {
    model(false);
}

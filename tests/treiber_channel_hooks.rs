//! The channel's hooks and modes: custom wakers (thread pools, async runtimes), allocation from a mapped region, and the
//! single-producer (1:1) mode.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::{
    GkChannelMemory, GkHeapMemory, GkRegionError, GkRegionMemory, GkTaskWaker, GkThreadWaker,
    GkTreiberChannel, GkTreiberChannelConfig, GkTreiberChannelError, GkTrySendError, GkWaker,
};

use std::{
    alloc::Layout,
    future::Future,
    pin::pin,
    ptr::NonNull,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

// ==== Helpers ====

fn within<R: Send + 'static>(seconds: u64, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(seconds))
        .expect("test timed out")
}

fn small(slab: usize) -> GkTreiberChannelConfig {
    GkTreiberChannelConfig::DEFAULT
        .with_slab_nodes(slab)
        .with_grow_nodes(slab)
        .with_return_batch(8)
}

/// Producers send `per` values each; the receiver (any waker) checks per-producer order and the count.
fn run_traffic<W: GkWaker + Default, M: GkChannelMemory>(
    channel: &GkTreiberChannel<u64, W, M>,
    producers: u64,
    per: u64,
) {
    let mut receiver = channel.receiver().unwrap();
    let handles: Vec<_> = (0..producers)
        .map(|id| {
            let mut sender = channel.sender().unwrap();
            thread::spawn(move || {
                for i in 0..per {
                    sender.send(id << 32 | i).unwrap();
                }
            })
        })
        .collect();
    let mut next = vec![0u64; producers as usize];
    let mut count = 0;
    while let Ok(value) = receiver.recv() {
        let (id, seq) = ((value >> 32) as usize, value & 0xffff_ffff);
        assert_eq!(seq, next[id]);
        next[id] += 1;
        count += 1;
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(count, producers * per);
}

// ==== Waker hook ====

/// A thread-pool style waker: sleeps on a condvar, counts wakeups.
#[derive(Default)]
struct PoolWaker {
    state: Arc<(Mutex<bool>, Condvar)>,
    unparks: Arc<AtomicUsize>,
}

impl GkWaker for PoolWaker {
    fn prepare(&mut self) {}

    fn park(&self, timeout: Option<Duration>) {
        let (lock, condvar) = &*self.state;
        let mut woken = lock.lock().unwrap();
        // BRANCH: an unpark that came first is consumed at once
        if !*woken {
            woken = condvar
                .wait_timeout(woken, timeout.unwrap_or(Duration::from_secs(5)))
                .unwrap()
                .0;
        }
        *woken = false;
    }

    fn unpark(&self) {
        self.unparks.fetch_add(1, Ordering::SeqCst);
        let (lock, condvar) = &*self.state;
        *lock.lock().unwrap() = true;
        condvar.notify_one();
    }
}

#[test]
fn a_custom_waker_parks_and_is_unparked() {
    within(20, || {
        let channel = GkTreiberChannel::<u64, PoolWaker>::with_config(small(16)).unwrap();
        let waker = PoolWaker::default();
        let unparks = waker.unparks.clone();
        let mut receiver = channel.receiver_with_waker(waker).unwrap();
        let mut sender = channel.sender().unwrap();
        let consumer = thread::spawn(move || {
            let value = receiver.recv().unwrap();
            (value, receiver)
        });
        // BRANCH: let the receiver park first
        thread::sleep(Duration::from_millis(100));
        let start = Instant::now();
        sender.send(5).unwrap();
        let (value, _receiver) = consumer.join().unwrap();
        assert_eq!(value, 5);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(unparks.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn a_custom_waker_carries_heavy_traffic() {
    within(60, || {
        let channel = GkTreiberChannel::<u64, PoolWaker>::with_config(small(32)).unwrap();
        run_traffic(&channel, 4, 20_000);
    });
}

#[test]
fn async_receive_on_tokio() {
    within(30, || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let channel = GkTreiberChannel::<u64, GkTaskWaker>::with_config(small(16)).unwrap();
            let mut receiver = channel.receiver().unwrap();
            let handles: Vec<_> = (0..3u64)
                .map(|id| {
                    let mut sender = channel.sender().unwrap();
                    thread::spawn(move || {
                        for i in 0..5_000 {
                            // BRANCH: pause now and then so the receiver task really goes Pending
                            if i % 1_000 == 0 {
                                thread::sleep(Duration::from_millis(5));
                            }
                            sender.send(id << 32 | i).unwrap();
                        }
                    })
                })
                .collect();
            drop(channel);
            let consumer = tokio::spawn(async move {
                let mut count = 0u64;
                while let Ok(_value) = receiver.recv_async().await {
                    count += 1;
                }
                count
            });
            assert_eq!(consumer.await.unwrap(), 15_000);
            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

/// A minimal executor: polls one future, parking the thread between polls.
fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread, AtomicUsize);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.1.fetch_add(1, Ordering::SeqCst);
            self.0.unpark();
        }
    }
    let wake = Arc::new(ThreadWake(thread::current(), AtomicUsize::new(0)));
    let waker = Waker::from(wake);
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        // BRANCH: ready, or sleep until the channel wakes the task
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

#[test]
fn async_receive_without_a_runtime() {
    within(20, || {
        let channel = GkTreiberChannel::<u64, GkTaskWaker>::with_config(small(4)).unwrap();
        let mut receiver = channel.receiver().unwrap();
        let mut sender = channel.sender().unwrap();
        // BRANCH: nothing yet, so the first poll is Pending; then a send wakes the task
        let producer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            sender.send(11).unwrap();
            sender
        });
        assert_eq!(block_on(receiver.recv_async()).unwrap(), 11);
        drop(producer.join().unwrap());
        assert_eq!(
            block_on(receiver.recv_async()).unwrap_err(),
            GkTreiberChannelError::DISCONNECTED
        );
    });
}

#[test]
fn a_pending_poll_then_drop_is_clean() {
    let channel = GkTreiberChannel::<u64, GkTaskWaker>::with_config(small(4)).unwrap();
    let mut receiver = channel.receiver().unwrap();
    let mut sender = channel.sender().unwrap();
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(receiver.poll_recv(&mut cx).is_pending());
    // BRANCH: a blocking receive after a pending poll takes the published waker back first
    sender.send(3).unwrap();
    assert_eq!(receiver.recv().unwrap(), 3);
    assert!(receiver.poll_recv(&mut cx).is_pending());
    drop(receiver);
    // The receiver is gone: sends fail and nothing wakes a dead task.
    assert_eq!(sender.send(4), Err(4));
}

// ==== Memory hook ====

/// The heap, counting live bytes and calls.
#[derive(Clone)]
struct Counted {
    live: Arc<AtomicIsize>,
    calls: Arc<AtomicUsize>,
}

impl Counted {
    fn new() -> Self {
        Self {
            live: Arc::new(AtomicIsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

// Safety: forwards to the heap.
unsafe impl GkChannelMemory for Counted {
    fn allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.live
            .fetch_add(layout.size() as isize, Ordering::SeqCst);
        GkHeapMemory.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.live
            .fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { GkHeapMemory.deallocate(ptr, layout) }
    }
}

/// Stand-in for an `mmap`: aligned memory with a stable address.
struct Mapping(Vec<u64>);

impl Mapping {
    fn new(bytes: usize) -> Self {
        Self(vec![0; bytes.div_ceil(8)])
    }

    fn region(&mut self) -> GkRegionMemory {
        let base = NonNull::new(self.0.as_mut_ptr().cast::<u8>()).unwrap();
        // Safety: the mapping outlives every channel in the tests and is used only through the region.
        unsafe { GkRegionMemory::new(base, self.0.len() * 8) }.unwrap()
    }
}

#[test]
fn every_allocation_goes_through_the_memory_hook() {
    let memory = Counted::new();
    let channel =
        GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(small(16), memory.clone()).unwrap();
    run_traffic(&channel, 4, 2_000);
    assert!(memory.calls.load(Ordering::SeqCst) > 4);
    drop(channel);
    // BRANCH: shared state, thread channels, slabs, heap nodes and the waker were all given back
    assert_eq!(memory.live.load(Ordering::SeqCst), 0);
}

#[test]
fn many_channels_share_one_region_and_give_it_back() {
    let mut mapping = Mapping::new(8 << 20);
    let region = mapping.region();
    let config = small(32);
    // BRANCH: a large number of channels live in the region at once
    let channels: Vec<_> = (0..200)
        .map(|_| GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(config, region).unwrap())
        .collect();
    for channel in channels.iter().step_by(20) {
        run_traffic(channel, 2, 500);
    }
    drop(channels);
    assert_eq!(region.in_use(), 0);
    // Freed blocks are reused: a second round fits in the same region.
    for _ in 0..3 {
        let channel =
            GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(config, region).unwrap();
        run_traffic(&channel, 3, 1_000);
    }
    assert_eq!(region.in_use(), 0);
}

#[test]
fn a_full_region_reports_out_of_memory_without_leaking() {
    let mut mapping = Mapping::new(GkRegionMemory::HEADER_LEN + 64);
    let region = mapping.region();
    assert_eq!(
        GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(small(4), region).err(),
        Some(GkTreiberChannelError::OUT_OF_MEMORY)
    );
    assert_eq!(region.in_use(), 0);

    let mut mapping = Mapping::new(64 * 1024);
    let region = mapping.region();
    let channel =
        GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(small(512), region).unwrap();
    let mut receiver = channel.receiver().unwrap();
    let mut senders = Vec::new();
    // BRANCH: senders until the region is full; the failing one leaves nothing behind
    let error = loop {
        match channel.sender() {
            Ok(sender) => senders.push(sender),
            Err(error) => break error,
        }
    };
    assert_eq!(error, GkTreiberChannelError::OUT_OF_MEMORY);
    let full = region.in_use();
    assert!(channel.sender().is_err());
    assert_eq!(region.in_use(), full);
    // BRANCH: past its slab in a full region, an unbounded sender reports Full instead of allocating
    let sender = &mut senders[0];
    let mut sent = 0u64;
    loop {
        match sender.try_send(sent) {
            Ok(()) => sent += 1,
            Err(GkTrySendError::Full(value)) => break assert_eq!(value, sent),
            Err(GkTrySendError::Closed(_)) => unreachable!(),
        }
    }
    for i in 0..sent {
        assert_eq!(receiver.try_recv(), Some(i));
    }
    sender.try_send(1).unwrap();
    drop((senders, receiver, channel));
    assert_eq!(region.in_use(), 0);
}

#[test]
fn region_set_up_errors() {
    let mut mapping = Mapping::new(4096);
    let base = NonNull::new(mapping.0.as_mut_ptr().cast::<u8>()).unwrap();
    // Safety (both calls): refused before the memory is touched.
    assert_eq!(
        unsafe { GkRegionMemory::new(base, GkRegionMemory::HEADER_LEN - 1) }.err(),
        Some(GkRegionError::TooSmall)
    );
    let odd = NonNull::new(base.as_ptr().wrapping_add(1)).unwrap();
    assert_eq!(
        unsafe { GkRegionMemory::new(odd, 1024) }.err(),
        Some(GkRegionError::Misaligned)
    );
}

// ==== Single producer ====

#[test]
fn single_producer_has_one_sender_and_keeps_order() {
    within(30, || {
        let config = GkTreiberChannelConfig::DEFAULT.with_single_producer(true);
        let channel = GkTreiberChannel::<u64>::with_config(config).unwrap();
        let mut receiver = channel.receiver().unwrap();
        let mut sender = channel.sender().unwrap();
        assert_eq!(
            channel.sender().err(),
            Some(GkTreiberChannelError::SENDER_TAKEN)
        );
        let producer = thread::spawn(move || {
            for i in 0..200_000u64 {
                sender.send(i).unwrap();
            }
        });
        for i in 0..200_000u64 {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        producer.join().unwrap();
        assert_eq!(
            receiver.recv().unwrap_err(),
            GkTreiberChannelError::DISCONNECTED
        );
        // The one sender is gone for good.
        assert_eq!(
            channel.sender().err(),
            Some(GkTreiberChannelError::SENDER_TAKEN)
        );
    });
}

#[test]
fn single_producer_ping_pong() {
    within(30, || {
        let config = GkTreiberChannelConfig::DEFAULT
            .with_single_producer(true)
            .with_slab_nodes(16);
        let (request, reply) = (
            GkTreiberChannel::<u64>::with_config(config).unwrap(),
            GkTreiberChannel::<u64>::with_config(config).unwrap(),
        );
        let (mut request_tx, mut request_rx) =
            (request.sender().unwrap(), request.receiver().unwrap());
        let (mut reply_tx, mut reply_rx) = (reply.sender().unwrap(), reply.receiver().unwrap());
        let server = thread::spawn(move || {
            while let Ok(value) = request_rx.recv() {
                reply_tx.send(value + 1).unwrap();
            }
        });
        for i in 0..20_000 {
            request_tx.send(i).unwrap();
            assert_eq!(reply_rx.recv().unwrap(), i + 1);
        }
        drop(request_tx);
        server.join().unwrap();
    });
}

/// Small enough for Miri: `cargo +nightly miri test --test treiber_channel_hooks small_`.
#[test]
fn small_hooks_under_contention() {
    let mut mapping = Mapping::new(64 * 1024);
    let region = mapping.region();
    let config = small(4).with_return_batch(3).with_spin_polls(2);
    // BRANCH: unbounded and bounded in a region, then a single-producer channel on the heap
    for bounded in [false, true] {
        let channel = GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(
            config.with_bounded(bounded),
            region,
        )
        .unwrap();
        run_traffic(&channel, 3, 20);
        drop(channel);
        assert_eq!(region.in_use(), 0);
    }
    let channel = GkTreiberChannel::<u64>::with_config(config.with_single_producer(true)).unwrap();
    run_traffic(&channel, 1, 40);
}

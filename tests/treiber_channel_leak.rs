//! Leak check for the treiber channel: a counting allocator proves every slab, heap node and thread channel is freed.
//! One test only, in its own binary, so no other test allocates while it counts.

// ==== Import Definition ====

use adaptif_gatotkaca_utils::treiber_channel::GkTreiberChannel;

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicIsize, Ordering},
    thread,
};

// ==== Counting allocator ====

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size as isize - layout.size() as isize, Ordering::SeqCst);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

// ==== Scenarios ====

/// Which end goes away first, and whether jobs outlive the receiver.
#[derive(Clone, Copy)]
enum Ending {
    ReceiverLast,
    ReceiverFirst,
    JobsOutliveBoth,
}

fn round(ending: Ending) {
    let channel = GkTreiberChannel::<[u64; 4]>::new();
    let mut receiver = channel.receiver().unwrap();
    let handles: Vec<_> = (0..4u64)
        .map(|id| {
            let mut sender = channel.sender().unwrap();
            thread::spawn(move || {
                // More than the slab, so heap nodes exist too.
                for i in 0..6_000u64 {
                    sender.send([id, i, 0, 0]).unwrap();
                }
            })
        })
        .collect();
    let mut held = Vec::new();
    for i in 0..12_000 {
        let job = receiver.recv().unwrap();
        // BRANCH: keep a few jobs to return after the endpoints are gone
        if i % 100 == 0 {
            held.push(job);
        }
    }
    for handle in handles {
        handle.join().unwrap();
    }
    match ending {
        // BRANCH: receiver drops last, after the held jobs went home
        Ending::ReceiverLast => {
            drop(held);
            drop(receiver);
        }
        // BRANCH: receiver drops while jobs are still queued, then the held jobs return
        Ending::ReceiverFirst => {
            drop(receiver);
            drop(held);
        }
        // BRANCH: the factory and receiver go first, jobs come home to dead thread channels
        Ending::JobsOutliveBoth => {
            drop(receiver);
            drop(channel);
            drop(held);
        }
    }
}

/// The receiver parks (publishing a reused, boxed thread handle) and is woken, many times, then the channel is dropped.
fn parking_round() {
    let channel = GkTreiberChannel::<u64>::new();
    let mut sender = channel.sender().unwrap();
    let mut receiver = channel.receiver().unwrap();
    let consumer = thread::spawn(move || {
        let mut count = 0;
        while receiver.recv().is_ok() {
            count += 1;
        }
        count
    });
    for i in 0..20 {
        // BRANCH: leave time for the consumer to park before each send
        thread::sleep(std::time::Duration::from_millis(5));
        sender.send(i).unwrap();
    }
    drop(sender);
    assert_eq!(consumer.join().unwrap(), 20);
}

#[test]
fn nothing_leaks_in_any_ending() {
    // Warm up the allocator and thread machinery, then measure.
    round(Ending::ReceiverLast);
    for ending in [
        Ending::ReceiverLast,
        Ending::ReceiverFirst,
        Ending::JobsOutliveBoth,
    ] {
        let before = LIVE.load(Ordering::SeqCst);
        for _ in 0..3 {
            round(ending);
        }
        let after = LIVE.load(Ordering::SeqCst);
        assert_eq!(before, after, "leaked {} bytes", after - before);
    }
    // Parking reuses one boxed thread handle; it is freed with the channel.
    parking_round();
    let before = LIVE.load(Ordering::SeqCst);
    parking_round();
    let after = LIVE.load(Ordering::SeqCst);
    assert_eq!(before, after, "parking leaked {} bytes", after - before);
}

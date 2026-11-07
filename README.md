# adaptif-gatotkaca-utils

[![crates.io](https://img.shields.io/crates/v/adaptif-gatotkaca-utils.svg)](https://crates.io/crates/adaptif-gatotkaca-utils)
[![docs.rs](https://docs.rs/adaptif-gatotkaca-utils/badge.svg)](https://docs.rs/adaptif-gatotkaca-utils)

Low-level building blocks used by the adaptif-gatotkaca transport system,
developed by [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).

| Module | What it provides |
| --- | --- |
| `fast_hashmap` | `FastU64Map<V>`: a `HashMap<u64, V>` that uses the key as its own hash (no SipHash). |
| `refcounted` | `GkRefCounted<T>`: an `Arc`-like pointer whose reference count lives inside `T` (`GkIntrinsicRefCounted`). |
| `treiber_stack` | Lock-free Treiber stack helpers on `AtomicPtr`, FIFO/LIFO iterators, and a bounded `TreiberQueue`. |
| `treiber_channel` | `GkTreiberChannel`: a many-producer, one-consumer channel. `GkTreiberMpmc`: a many-producer, many-consumer job queue for thread pools. |

## Installation

```toml
[dependencies]
adaptif-gatotkaca-utils = "0.15"
```

The crate re-exports its dependencies, `crossbeam_utils` and `nohash_hasher`,
so you can use them without adding them to your own `Cargo.toml`. This also
guarantees you get the same versions the public types use:

```rust
use adaptif_gatotkaca_utils::crossbeam_utils::CachePadded;
use adaptif_gatotkaca_utils::nohash_hasher::IsEnabled;

let padded = CachePadded::new(1u64);
assert_eq!(*padded, 1);
fn takes_key<K: IsEnabled>(_: K) {}
takes_key(5u64);
```

## FastU64Map

A `HashMap<u64, V>` that uses each key as its own hash, so there is no SipHash
cost on every lookup. It derefs to the inner `HashMap`, so all the usual
methods work.

Use it when keys are already spread out (IDs, counters, row IDs). Don't use it
with keys an attacker can choose, because it gives no HashDoS protection.

```rust
use adaptif_gatotkaca_utils::FastU64Map;

let mut sessions = FastU64Map::with_capacity(1024);
sessions.insert(1001, "alice");
sessions.insert(1002, "bob");

assert_eq!(sessions.get(&1001), Some(&"alice"));
sessions.remove(&1002);

for (id, name) in &sessions {
    println!("{id} -> {name}");
}

// Get the plain HashMap back if you need it.
let inner = sessions.into_inner();
assert_eq!(inner.len(), 1);
```

## GkRefCounted

An `Arc`-like pointer whose reference count is a field inside your own type.
A handle is a single pointer, and you can turn it into a raw pointer and back
(for example to pass it through a C API or an atomic) without extra allocation.

Steps:

1. Add a counter field (usually `AtomicU32`) to your type.
2. Implement `GkIntrinsicRefCounted` for it.
3. Allocate the object with `Box::into_raw` and wrap it with
   `unsafe { GkRefCounted::new(ptr) }`. The count starts at 1.
4. `clone()` adds a reference, and dropping a handle removes one. The last
   handle calls `release`, which by default drops the object and frees it with
   `Box`.

`GkRefCounted<T>` is `Send` and `Sync` only when `T` is both, like `Arc`. It
has no `DerefMut`, so safe code can never get two `&mut` to the same object.

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use adaptif_gatotkaca_utils::{GkIntrinsicRefCounted, GkRefCounted};

struct Connection {
    ref_count: AtomicU32,
    id: u64,
}

impl GkIntrinsicRefCounted for Connection {
    fn set_ref_count(&self, val: u32, ordering: Ordering) {
        self.ref_count.store(val, ordering)
    }
    fn inc_ref_count(&self, ordering: Ordering) -> u32 {
        self.ref_count.fetch_add(1, ordering)
    }
    fn dec_ref_count(&self, ordering: Ordering) -> u32 {
        self.ref_count.fetch_sub(1, ordering)
    }
    fn get_ref_count(&self, ordering: Ordering) -> u32 {
        self.ref_count.load(ordering)
    }
}

// The pointer must come from Box::into_raw, because the last handle frees it with Box.
let raw = Box::into_raw(Box::new(Connection { ref_count: AtomicU32::new(0), id: 7 }));
// Safety: `raw` is a fresh Box pointer that no other GkRefCounted manages.
let mut conn = unsafe { GkRefCounted::new(raw) };

// You hold the only handle, so safe mutable access is allowed.
GkRefCounted::get_mut(&mut conn).unwrap().id = 7;

// Share it with another thread.
let for_thread = conn.clone();
assert_eq!(conn.get_ref_count(Ordering::Relaxed), 2);
std::thread::spawn(move || {
    assert_eq!(for_thread.id, 7);
})
.join()
.unwrap();
assert_eq!(conn.get_ref_count(Ordering::Relaxed), 1);

// Round-trip through a raw pointer without changing the count.
let ptr = conn.into_raw();
// Safety: `ptr` came from `into_raw`, so the count already includes this handle.
let conn = unsafe { GkRefCounted::from_raw(ptr) };
assert_eq!(conn.id, 7);
// `conn` is the last handle, so dropping it frees the Connection.
```

### Mutable access

- `GkRefCounted::get_mut(&mut handle)` returns `Some(&mut T)` only when this is
  the only handle, like `Arc::get_mut`. It is safe.
- For state that many handles change, put atomics or a lock inside `T`.
- `unsafe { handle.as_mut() }` skips the check. Use it only when you can
  guarantee nothing else touches the object while the reference is alive.

### Custom release (arenas, slabs)

By default the last handle frees the object with `Box`. If your objects live in
an arena or slab, override `release` so the memory goes back where it came from:

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use adaptif_gatotkaca_utils::{GkIntrinsicRefCounted, GkRefCounted};

struct Header {
    ref_count: AtomicU32,
}

impl GkIntrinsicRefCounted for Header {
    fn set_ref_count(&self, val: u32, ordering: Ordering) {
        self.ref_count.store(val, ordering)
    }
    fn inc_ref_count(&self, ordering: Ordering) -> u32 {
        self.ref_count.fetch_add(1, ordering)
    }
    fn dec_ref_count(&self, ordering: Ordering) -> u32 {
        self.ref_count.fetch_sub(1, ordering)
    }
    fn get_ref_count(&self, ordering: Ordering) -> u32 {
        self.ref_count.load(ordering)
    }

    // Called once, by the last handle. Do not use `ptr` afterwards.
    unsafe fn release(ptr: *mut Self) {
        // Run the destructor, then give the slot back to your arena here,
        // for example `arena.free(ptr)`. This sketch only runs the destructor.
        unsafe { std::ptr::drop_in_place(ptr) };
    }
}

// The slot lives outside Box (here: leaked, standing in for arena memory).
let slot: &'static mut Header = Box::leak(Box::new(Header { ref_count: AtomicU32::new(0) }));
// Safety: the slot is valid, and `release` does not free it with Box.
let handle = unsafe { GkRefCounted::new(slot) };
drop(handle); // calls Header::release
```

## Treiber stack and TreiberQueue

A lock-free stack where each node holds its own `next` pointer. Many threads
can push at once. A consumer takes the whole stack with one atomic swap
(`reap_all`) and then walks the nodes with no further atomic operations.
Nodes are never popped one at a time, so the stack is not affected by the ABA
problem.

Steps:

1. Add a `next: AtomicPtr<Self>` field to your node type.
2. Implement `TreiberNode` for it.
3. Push nodes allocated with `Box::into_raw`.
4. Reap, then iterate. Each yielded pointer is yours: turn it back into a
   `Box` with `Box::from_raw`. Nodes left over when the iterator is dropped are
   freed for you with `TreiberNode::drop_node`.

### When it breaks

`push_to_stack`, `push_chain` and `TreiberQueue::push` are not marked `unsafe`, but they
dereference the pointer you give them. They cause undefined behaviour if:

- the pointer is null, dangling or misaligned,
- the node is already in a stack or queue (its `next` is overwritten, which
  corrupts the chain),
- any node of a `push_chain` chain has one of these problems,
- you free or change the node while it is still in the stack or queue.

When `push` returns `false` the node was not added, so you still own it and
must free it yourself.

### Bounded queue with many producers

`TreiberQueue` adds an approximate size limit. `push` returns `false` when the
queue is full; the node was not pushed and you still own it.

The limit is approximate: while several threads push at once, the count can
briefly reach `limit` plus the number of pushing threads before the rejected
pushes subtract again. A node is never pushed unless the count was below the
limit when it was checked.

```rust
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use adaptif_gatotkaca_utils::{GkTreiberStackIterator, TreiberNode, TreiberQueue};

struct Job {
    next: AtomicPtr<Job>,
    value: u64,
}

impl TreiberNode for Job {
    type Value = u64;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &u64 {
        &self.value
    }
}

fn new_job(value: u64) -> *mut Job {
    Box::into_raw(Box::new(Job { next: AtomicPtr::new(ptr::null_mut()), value }))
}

let queue = Arc::new(TreiberQueue::<Job>::new(1024));

// Four producer threads push 100 jobs each.
let producers: Vec<_> = (0..4)
    .map(|t| {
        let queue = Arc::clone(&queue);
        std::thread::spawn(move || {
            for i in 0..100 {
                let job = new_job(t * 100 + i);
                if !queue.push(job) {
                    // Full: the job was not pushed, so free it ourselves.
                    drop(unsafe { Box::from_raw(job) });
                }
            }
        })
    })
    .collect();
for p in producers {
    p.join().unwrap();
}
assert_eq!(queue.len(), 400);

// Consumer: take everything in one swap. The count resets to 0.
let chain = queue.reap_all();
let mut total = 0;
for job in chain.iterator() {
    // `iterator()` gives oldest first; `reversed_iterator()` gives newest first.
    let job = unsafe { Box::from_raw(job) };
    total += *job.value();
}
assert_eq!(total, (0..400).sum::<u64>());
assert!(queue.is_empty());
```

### Pushing a batch with one CAS

`push_chain` publishes several nodes at once: other threads see all of them or
none, and they stay contiguous and in order. `TreiberNode::link_chain` builds
the chain from nodes given oldest first.

```rust
use std::ptr;
use std::sync::atomic::AtomicPtr;
use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode};

struct Msg {
    next: AtomicPtr<Msg>,
    id: u32,
}

impl TreiberNode for Msg {
    type Value = u32;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &u32 {
        &self.id
    }
}

let stack: AtomicPtr<Msg> = AtomicPtr::new(ptr::null_mut());
let batch = (1..=3).map(|id| {
    Box::into_raw(Box::new(Msg { next: AtomicPtr::new(ptr::null_mut()), id }))
});

// Oldest first in, one CAS out.
let (first, last) = Msg::link_chain(batch).unwrap();
stack.push_chain(first, last);

let ids: Vec<u32> = stack
    .reap_all()
    .iterator() // oldest first
    .map(|p| unsafe { Box::from_raw(p) }.id)
    .collect();
assert_eq!(ids, [1, 2, 3]);
```

`push_chain` has the same rules as `push_to_stack` for every node in the chain.

### Contention

Pushes back off with a short spin after a failed compare-and-swap (they never
yield to the OS). `reap_all` on an empty stack or queue only does a load, so a
consumer that polls often does not slow the producers down. In `TreiberQueue`,
the head and the counter sit on separate cache lines.

If several consumers share the queue, or you need an exact count, use
`reap_all_raw()` instead. It leaves the count alone; subtract from
`grab_counter()` as you consume items.

### Plain stack on an `AtomicPtr`

Without a limit, use the `GkTreiberPtr` methods directly on an `AtomicPtr`:

```rust
use std::ptr;
use std::sync::atomic::AtomicPtr;
use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode};

// Same Job type as above.
struct Job {
    next: AtomicPtr<Job>,
    value: u64,
}

impl TreiberNode for Job {
    type Value = u64;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &u64 {
        &self.value
    }
}

fn new_job(value: u64) -> *mut Job {
    Box::into_raw(Box::new(Job { next: AtomicPtr::new(ptr::null_mut()), value }))
}

let stack: AtomicPtr<Job> = AtomicPtr::new(ptr::null_mut());
stack.push_to_stack(new_job(1));
stack.push_to_stack(new_job(2));
stack.push_to_stack(new_job(3));

let newest_first: Vec<u64> = stack
    .reap_all()
    .reversed_iterator()
    .map(|job| unsafe { Box::from_raw(job) }.value)
    .collect();
assert_eq!(newest_first, [3, 2, 1]);
assert!(stack.is_empty());
```

## GkTreiberChannel

A many-producer, one-consumer channel built from one Treiber stack set per producing thread. Each producer owns a
`GkSender` with its own pre-allocated pool of job nodes, so a warm sender never allocates and producers never touch each
other's cache lines. The receiver works in laps over the producers: it takes one producer's outbox with a single swap
(an empty outbox costs only a load), hands its jobs out in order (FIFO per producer, unspecified between producers) and
moves on to the next; chains are never joined or walked to find a tail. `recv` moves the value out to the receiver, which
owns and drops it; the emptied nodes go back to their sender in batches, ready for the next job. A node is just a link
and the payload (16 bytes for a `u64`): the receiver knows whose chain it is draining, so the return batch is a slice of
the chain itself and costs nothing per node.

```rust
use adaptif_gatotkaca_utils::GkTreiberChannel;
use std::thread;

let channel = GkTreiberChannel::<u64>::new();
let mut receiver = channel.receiver().unwrap();

let producers: Vec<_> = (0..4u64)
    .map(|id| {
        let mut sender = channel.sender().unwrap(); // one per producing thread
        thread::spawn(move || {
            for i in 0..100 {
                sender.send(id * 1000 + i).unwrap();
            }
        })
    })
    .collect();

let mut received = 0;
while let Ok(value) = receiver.recv() {
    let _value: u64 = value; // the receiver owns the value; its node is already on its way home
    received += 1;
}
for producer in producers {
    producer.join().unwrap();
}
assert_eq!(received, 400);
```

`GkTreiberChannel::new_bounded()` caps each sender at `GK_SLAB_NODES` (4096) jobs not yet received: `send` then waits for
the receiver, and `try_send` returns `GkTrySendError::Full`. An unbounded sender that runs out of nodes grows by a whole
chunk of `GK_GROW_NODES` (4096) nodes in one allocation and keeps it until it is dropped; `GkSender::shrink()` frees the
chunks whose nodes are all back home. `GkTreiberChannel::with_config(GkTreiberChannelConfig)` sets the slab size, chunk
size, receiver spin, return batch, bound, `single_producer` (a 1:1 channel) and `lifo`; every field defaults to its
`GK_*` constant. The receiver spins briefly, then sleeps; the first producer to find it asleep wakes it. Memory: about
65 KiB per sender while idle (4096 16-byte nodes), 15.7 bytes per queued `u64` against crossbeam's 16.3.

`lifo` is for work that needs no ordering: the receiver skips the reverse that makes each producer's chain oldest-first
and hands it out newest-first. With two or more producers that is the fastest mode (see the benchmark). A `lifo`
receiver that catches up with its producers paces its next take with a bounded `Backoff::spin` (under a microsecond),
so batches build up instead of a cache-line tug of war; idle receivers never wait, so latency is unchanged. With one
producer FIFO is still faster, because the reverse keeps the receiver behind the producer for free.

Need more throughput? Raise `slab_nodes` (`GkTreiberChannelConfig::DEFAULT.with_slab_nodes(65_536)`): each sender then
has more jobs in flight before it waits or grows, at the cost of memory per sender.

Two hooks, both type parameters with defaults (`GkTreiberChannel<T, W = GkThreadWaker, M = GkHeapMemory>`):

- `GkWaker` (`prepare`, `park`, `unpark`): how the receiver sleeps and is woken. The default uses `std::thread::park`;
  a thread pool or runtime supplies its own through `receiver_with_waker`. With `GkTaskWaker`, `recv_async().await`
  and `poll_recv` receive from async code on tokio or any other executor.
- `GkChannelMemory` (`allocate`, `deallocate`): where the channel allocates. `GkTreiberChannel::with_memory(config,
  memory)` with `GkRegionMemory::new(base, len)` puts many channels in a region you map yourself.

### Benchmark

Throughput in millions of `u64` messages per second, with the default configuration (4096-node slab, 4096-node growth
chunks), 500,000 messages per producer, one receiver draining until every sender is gone, median of 5 runs. Measured on
an Apple M3 (4 performance + 4 efficiency cores), so from 4 producers up some threads run on efficiency cores.

| Producers | treiber | treiber 1:1 | treiber bounded | treiber lifo | crossbeam unbounded | crossbeam bounded(4096) | std mpsc |
|---|---|---|---|---|---|---|---|
| 1 | 264 | 237 | 85 | 101 | 97 | 95 | 108 |
| 2 | 286 | – | 202 | 454 | 127 | 121 | 43 |
| 3 | 285 | – | 215 | 447 | 131 | 68 | 50 |
| 4 | 280 | – | 219 | 446 | 128 | 52 | 12 |
| 5 | 277 | – | 217 | 444 | 125 | 38 | 17 |
| 6 | 281 | – | 217 | 429 | 116 | 19 | 16 |

- `treiber`: `GkTreiberChannel::new()` (unbounded). `treiber 1:1`: `single_producer`, measured with one producer only.
  `treiber bounded`: `new_bounded()`, 4096 jobs per sender; crossbeam's bound of 4096 is for the whole channel.
  `treiber lifo`: `with_lifo(true)`, unbounded.
- The 1-producer rows vary most between runs (the unbounded treiber measured 107 to 264 in back-to-back runs, the
  bounded one 85 to 157).
- Ping-pong round trip (send one value, wait for the echo), in microseconds: treiber p50 0.2 / p99 0.4 (1:1 0.3 / 0.4,
  lifo 0.2 / 0.4), crossbeam unbounded 0.2 / 0.8, crossbeam bounded 0.2 / 0.2, std mpsc 1.9 / 3.9.
- A bigger slab is faster: with `slab_nodes(65_536)`, bounded treiber measured 232 to 274 across 1 to 6 producers.

The tests in `tests/treiber_channel*.rs` cover ordering, wakeups, bounds, drops and leaks. The measurements
`tests/bench_treiber_channel.rs` and `tests/bench_treiber_channel_memory.rs` compare it with `crossbeam_channel` and
`std::sync::mpsc`; run them with `cargo test --release --test bench_treiber_channel -- --ignored --nocapture`.

## GkTreiberMpmc

A many-producer, many-consumer job queue for a thread pool fed by a few long-lived runtime threads. Each producer
takes a lane (`queue.lane()`) and pushes into it; a worker reaps a whole lane at once with `queue.try_reap()`, with no
registration and no state of its own, and runs the chain itself.

- **Intrusive jobs.** The job type implements `TreiberNode` and links through its own `next` field, so the queue never
  allocates per job. A reaped chain is a `TreiberStackIterator`: jobs a worker drops unrun, and jobs still queued when
  the last handle drops, are freed with `TreiberNode::drop_node`.
- **One swap per reap.** A lane is one cache line: a Treiber stack and two counters. A reap takes the stack with one
  swap (an empty lane costs a load) and reverses it, so a chain runs oldest first. Chains of one lane reaped by
  different workers run side by side, and a worker that blocks holds the rest of its chain.
- **Unbounded.** `push` never fails, waits or allocates.
- **No parking inside.** The pool decides who sleeps. `push` publishes the job with a `SeqCst` compare-and-swap before
  it returns; pair it with a `SeqCst` read-modify-write on the pool's state word, and have a worker do its own before
  its last `has_work()` re-check (one load per lane), and no wake-up is lost.
- **Ranking.** Each push stores the lane's push count (one relaxed store) and each reap adds the chain length to its
  taken count; `try_reap` tries the lane with the largest backlog first, then every lane.
- **Lanes are never freed** while the queue lives (`max_lanes`, 32 by default), so workers scan them while runtimes
  come and go. A dropped lane is reused by the next `lane()` call; jobs left in it are still reaped, before the new
  owner's. `lane.drain(f)` takes back what workers have not reaped (for a producer that sees the pool shut down),
  `queue.drain(f)` does so for every lane.

```rust
use adaptif_gatotkaca_utils::{GkTreiberMpmc, TreiberNode};
use std::{ptr::NonNull, sync::atomic::AtomicPtr, thread};

struct Task {
    next: AtomicPtr<Task>,
    id: u64,
}

impl TreiberNode for Task {
    type Value = u64;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &u64 {
        &self.id
    }
}

let queue = GkTreiberMpmc::<Task>::new();
let mut lane = queue.lane().unwrap(); // one per runtime thread
for id in 0..100 {
    let task = Box::into_raw(Box::new(Task { next: AtomicPtr::default(), id }));
    // Safety: a fresh task in no other queue; freed with `Box` if never run.
    unsafe { lane.push(NonNull::new(task).unwrap()) };
}
let workers: Vec<_> = (0..4)
    .map(|_| {
        let queue = queue.clone();
        thread::spawn(move || {
            let mut done = 0;
            while let Some(chain) = queue.try_reap() {
                for task in chain {
                    // Safety: the task is ours now.
                    drop(unsafe { Box::from_raw(task) });
                    done += 1;
                }
            }
            done
        })
    })
    .collect();
let done: u64 = workers.into_iter().map(|w| w.join().unwrap()).sum();
assert_eq!(done, 100);
```

Throughput on an 8-core Apple Silicon machine, millions of boxed jobs per second (P producers, C workers). Treiber
and the crossbeam-deque workers spin on their queue; crossbeam channel workers use `recv`.

| P × C | 1 × 2 | 1 × 4 | 1 × 8 | 2 × 2 | 2 × 4 | 2 × 8 | 4 × 2 | 4 × 4 | 4 × 8 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| treiber mpmc | 15.5 | 27.0 | 12.1 | 11.1 | 11.4 | 8.6 | 10.1 | 14.2 | 7.2 |
| crossbeam channel | 7.1 | 5.6 | 2.2 | 6.5 | 5.7 | 2.2 | 6.6 | 5.7 | 2.2 |
| crossbeam-deque `Injector` | 6.9 | 5.7 | 2.2 | 7.1 | 5.6 | 2.2 | 6.5 | 5.6 | 2.3 |
| crossbeam-deque, tokio model | 11.1 | 6.6 | 2.3 | 9.2 | 6.4 | 2.3 | 9.2 | 6.4 | 2.4 |

The `Injector` row takes one job per steal; the tokio model is the injector plus a FIFO deque per worker, filled with
`steal_batch_and_pop` and stolen from by the others, so it takes batches like `try_reap`. Treiber varies about ±15%
between runs. At 8 workers the machine is oversubscribed (8 workers plus the producers on 8 cores).

The tests are in `tests/treiber_mpmc.rs` (they also run under Miri); the measurement is
`cargo test --release --test bench_treiber_mpmc -- --ignored --nocapture`.

## Safety

`GkRefCounted` and the Treiber stack work with raw pointers, and the compiler
can't check how you use them. You must:

- allocate objects with `Box::into_raw`, or override
  `GkIntrinsicRefCounted::release` if you allocate another way,
- allocate Treiber nodes with `Box::into_raw`, or override
  `TreiberNode::drop_node`,
- call `GkRefCounted::new` and `from_raw` (both `unsafe`) only with pointers that
  meet their `# Safety` docs,
- never push a node that is already in a stack,
- never use a pointer after the last handle or the consumer has freed it.

See the [API docs](https://docs.rs/adaptif-gatotkaca-utils) for details.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Copyright (c) 2026 [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).

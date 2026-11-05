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

## Installation

```toml
[dependencies]
adaptif-gatotkaca-utils = "0.11"
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

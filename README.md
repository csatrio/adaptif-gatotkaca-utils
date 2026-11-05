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
adaptif-gatotkaca-utils = "0.1"
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
3. Allocate the object with `Box::into_raw` and wrap it with `GkRefCounted::new`.
   The count starts at 1.
4. `clone()` adds a reference, and dropping a handle removes one. The last
   handle drops the object and frees its memory.

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
let conn = GkRefCounted::new(raw);

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
let conn = GkRefCounted::from_raw(ptr);
assert_eq!(conn.id, 7);
// `conn` is the last handle, so dropping it frees the Connection.
```

Mutable access (`as_mut`, `DerefMut`) is not checked. Only use it when no other
handle is reading or writing the object at the same time.

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

### Bounded queue with many producers

`TreiberQueue` adds an approximate size limit. `push` returns `false` when the
queue is full; the node was not pushed and you still own it.

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

- allocate objects and nodes with `Box::into_raw` (or override
  `TreiberNode::drop_node` if you allocate another way),
- never push a node that is already in a stack,
- never use a pointer after the last handle or the consumer has freed it.

See the [API docs](https://docs.rs/adaptif-gatotkaca-utils) for details.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Copyright (c) 2026 [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).

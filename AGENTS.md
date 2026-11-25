# adaptif-gatotkaca-utils: guide for coding agents

For agents **using** this crate in another project (the last section is for agents changing the crate itself). The doc
comments are the source of truth; this file is the map, the recipes, and the safety rules. Every Rust example here is
compiled and run by `cargo test` (as doctests), so it matches the current API.

Most types here move **raw pointers** between threads. The compiler cannot check that use: follow the per-type
contracts in [Safety rules](#safety-rules) exactly, and test your integration under Miri (and loom for custom
protocols) as shown in [Testing your integration](#testing-your-integration).

## Pick the right type

| You need | Use | Producer cost | Notes |
|---|---|---|---|
| Many threads hand *values* to one thread | `GkTreiberChannel<T>` | one push into a per-thread outbox, no allocation when warm | owns the values; blocking, timeout, `try_recv`, or async receive |
| Many threads hand *intrusive nodes* to one consumer, no wake-up needed | `AtomicPtr<N>` + `GkTreiberPtr` | 1 CAS | the plainest building block |
| Same, but the consumer **sleeps** (event loop, eventfd, condvar) | `GkParkingStack<N>` | 1 CAS; a wake-up only once per sleep | `push` says when to wake; see the consumer loop recipe |
| Same, bounded, pushes may be **refused** (HTTP 503) | `GkParkingQueue<N>` | 1 load + 1 `fetch_add` + 1 CAS | exact count: the consumer calls `release(n)` |
| Bounded stack, approximate count, no sleep tracking | `TreiberQueue<N>` | 1 load + 1 `fetch_add` + 1 CAS | `reap_all` resets the count (slightly low under races) |
| A few runtime threads feed a pool of workers | `GkTreiberMpmc<N>` | 1 `SeqCst` CAS per push into its lane | the pool parks workers itself; see `has_work`, `shared_chains` |
| `Arc` without the separate control block | `GkRefCounted<T>` | atomic inc/dec on a counter inside `T` | override `release` for arenas/slabs |
| `HashMap<u64, V>` keyed by ids that are already well spread | `FastU64Map<V>` | no hashing | not for attacker-chosen keys (no hash flooding protection) |

Rules of thumb:
- Need to own and move plain values? Use `GkTreiberChannel`. Already have a struct you want queued without allocating?
  Use the intrusive stacks.
- Never pop nodes one at a time from a shared stack: every type here takes the **whole** chain at once. That is
  what makes them ABA-free without hazard pointers, epochs or tagged pointers.

## Intrusive nodes: `TreiberNode`

Every stack/queue type except the channel works on your own node type with an embedded link:

```rust
use adaptif_gatotkaca_utils::TreiberNode;
use std::sync::atomic::AtomicPtr;

struct Job {
    next: AtomicPtr<Job>, // owned by the queue while the node is queued: never touch it yourself
    id: u64,
}

impl TreiberNode for Job {
    type Value = u64;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &u64 {
        &self.id
    }
    // Default `drop_node` frees with `Box::from_raw`. Override it if nodes come from an arena, a slab, a pool,
    // or are reference-counted objects (e.g. Python objects): it is called for nodes a queue or iterator still
    // holds when it is dropped.
}
```

- Allocate nodes with `Box::into_raw` / `Box::leak`, or override `drop_node`.
- A node may be in **one** stack/queue at a time. Pushing it again before it is taken back corrupts both chains.
- Write the payload **before** pushing, with plain stores. The push publishes it (`Release`); the reap makes it
  visible (`Acquire`). Do not write to a node after pushing it.
- After a reap the chain is yours alone: walk it with no further synchronisation.

## Recipes

### Batch hand-off with a plain `AtomicPtr`

```rust
use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode};
use std::sync::atomic::AtomicPtr;
# struct Job { next: AtomicPtr<Job>, id: u64 }
# impl TreiberNode for Job {
#     type Value = u64;
#     fn next(&self) -> &AtomicPtr<Self> { &self.next }
#     fn value(&self) -> &u64 { &self.id }
# }

let stack: AtomicPtr<Job> = GkTreiberPtr::new();
for id in 0..3 {
    stack.push_to_stack(Box::into_raw(Box::new(Job { next: AtomicPtr::default(), id })));
}
// Consumer: one swap takes everything; `iterator()` yields oldest first, `reversed_iterator()` newest first.
let ids: Vec<u64> = stack.reap_all().iterator().map(|j| unsafe { Box::from_raw(j) }.id).collect();
assert_eq!(ids, [0, 1, 2]);
```

### A sleeping consumer: `GkParkingStack` (event loops, eventfd)

The head is `PARKED` (consumer asleep), null (awake, nothing queued) or the newest node. `push` returns `true`
exactly once per sleep: that caller wakes the consumer. While the consumer is draining, pushes never wake it.

The consumer loop below is the canonical shape, with a per-turn budget. `Signal` stands for your wake-up primitive
(an eventfd, a semaphore, a condvar counter).

```rust
use adaptif_gatotkaca_utils::{GkParkingStack, TreiberNode, TreiberStackIterator};
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
# struct Job { next: AtomicPtr<Job>, id: u64 }
# impl TreiberNode for Job {
#     type Value = u64;
#     fn next(&self) -> &AtomicPtr<Self> { &self.next }
#     fn value(&self) -> &u64 { &self.id }
# }

/// Level-triggered wake-up: `notify` adds, `clear` resets, readiness stays while non-zero.
trait Signal {
    fn notify(&self);
    fn clear(&self);
}

/// Producer side: any thread.
fn submit(stack: &GkParkingStack<Job>, signal: &impl Signal, id: u64) {
    let job = NonNull::from(Box::leak(Box::new(Job { next: AtomicPtr::default(), id })));
    // Safety: a fresh node, in no other stack; we never touch it again.
    if unsafe { stack.push(job) } {
        signal.notify(); // the consumer was asleep
    }
}

/// Consumer side: one thread, called whenever the signal is ready. `cursor` keeps the unprocessed rest between
/// turns. Returns how many jobs ran.
fn turn(
    stack: &GkParkingStack<Job>,
    signal: &impl Signal,
    cursor: &mut *mut Job,
    budget: usize,
    run: &mut impl FnMut(u64),
) -> usize {
    let mut done = 0;
    loop {
        if cursor.is_null() {
            match stack.reap() {
                // Reverse once so jobs run in push order.
                Some(head) => *cursor = Job::reverse_in_place(head.as_ptr()).0,
                None => {
                    signal.clear(); // BEFORE try_park, never after
                    if stack.try_park() {
                        return done; // asleep: the next push notifies
                    }
                    continue; // a job raced in: take it
                }
            }
        }
        // Safety: the chain at `cursor` was reaped by us.
        let mut it = unsafe { TreiberStackIterator::from_head(*cursor) };
        for job in it.by_ref().take(budget - done) {
            let job = unsafe { Box::from_raw(job) }; // popped before running: re-entrant code sees consistent state
            run(job.id);
            done += 1;
        }
        *cursor = it.into_remaining(); // keep the rest, nothing freed
        if done == budget {
            // Budget spent with work left: do NOT clear the signal. A level-triggered poller calls us again.
            return done;
        }
    }
}

# struct Bell(std::cell::Cell<u64>);
# impl Signal for Bell { fn notify(&self) { self.0.set(self.0.get() + 1) } fn clear(&self) { self.0.set(0) } }
let (stack, bell) = (GkParkingStack::<Job>::new(), Bell(std::cell::Cell::new(0)));
for id in 0..5 {
    submit(&stack, &bell, id);
}
assert_eq!(bell.0.get(), 1, "one wake-up for five pushes");
let (mut cursor, mut seen) = (std::ptr::null_mut(), Vec::new());
assert_eq!(turn(&stack, &bell, &mut cursor, 3, &mut |id| seen.push(id)), 3);
assert_eq!(turn(&stack, &bell, &mut cursor, 3, &mut |id| seen.push(id)), 2);
assert_eq!(seen, [0, 1, 2, 3, 4]);
assert!(stack.is_parked() && bell.0.get() == 0);
```

Protocol rules (each one is a lost wake-up or a hang if broken):
1. **Clear the signal before `try_park`.** Clearing after a successful park can eat the wake-up of a push that landed
   in between (the loom model `clearing_after_park_loses_a_wakeup` demonstrates the deadlock).
2. After any `reap` the consumer counts as **awake**: it must reach `try_park() == true` before it sleeps, or no push
   will wake it.
3. When `try_park` fails, reap again: nodes are waiting and their producers did not (and will not) notify.
4. Budget exhausted: leave the signal set (level-triggered pollers re-run you), or, if your signal is consumed on wait
   (edge-triggered, a thread that blocks in `read`), schedule another turn yourself.
5. One consumer only. `reap`, `try_park` and the cursor are single-consumer.
6. The stack starts parked: the very first push notifies.

### Bounded intake that may refuse: `GkParkingQueue`

```rust
use adaptif_gatotkaca_utils::{GkParkingQueue, GkQueueFull, TreiberNode, TreiberStackIterator};
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
# struct Job { next: AtomicPtr<Job>, id: u64 }
# impl TreiberNode for Job {
#     type Value = u64;
#     fn next(&self) -> &AtomicPtr<Self> { &self.next }
#     fn value(&self) -> &u64 { &self.id }
# }

let queue = GkParkingQueue::<Job>::new(2);
let mut refused = 0;
for id in 0..3 {
    let job = NonNull::from(Box::leak(Box::new(Job { next: AtomicPtr::default(), id })));
    // Safety: fresh node in no other queue. On Err we still own it.
    match unsafe { queue.try_push(job) } {
        Ok(_wake) => {} // `true`: notify the consumer, exactly as with GkParkingStack
        Err(GkQueueFull) => {
            refused += 1; // e.g. answer 503
            drop(unsafe { Box::from_raw(job.as_ptr()) });
        }
    }
}
assert_eq!(refused, 1);
// Consumer: reaping does NOT free capacity; release once done with the nodes.
let head = queue.reap().unwrap();
let (oldest, _, count) = Job::reverse_in_place(head.as_ptr());
for job in unsafe { TreiberStackIterator::from_head(oldest) } {
    drop(unsafe { Box::from_raw(job) });
}
queue.release(count);
assert!(queue.is_empty() && queue.try_park());
```

The limit is approximate under a race: the count can briefly reach `limit` plus the number of threads pushing at that
moment, but a push is never admitted unless the count was below `limit`. Release exactly what was admitted: releasing
more is a bug (debug builds assert).

### Values between threads: `GkTreiberChannel`

```rust
use adaptif_gatotkaca_utils::GkTreiberChannel;
use std::thread;

let channel = GkTreiberChannel::<String>::new(); // `new_bounded()` / `with_config(..)` for limits and tuning
let mut rx = channel.receiver().unwrap(); // exactly one receiver
let mut tx = channel.sender().unwrap(); // one sender per producing thread (movable, not shared)
let t = thread::spawn(move || tx.send("hi".to_string()).unwrap());
assert_eq!(rx.recv().unwrap(), "hi");
t.join().unwrap();
```

- `send` returns the value back (`Err(value)`) if the receiver is gone. `recv` fails with `DISCONNECTED` once every
  sender is gone and nothing is left.
- For async receivers use `GkTreiberChannel<T, GkTaskWaker>` and `rx.recv_async().await` (or `poll_recv`).
- `GkRegionMemory` lets a channel allocate from memory you mapped yourself (huge pages, shared mappings). It takes a
  small spin lock, but only on slow paths (creating senders, overflow nodes).

### Worker pools: `GkTreiberMpmc`

Producers take a lane (`queue.lane()`, one per producing thread, at most `max_lanes`), push intrusive jobs with
`unsafe lane.push(NonNull<N>)`; workers call `queue.try_reap()` and iterate the returned chain. The queue never parks:
your pool does. Before parking a worker, re-check `has_work()`; while `shared_chains() > 0`, park with a timeout of
about `rescue_after` so stalled chains get rescued. At shutdown, `drain` takes every unrun job. Read the module docs of
`treiber_channel::mpmc` before writing the pool's park/wake handshake.

### `GkRefCounted` and `FastU64Map`

- `unsafe GkRefCounted::new(ptr)` takes ownership and **sets** the count to 1. Pass a pointer no other handle manages,
  from `Box::into_raw` unless `release` is overridden. `into_raw`/`from_raw` round-trip one handle without touching
  the count. There is no `DerefMut`: use `get_mut` (only handle) or put atomics/locks inside `T`; `as_mut` is
  `unsafe` (you guarantee no other access).
- `FastU64Map` uses the key as its own hash: perfect for sequential or random ids, wrong for keys an attacker chooses.

## Safety rules

| Rule | If broken |
|---|---|
| A node is in at most one stack/queue at a time | chain corruption, nodes lost or run twice |
| Don't touch a node after pushing it, until a consumer gives it back | data race / use-after-free (the consumer may free or reuse it at once) |
| Payload written before the push | consumer reads stale or torn data |
| Single consumer for `reap`/`try_park`/`release` and for a `GkTreiberChannel` receiver | lost nodes, broken park state |
| Allocation matches `drop_node` / `release` (Box by default) | wrong deallocator, UB |
| Nodes still queued when a stack/iterator/queue is dropped are freed with `drop_node`: make it correct for your nodes (refcounted objects must decrement, not `Box::from_raw`), or drain first | double free or leak |
| `TreiberStackIterator::from_head` only on a chain you own (reaped, or from `into_remaining`) | data race |
| Clear a level-triggered signal *before* `try_park` | lost wake-up, consumer sleeps forever |
| Never rely on `is_empty`, `is_parked`, `len` for correctness | they are `Relaxed` hints: use `reap` to see data |
| `unsafe` calls need a `// SAFETY:` comment stating which of the above holds | review cannot check your reasoning |

## Progress guarantees

| Operation | Guarantee |
|---|---|
| push / `push_chain` / `try_push` (all stacks and queues) | lock-free (CAS retry loop under contention) |
| `reap`, `reap_all`, `try_park`, `release` | wait-free |
| `GkTreiberChannel::send` (unbounded) | lock-free; a bounded channel waits only when all of the sender's nodes are in flight |
| `GkTreiberChannel::recv` | blocks (by design) through its `GkWaker`; `try_recv` never blocks |
| `GkTreiberMpmc` push / `try_reap` | never wait, never allocate |
| `GkRegionMemory` allocation | spin lock, slow paths only |

## Testing your integration

- **Miri** (UB, data races, leaks) on small workloads: `cargo +nightly miri test`. Shrink thread and item counts under
  `cfg(miri)`, as this crate's own tests do. Use `MIRIFLAGS="-Zmiri-many-seeds=0..8"` to try more schedules.
- **loom** for your own consumer protocol: build with `RUSTFLAGS="--cfg gk_loom"`. That switches this crate's parking
  types to loom atomics (and pulls in `loom`), so loom explores their interleavings together with yours. Use
  `loom::sync`/`loom::thread`/`loom::cell::UnsafeCell` in the model and put payloads in a loom `UnsafeCell` so missing
  synchronisation is reported. A lost wake-up shows up as a loom deadlock. See `tests/loom_parking.rs` in the
  repository for a complete model, including a deliberately wrong one.
- A loom deadlock aborts the process while unwinding, so `#[should_panic]` cannot test a "must fail" model; run it in a
  child process (see `clearing_after_park_loses_a_wakeup`).

## Working on this crate

- MSRV 1.85 (`rust-version`), edition 2024. Formatting: `rustfmt.toml` (140 columns, `use_small_heuristics = "Max"`,
  Unix newlines), `.editorconfig` for other files, LF enforced by `.gitattributes`. rustfmt does not wrap comments or
  strings: keep them within 140 columns by hand (Markdown within 120).
- Before finishing:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
RUSTFLAGS="--cfg gk_loom" cargo clippy --all-targets -- -D warnings
cargo test                                         # includes README.md and AGENTS.md examples
RUSTFLAGS="--cfg gk_loom" cargo test --release --test loom_parking
cargo +nightly miri test --test treiber_parking --test treiber_publish --test treiber_iterator
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo +1.85 check --all-targets
cargo publish --dry-run
```

- New atomic protocol code: put a loom model in `tests/` and, if it needs loom atomics inside the crate, import them
  through the `cfg(gk_loom)` switch (as `src/treiber_parking.rs` does), never `cfg(loom)` (dependencies react to it).
- Every public item has a doc comment; every `unsafe fn` has `# Safety`; every `unsafe` block says why it holds.
- Record changes in `CHANGELOG.md` under the next version; bump `version` in `Cargo.toml` and the README's install
  line together.

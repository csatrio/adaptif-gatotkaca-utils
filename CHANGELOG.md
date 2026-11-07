# Changelog

## 0.15.0

- Added `GkTreiberMpmc`, a many-producer many-consumer job queue for thread pools fed by a few runtime threads. Each
  producer pushes into its own lane (`GkMpmcLane`, one `SeqCst` compare-and-swap per push); a worker reaps a whole
  lane with `try_reap`, without registering: one swap takes the lane's stack, reversed so the chain runs oldest first.
  Jobs are intrusive (`TreiberNode`), so nothing is allocated per job and nothing goes home. A lane is one cache line.
  `try_reap` tries the lane with the largest backlog first (push count minus reaped count). Lanes live in a fixed
  table (`max_lanes`, `GK_MPMC_MAX_LANES` = 32) freed only with the queue; a dropped lane is reused.
- A worker blocked in a job does not hold the rest of its reap. The reaping worker keeps the first job and publishes
  the rest in a chain slot (`max_chains`, `GK_MPMC_MAX_CHAINS` = 64), then takes its jobs from there one at a time (a
  compare-and-swap and a store on a cache line nobody else touches while it works). When a chain's first job stays the
  same for `rescue_after` (`GK_MPMC_RESCUE_AFTER` = 50 µs), an idle worker takes the chain over and the old owner stops
  at its next take. The check costs the owner nothing: idle workers remember, per thread, what they last saw in each
  slot, and look at most every `rescue_after / 2`. Every 16th `try_reap` per thread looks for stalled chains first.
  Each slot carries a generation, so a chain yields the jobs of one reap only, oldest first. `max_chains(0)` keeps
  every reap private; `rescue_after(0)` shares every chain with idle workers at once.
- `try_reap` returns a `GkMpmcChain`, an iterator of `NonNull<N>`. Dropping a chain never loses a job: the rest stays
  in its slot, or goes back on its lane when no slot is free; jobs still queued when the last handle drops are freed
  with `drop_node`.
- The queue never parks: the pool does. `has_work` (one load per lane) gives a worker its last re-check before
  parking; shared chains have owners, so they are counted apart by `shared_chains`, and a worker should park with a
  timeout of about `rescue_after` while it is above 0. `GkMpmcLane::drain` takes back a lane's unreaped jobs and
  `GkTreiberMpmc::drain` every lane's and chain slot's, for shutdown.
- `GkTreiberChannelError::TOO_MANY_LANES`.
- Measured on 8 cores: with 1 ms blocking jobs, wall time within about 2 ms of crossbeam's channel, crossbeam-deque's
  `Injector` and tokio's model (keeping each reap private took 1.4 to 3.6 times as long); with empty jobs, 6 to 18 M
  jobs/s against 2 to 11 for those three.

## 0.14.0

- Job nodes no longer record their producer: a node is a link and the payload (16 bytes for a `u64`, was 24), so an
  idle sender holds about 65 KiB instead of 97 and a queued `u64` costs 15.7 bytes. The receiver knows whose chain it
  is draining, so the return batch is a slice of that chain and `pop` writes nothing to a node: every `return_batch`
  pops, or when the chain runs out, the slice goes home with one compare-and-swap. Nodes come home in chain order
  rather than newest-first. Measured 10 to 15 percent more throughput with 2 to 6 producers (FIFO 277 to 286 M msgs/s,
  lifo 429 to 454).

## 0.13.0

- `GkTreiberChannelConfig::lifo` (`with_lifo`): the receiver hands each producer's chain out newest-first instead of
  reversing it, for work that needs no ordering. With two or more producers it measured 376 to 409 M msgs/s against
  245 to 262 for FIFO. A `lifo` receiver that catches up with its producers paces its next take with `Backoff::spin`
  (bounded, reset by a long chain or an idle lap; `GK_LIFO_SHORT_CHAIN` is the chain length that counts as caught up),
  so batches build up; latency is unchanged.
- The receiver now works in laps: one producer's outbox per take, handed out to the end before the next producer's,
  instead of taking every outbox and joining the chains. Nothing is walked to find a tail, each producer gets one turn
  per lap, and the receiver parks only after a whole lap found nothing. FIFO throughput went up across the board (1:1
  from 176 to 247 M msgs/s).
- Removed `GkReceiver::collect`: with laps, new jobs are picked up as the receiver moves on, and nothing is chained.
- The receiver's return batch reuses the chain's own links instead of rewriting each node's `next`.

## 0.12.0

- Added `GkTreiberChannel`, a many-producer single-consumer channel built from per-thread Treiber stacks
  (`GkSender`, `GkReceiver`), with an optional bounded mode. `recv` returns the value itself: the receiver owns and
  drops it, and sends the emptied nodes home in batches (one compare-and-swap per batch). The receiver spins with
  `Backoff::spin`, then parks its thread; producers wake it with `Thread::unpark`. The receiver parks with one thread
  handle allocated when it is taken, and tracks producers in an intrusive list, so it never allocates afterwards.
- `GkTreiberChannelConfig` and `GkTreiberChannel::with_config` set the slab size, growth chunk size, spin polls,
  return batch and bound; the defaults are `GK_SLAB_NODES`, `GK_GROW_NODES`, `GK_SPIN_POLLS` and `GK_RETURN_BATCH`.
  An unbounded sender grows by whole chunks (one allocation each) and keeps them until dropped or
  `GkSender::shrink()`, so bursts cause no allocate / free churn. An
  invalid configuration fails with `GkTreiberChannelError::INVALID_CONFIG` before anything is allocated.
- Waker hook: `GkWaker` (`prepare` / `park` / `unpark`), with `GkThreadWaker` (std park / unpark, the default) and
  `GkTaskWaker` for async receive (`GkReceiver::poll_recv`, `GkReceiver::recv_async`) on any executor.
  `GkTreiberChannel::receiver_with_waker` supplies a custom waker.
- Memory hook: `GkChannelMemory` (`allocate` / `deallocate`), with `GkHeapMemory` (the default) and `GkRegionMemory`,
  an allocator over a region the caller maps. `GkTreiberChannel::with_memory` allocates everything through it; a full
  memory gives `GkTreiberChannelError::OUT_OF_MEMORY` when creating and `Full` on an unbounded send.
- `GkTreiberChannelConfig::single_producer` makes a 1:1 channel (a second `sender()` fails with `SENDER_TAKEN`).
- New dev-dependency `tokio` for the async receive tests.
- A send publishes with a `SeqCst` compare-and-swap and checks the waiter with a `SeqCst` load, with no separate fence.
- New dev-dependency `crossbeam-channel` for the comparison measurements in `tests/`.

## 0.11.4

- `reap_all` takes the head with an `Acquire` swap instead of `AcqRel`. The
  swap only stores null, so it has nothing to release; the pushes' `Release`
  CAS operations form one release sequence that the swap acquires.
- `is_empty`, `TreiberQueue::len` and the count reset in `TreiberQueue::reap_all`
  use `Relaxed`. These values are hints and never made node data visible.
- The module docs describe the memory ordering of the stack.
- Added a concurrent publish test that also runs under Miri.

## 0.11.3

- Re-export the dependencies `crossbeam_utils` and `nohash_hasher` at the crate root.

## 0.11.2

- Reaper-side iterators no longer use atomic swaps. `iterator()` reverses the
  chain with plain loads and stores, and `TreiberStackIterator::next` and `Drop`
  do the same.
- `TreiberStackIterator` now holds a raw pointer instead of an `AtomicPtr`.
  - **Breaking:** it is `Send` only when the node type is `Send`, and no longer
    `Sync`. Before, it was `Send + Sync` for every node type, which let a chain
    of `!Send` values move to another thread in safe code.
  - Added `unsafe TreiberStackIterator::from_head`.

## 0.11.1

- `push_to_stack` backs off after a failed CAS and uses `Relaxed` loads and
  failure ordering.
- Added `GkTreiberPtr::push_chain` and `TreiberNode::link_chain`. Adding a
  required method to `GkTreiberPtr` breaks external implementors.
- `TreiberQueue::push` uses one `fetch_add` instead of a CAS loop; the head and
  the counter are on separate cache lines.
- `reap_all` skips the swap when the stack is empty.

## 0.11.0

- `GkRefCounted::new`, `from_raw` and `as_mut` are `unsafe`. `DerefMut` is
  removed; use the new `get_mut`.
- `GkRefCounted<T>` is `Send` and `Sync` only if `T` is.
- Added `GkIntrinsicRefCounted::release` to free objects that don't come from `Box`.

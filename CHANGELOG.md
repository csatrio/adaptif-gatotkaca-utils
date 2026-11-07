# Changelog

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

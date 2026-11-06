# Changelog

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

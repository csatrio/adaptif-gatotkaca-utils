//! Lock-free Treiber stack built on an intrusive `next` pointer.
//!
//! - [`TreiberNode`]: implement for your node type to give it a `next` link.
//! - [`GkTreiberPtr`]: push and take-all operations on an `AtomicPtr` head.
//! - [`GkTreiberStackIterator`]: walk a chain taken from the stack, oldest or
//!   newest first.
//! - [`TreiberQueue`]: a stack with an approximate size limit.
//!
//! Nodes are only ever removed all at once (one atomic swap of the head), never
//! popped one by one. Because there is no compare-and-swap pop, the stack is
//! not affected by the ABA problem and works with many concurrent producers.
//!
//! All nodes are passed as raw pointers. A pushed node must be valid, must not
//! be in another stack, and must stay alive until it is taken off again.
//!
//! # Memory ordering
//!
//! - A push writes the node (payload and `next`), then publishes it with a
//!   `Release` compare-and-swap on the head.
//! - [`reap_all`](GkTreiberPtr::reap_all) takes the head with an `Acquire`
//!   swap. Every successful push is a read-modify-write on the head, so all
//!   pushes in the taken chain form one release sequence, and the swap sees the
//!   writes of every node in the chain, not only the newest.
//! - Once taken, the chain belongs to one thread. Walking, reversing and
//!   linking chains use `Relaxed` loads and stores. These compile to plain
//!   loads and stores, and unlike non-atomic access they stop the compiler
//!   from caching or reordering `next` in ways that were measured to slow
//!   pointer-chasing loops down.
//! - Everything else (`is_empty`, the [`TreiberQueue`] count) is only a hint
//!   and uses `Relaxed`. Never use them to decide that a node's data is visible.

use core::{
    ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};
use crossbeam_utils::{Backoff, CachePadded};

// ============================================================================
// Abstraction and traits
// ============================================================================

/// Treiber stack operations on an atomic head pointer.
///
/// Implemented for `AtomicPtr<T>`, where a null pointer means the stack is empty.
pub trait GkTreiberPtr<T: TreiberNode> {
    /// Creates an empty stack (null head).
    fn new() -> Self;

    /// Creates a stack whose head is `raw_ptr`.
    fn new_from_raw(raw_ptr: *mut T) -> Self;

    /// Returns true if the stack is empty.
    fn is_empty(&self) -> bool;

    /// Takes the whole chain and leaves the stack empty.
    ///
    /// The returned head now owns the chain, newest node first. Use
    /// [`GkTreiberStackIterator`] to walk it.
    ///
    /// If the stack is already empty this only does a load, so polling an empty
    /// stack does not take the cache line away from the producers.
    fn reap_all(&self) -> Self;

    /// Pushes `item_ptr` onto the stack. Safe to call from many threads at once.
    ///
    /// This function is not marked `unsafe`, but it dereferences `item_ptr`.
    /// It breaks (undefined behaviour) when:
    /// - `item_ptr` is null, dangling, or not properly aligned;
    /// - the node is already in a stack, since its `next` pointer is
    ///   overwritten and the stack gets corrupted;
    /// - the node is freed or modified while it is still in the stack.
    fn push_to_stack(&self, item_ptr: *mut T);

    /// Pushes a whole chain with one compare-and-swap.
    ///
    /// `first ..= last` must already be linked through `next`, with `first`
    /// the newest node (the one that ends up at the head). `last.next` is
    /// overwritten. Use [`TreiberNode::link_chain`] to build the chain.
    ///
    /// Useful to publish a batch of items at once: other threads see all of
    /// them or none of them, and the batch stays contiguous and in order.
    ///
    /// Like [`push_to_stack`](Self::push_to_stack), this is not marked `unsafe`
    /// but breaks (undefined behaviour) if any node in the chain is null,
    /// dangling, already in another stack, or freed or modified while it is
    /// still in the stack. `first` and `last` must belong to the same chain.
    fn push_chain(&self, first: *mut T, last: *mut T);
}

impl<T: TreiberNode> GkTreiberPtr<T> for AtomicPtr<T> {
    fn new() -> Self {
        AtomicPtr::new(ptr::null_mut())
    }

    fn new_from_raw(raw_ptr: *mut T) -> Self {
        AtomicPtr::new(raw_ptr)
    }

    fn is_empty(&self) -> bool {
        // A hint only: `reap_all` does the acquire that makes the nodes visible.
        self.load(Ordering::Relaxed).is_null()
    }

    fn reap_all(&self) -> Self {
        // Swapping an empty head still takes the cache line exclusively from
        // the producers, so check first.
        if self.load(Ordering::Relaxed).is_null() {
            return AtomicPtr::new(ptr::null_mut());
        }
        // Acquire pairs with the pushes' Release CAS. The swap only stores null,
        // so it publishes nothing and needs no Release half.
        Self::new_from_raw(self.swap(ptr::null_mut(), Ordering::Acquire))
    }

    fn push_to_stack(&self, item_ptr: *mut T) {
        self.push_chain(item_ptr, item_ptr);
    }

    // Safe by design (see the trait docs): the caller vouches for the pointers, like `push_to_stack`.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn push_chain(&self, first: *mut T, last: *mut T) {
        // A push that succeeds first time never calls the backoff. `spin` (not
        // `snooze`) so a lock-free push never yields its time slice to the OS.
        let backoff = Backoff::new();
        // The head is only stored into our own node, never dereferenced, so
        // Relaxed is enough here and on CAS failure.
        let mut head = self.load(Ordering::Relaxed);
        loop {
            // Safety: the caller owns the chain exclusively until the CAS succeeds.
            unsafe { (*last).next().store(head, Ordering::Relaxed) };
            // Release on success publishes the nodes' `next` and payload to the reaper.
            match self.compare_exchange_weak(head, first, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => return,
                Err(h) => {
                    head = h;
                    backoff.spin();
                }
            }
        }
    }
}

/// A node that can be linked into a Treiber stack.
///
/// The node holds its own `next` pointer, so pushing needs no extra allocation.
pub trait TreiberNode {
    /// The data the node carries.
    type Value;

    /// Returns the node's link to the next node in the chain.
    fn next(&self) -> &AtomicPtr<Self>
    where
        Self: Sized;

    /// Returns the node's data.
    fn value(&self) -> &Self::Value;

    /// Links nodes into a chain for [`GkTreiberPtr::push_chain`].
    ///
    /// `nodes` is given in push order, oldest first. Returns `Some((first, last))`
    /// to pass to `push_chain`, or `None` if `nodes` is empty. After pushing,
    /// [`GkTreiberStackIterator::iterator`] yields the nodes in the order given.
    ///
    /// Every pointer must be valid, appear only once, and not be in any stack.
    /// Nothing else may touch these nodes while they are linked.
    fn link_chain(nodes: impl IntoIterator<Item = *mut Self>) -> Option<(*mut Self, *mut Self)>
    where
        Self: Sized,
    {
        let mut nodes = nodes.into_iter();
        let oldest = nodes.next()?;
        // Safety: the caller guarantees the pointers are valid and unshared.
        unsafe { (*oldest).next().store(ptr::null_mut(), Ordering::Relaxed) };
        let mut newest = oldest;
        for node in nodes {
            unsafe { (*node).next().store(newest, Ordering::Relaxed) };
            newest = node;
        }
        Some((newest, oldest))
    }

    /// Reverses a chain in place and counts its nodes.
    ///
    /// Returns `(new_head, new_tail, count)`. `new_tail` is the old `head`.
    /// For an empty chain (`head` is null) it returns `(null, null, 0)`.
    ///
    /// Every node in the chain must be valid, and no other thread may touch
    /// the chain while it is being reversed.
    fn reverse_in_place(head: *mut Self) -> (*mut Self, *mut Self, usize)
    where
        Self: Sized,
    {
        let mut curr = head;
        let mut prev = core::ptr::null_mut();
        let tail = head;
        let mut count = 0;

        while !curr.is_null() {
            let next_ptr = unsafe { (*curr).next().load(Ordering::Relaxed) };
            unsafe { (*curr).next().store(prev, Ordering::Relaxed) };
            prev = curr;
            curr = next_ptr;
            count += 1;
        }

        (prev, tail, count)
    }

    /// Frees a node that a [`TreiberStackIterator`] was dropped without yielding.
    ///
    /// The default frees it with `Box::from_raw`. Override this if your nodes
    /// are allocated some other way.
    ///
    /// # Safety
    /// `ptr` must be null or a valid node that nothing else still uses.
    unsafe fn drop_node(ptr: *mut Self)
    where
        Self: Sized,
    {
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }
}

// ============================================================================
// Treiber Linked-list Iterator
// ============================================================================

/// Takes nodes off a chain one at a time.
///
/// Each yielded pointer has its `next` cleared, and the caller now owns it.
/// Nodes not yet yielded when the iterator is dropped are freed with
/// [`TreiberNode::drop_node`].
///
/// The iterator owns its chain exclusively, so it walks it with `Relaxed` loads
/// and stores (plain loads and stores on x86 and ARM) and no atomic
/// read-modify-write.
///
/// It is `Send` only if the node type is `Send`, and never `Sync`:
///
/// ```
/// use std::sync::atomic::AtomicPtr;
/// use adaptif_gatotkaca_utils::{TreiberNode, TreiberStackIterator};
///
/// struct Node { next: AtomicPtr<Node>, value: u32 }
/// impl TreiberNode for Node {
///     type Value = u32;
///     fn next(&self) -> &AtomicPtr<Self> { &self.next }
///     fn value(&self) -> &u32 { &self.value }
/// }
///
/// fn assert_send<T: Send>() {}
/// assert_send::<TreiberStackIterator<Node>>();
/// ```
///
/// A node holding an `Rc` is not `Send`, so its iterator can't move to another thread:
///
/// ```compile_fail
/// use std::{rc::Rc, sync::atomic::AtomicPtr};
/// use adaptif_gatotkaca_utils::{TreiberNode, TreiberStackIterator};
///
/// struct Node { next: AtomicPtr<Node>, value: Rc<u32> }
/// impl TreiberNode for Node {
///     type Value = Rc<u32>;
///     fn next(&self) -> &AtomicPtr<Self> { &self.next }
///     fn value(&self) -> &Rc<u32> { &self.value }
/// }
///
/// fn assert_send<T: Send>() {}
/// assert_send::<TreiberStackIterator<Node>>();
/// ```
pub struct TreiberStackIterator<N: TreiberNode> {
    curr: *mut N,
}

// Safety: the iterator owns the chain; moving it moves the nodes and their values.
// Not `Sync`: `next` takes `&mut self`, so there is nothing to share.
unsafe impl<N: TreiberNode + Send> Send for TreiberStackIterator<N> {}

impl<N: TreiberNode> TreiberStackIterator<N> {
    /// Creates an iterator that yields the chain starting at `head`, in chain order.
    ///
    /// # Safety
    ///
    /// `head` must be null or the head of a chain the caller owns exclusively
    /// (for example one returned by `reap_all`). Every node in it must be valid.
    pub unsafe fn from_head(head: *mut N) -> Self {
        Self { curr: head }
    }
}

impl<N: TreiberNode> Iterator for TreiberStackIterator<N> {
    type Item = *mut N;

    fn next(&mut self) -> Option<Self::Item> {
        let ptr = self.curr;
        if ptr.is_null() {
            return None;
        }

        // Safety: nodes of a reaped chain are valid and owned by the iterator.
        let link = unsafe { (*ptr).next() };
        self.curr = link.load(Ordering::Relaxed);
        // The yielded node must not point back into the chain.
        link.store(ptr::null_mut(), Ordering::Relaxed);

        Some(ptr)
    }
}

impl<N: TreiberNode> Drop for TreiberStackIterator<N> {
    fn drop(&mut self) {
        let mut ptr = self.curr;
        self.curr = ptr::null_mut();

        while !ptr.is_null() {
            // Safety: unyielded nodes are valid and owned by the iterator.
            let link = unsafe { (*ptr).next() };
            let next_ptr = link.load(Ordering::Relaxed);
            link.store(ptr::null_mut(), Ordering::Relaxed);

            // Deallocate or return unconsumed node
            unsafe {
                N::drop_node(ptr);
            }

            ptr = next_ptr;
        }
    }
}

/// Turns a chain taken from the stack (for example by
/// [`GkTreiberPtr::reap_all`]) into an iterator.
pub trait GkTreiberStackIterator<N: TreiberNode> {
    /// Yields nodes oldest first (the order they were pushed, FIFO).
    ///
    /// Reverses the chain first, so it makes one extra pass over the nodes.
    fn iterator(self) -> TreiberStackIterator<N>;

    /// Yields nodes newest first (LIFO), the stack's own order. No extra pass.
    fn reversed_iterator(self) -> TreiberStackIterator<N>;
}

impl<N: TreiberNode> GkTreiberStackIterator<N> for AtomicPtr<N> {
    fn iterator(self) -> TreiberStackIterator<N> {
        // The chain is ours after `reap_all`: plain loads and stores, no RMW per node.
        let (head, _tail, _count) = N::reverse_in_place(self.into_inner());
        // Safety: `self` was passed by value, so nothing else can reach the chain.
        unsafe { TreiberStackIterator::from_head(head) }
    }

    fn reversed_iterator(self) -> TreiberStackIterator<N> {
        // Safety: `self` was passed by value, so nothing else can reach the chain.
        unsafe { TreiberStackIterator::from_head(self.into_inner()) }
    }
}

// ============================================================================
// Treiber Queue
// ============================================================================

/// A Treiber stack with an approximate size limit.
///
/// Producers call [`push`](Self::push), which returns `false` once the limit is
/// reached. A consumer takes everything at once with
/// [`reap_all`](Self::reap_all) (which resets the count) or
/// [`reap_all_raw`](Self::reap_all_raw) (which leaves the count for the
/// consumer to decrement through [`grab_counter`](Self::grab_counter)).
///
/// `head` and `count` sit on separate cache lines, so counting and pushing do
/// not slow each other down.
#[derive(Debug)]
pub struct TreiberQueue<N> {
    head: CachePadded<AtomicPtr<N>>,
    count: CachePadded<AtomicUsize>,
    limit: usize,
}

impl<N: TreiberNode> TreiberQueue<N> {
    /// Creates an empty queue that holds about `limit` items.
    pub fn new(limit: usize) -> Self {
        Self {
            head: CachePadded::new(<AtomicPtr<N> as GkTreiberPtr<N>>::new()),
            count: CachePadded::new(AtomicUsize::new(0)),
            limit,
        }
    }

    /// Pushes `item_ptr` onto the queue. Safe to call from many threads at once.
    ///
    /// Returns `true` if it was pushed. Returns `false` if the queue is full;
    /// the item was not pushed and the caller still owns it.
    ///
    /// The limit is approximate. Under a race the count can briefly reach
    /// `limit` plus the number of threads pushing at that moment, before the
    /// rejected pushes subtract again. An item is never pushed unless the
    /// count was below `limit` when it was checked.
    ///
    /// This function is not marked `unsafe`, but it dereferences `item_ptr`.
    /// It breaks (undefined behaviour) when:
    /// - `item_ptr` is null, dangling, or not properly aligned;
    /// - the node is already in a queue or stack, since its `next` pointer is
    ///   overwritten and the chain gets corrupted;
    /// - the node is freed or modified while it is still in the queue.
    pub fn push(&self, item_ptr: *mut N) -> bool {
        // One fetch_add that cannot fail, instead of a CAS retry loop. The
        // limit is approximate anyway. Relaxed is enough: the count publishes
        // no data, the Release CAS in `push_to_stack` does.
        if self.count.fetch_add(1, Ordering::Relaxed) >= self.limit {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        self.head.push_to_stack(item_ptr);
        true
    }

    /// Returns the item counter.
    ///
    /// Use with [`reap_all_raw`](Self::reap_all_raw): subtract from it as
    /// items are consumed so the limit stays accurate.
    pub fn grab_counter(&self) -> &AtomicUsize {
        &self.count
    }

    /// Takes all items and resets the count to 0.
    ///
    /// Taking the whole chain costs one atomic swap, however many items it
    /// holds, instead of one compare-and-swap per item.
    ///
    /// The count is reset just after the swap. A push that lands between the
    /// two is in the queue but not counted, so the count can be slightly low
    /// until the next reap. Use [`reap_all_raw`](Self::reap_all_raw) if you
    /// need an exact count.
    pub fn reap_all(&self) -> AtomicPtr<N> {
        let head = self.head.reap_all();
        // Skip the store when the count is already 0, so a consumer polling an
        // idle queue does not take the counter's cache line from the producers.
        // Relaxed: the count publishes no data (see `len`).
        if self.count.load(Ordering::Relaxed) != 0 {
            self.count.store(0, Ordering::Relaxed);
        }
        head
    }

    /// Takes all items without touching the count.
    ///
    /// The consumer must decrement [`grab_counter`](Self::grab_counter) for
    /// each item it consumes. Useful with several consumer threads or when the
    /// count must be exact.
    pub fn reap_all_raw(&self) -> AtomicPtr<N> {
        self.head.reap_all()
    }

    /// Returns the approximate number of items in the queue.
    ///
    /// This is a hint only. A non-zero count does not make any item visible
    /// to this thread; take the items with `reap_all` or `reap_all_raw`.
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Returns true if the count is 0.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the count has reached the limit.
    pub fn is_full(&self) -> bool {
        self.len() >= self.limit
    }
}

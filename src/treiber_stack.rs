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

use core::{
    ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

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
}

impl<T: TreiberNode> GkTreiberPtr<T> for AtomicPtr<T> {
    fn new() -> Self {
        AtomicPtr::new(ptr::null_mut())
    }

    fn new_from_raw(raw_ptr: *mut T) -> Self {
        AtomicPtr::new(raw_ptr)
    }

    fn is_empty(&self) -> bool {
        self.load(Ordering::Acquire).is_null()
    }

    fn reap_all(&self) -> Self {
        let raw_ptr = self.swap(ptr::null_mut(), Ordering::AcqRel);
        Self::new_from_raw(raw_ptr)
    }

    fn push_to_stack(&self, item_ptr: *mut T) {
        let mut current_head = self.load(Ordering::Acquire);
        loop {
            // Safety: item_ptr is assumed to be exclusively owned here.
            unsafe { (*item_ptr).next().store(current_head, Ordering::Relaxed) };
            // BRANCH: Attempt atomic CAS to push item onto stack
            match self.compare_exchange_weak(
                current_head,
                item_ptr,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                // BRANCH: CAS succeeded, item pushed to stack head
                Ok(_) => break,
                // BRANCH: CAS failed due to concurrent update, retry with new head snapshot
                Err(h) => current_head = h,
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
pub struct TreiberStackIterator<N: TreiberNode> {
    curr: AtomicPtr<N>,
}

impl<N: TreiberNode> Iterator for TreiberStackIterator<N> {
    type Item = *mut N;

    fn next(&mut self) -> Option<Self::Item> {
        let ptr = self.curr.swap(ptr::null_mut(), Ordering::Relaxed);
        if ptr.is_null() {
            return None;
        }

        // Safety: ptr is assumed to be valid since the queue was not empty
        let next_obj = unsafe { (*ptr).next() };

        // Clear the next pointer of the yielded element and advance curr
        let next_ptr = next_obj.swap(ptr::null_mut(), Ordering::Relaxed);
        self.curr.store(next_ptr, Ordering::Relaxed);

        Some(ptr)
    }
}

impl<N: TreiberNode> Drop for TreiberStackIterator<N> {
    fn drop(&mut self) {
        let mut ptr = self.curr.swap(ptr::null_mut(), Ordering::Relaxed);

        while !ptr.is_null() {
            let next_obj = unsafe { (*ptr).next() };

            // Cleanly sever the node links
            let next_ptr = next_obj.swap(ptr::null_mut(), Ordering::Relaxed);

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
        let mut prev = ptr::null_mut();
        let mut curr = self.load(Ordering::Relaxed);

        while !curr.is_null() {
            let next_obj = unsafe { (*curr).next() };

            // Re-link the current node to prev and advance
            let next_ptr = next_obj.swap(prev, Ordering::Relaxed);

            prev = curr;
            curr = next_ptr;
        }

        TreiberStackIterator {
            curr: <AtomicPtr<N> as GkTreiberPtr<N>>::new_from_raw(prev),
        }
    }

    fn reversed_iterator(self) -> TreiberStackIterator<N> {
        TreiberStackIterator { curr: self }
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
#[derive(Debug)]
pub struct TreiberQueue<N> {
    head: AtomicPtr<N>,
    count: AtomicUsize,
    limit: usize,
}

impl<N: TreiberNode> TreiberQueue<N> {
    /// Creates an empty queue that holds about `limit` items.
    pub fn new(limit: usize) -> Self {
        Self {
            head: <AtomicPtr<N> as GkTreiberPtr<N>>::new(),
            count: AtomicUsize::new(0),
            limit,
        }
    }

    /// Pushes `item_ptr` onto the queue. Safe to call from many threads at once.
    ///
    /// Returns `true` if it was pushed. Returns `false` if the queue is full;
    /// the item was not pushed and the caller still owns it.
    ///
    /// This function is not marked `unsafe`, but it dereferences `item_ptr`.
    /// It breaks (undefined behaviour) when:
    /// - `item_ptr` is null, dangling, or not properly aligned;
    /// - the node is already in a queue or stack, since its `next` pointer is
    ///   overwritten and the chain gets corrupted;
    /// - the node is freed or modified while it is still in the queue.
    pub fn push(&self, item_ptr: *mut N) -> bool {
        let mut count = self.count.load(Ordering::Acquire);
        loop {
            if count >= self.limit {
                return false;
            }
            match self.count.compare_exchange_weak(
                count,
                count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(c) => count = c,
            }
        }

        let mut head = self.head.load(Ordering::Acquire);
        loop {
            // Safety: item_ptr is assumed to be exclusively owned here.
            unsafe { (*item_ptr).next().store(head, Ordering::Relaxed) };
            match self.head.compare_exchange_weak(
                head,
                item_ptr,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(h) => head = h,
            }
        }

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
        self.count.store(0, Ordering::Release);
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
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
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

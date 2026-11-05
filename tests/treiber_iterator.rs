//! Single-threaded tests for the Treiber stack iterators. Small enough for Miri.

use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

struct Node {
    next: AtomicPtr<Node>,
    id: u32,
}

impl TreiberNode for Node {
    type Value = u32;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &u32 {
        &self.id
    }
}

fn node(id: u32) -> *mut Node {
    Box::into_raw(Box::new(Node {
        next: AtomicPtr::new(ptr::null_mut()),
        id,
    }))
}

fn stack_of(n: u32) -> AtomicPtr<Node> {
    let stack = AtomicPtr::new(ptr::null_mut());
    for id in 0..n {
        stack.push_to_stack(node(id));
    }
    stack
}

/// Yields and frees every node, checking each one has `next` cleared.
fn collect(it: impl Iterator<Item = *mut Node>) -> Vec<u32> {
    it.map(|p| {
        let n = unsafe { Box::from_raw(p) };
        assert!(n.next.load(Ordering::Relaxed).is_null(), "next not cleared");
        n.id
    })
    .collect()
}

#[test]
fn iterator_is_fifo() {
    let ids = collect(stack_of(10).reap_all().iterator());
    assert_eq!(ids, (0..10).collect::<Vec<_>>());
}

#[test]
fn reversed_iterator_is_lifo() {
    let ids = collect(stack_of(10).reap_all().reversed_iterator());
    assert_eq!(ids, (0..10).rev().collect::<Vec<_>>());
}

#[test]
fn empty_and_single() {
    let empty: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());
    assert!(empty.reap_all().iterator().next().is_none());
    assert_eq!(collect(stack_of(1).reap_all().iterator()), [0]);
    assert_eq!(collect(stack_of(1).reap_all().reversed_iterator()), [0]);
}

// A node type that counts how many times `drop_node` is called.
static DROPPED: AtomicUsize = AtomicUsize::new(0);

struct Counted {
    next: AtomicPtr<Counted>,
}

impl TreiberNode for Counted {
    type Value = ();
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &() {
        &()
    }
    unsafe fn drop_node(ptr: *mut Self) {
        DROPPED.fetch_add(1, Ordering::SeqCst);
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[test]
fn drop_mid_iteration_drops_each_unyielded_node_once() {
    let stack: AtomicPtr<Counted> = AtomicPtr::new(ptr::null_mut());
    for _ in 0..10 {
        stack.push_to_stack(Box::into_raw(Box::new(Counted {
            next: AtomicPtr::new(ptr::null_mut()),
        })));
    }

    let mut it = stack.reap_all().iterator();
    for _ in 0..4 {
        let p = it.next().unwrap();
        drop(unsafe { Box::from_raw(p) }); // yielded: ours to free, not drop_node's
    }
    assert_eq!(DROPPED.load(Ordering::SeqCst), 0);
    drop(it);
    assert_eq!(DROPPED.load(Ordering::SeqCst), 6);
}

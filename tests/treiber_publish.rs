//! Producers publish heap payloads while a consumer reaps concurrently.
//!
//! Small enough to run under Miri, which reports a data race if `reap_all`
//! does not see the producers' writes:
//! `cargo +nightly miri test --test treiber_publish`

use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode, TreiberQueue};
use std::ptr;
use std::sync::atomic::AtomicPtr;

struct Node {
    next: AtomicPtr<Node>,
    // Written with plain (non-atomic) stores before the node is pushed.
    value: Vec<u64>,
}

impl TreiberNode for Node {
    type Value = Vec<u64>;
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &Vec<u64> {
        &self.value
    }
}

fn node(v: u64) -> *mut Node {
    Box::into_raw(Box::new(Node {
        next: AtomicPtr::new(ptr::null_mut()),
        value: vec![v; 3],
    }))
}

struct SendPtr(*mut Node);
unsafe impl Send for SendPtr {}

#[test]
fn reaper_sees_payloads_of_concurrent_pushes() {
    const PRODUCERS: u64 = 3;
    const PER_PRODUCER: u64 = 12;
    // Every third item pushes a chain of two through push_chain.
    const STACK_ITEMS: u64 = PRODUCERS * (PER_PRODUCER + PER_PRODUCER / 3);
    const QUEUE_ITEMS: u64 = PRODUCERS * PER_PRODUCER;

    let stack: AtomicPtr<Node> = GkTreiberPtr::new();
    let queue: TreiberQueue<Node> = TreiberQueue::new(4);

    std::thread::scope(|s| {
        for t in 0..PRODUCERS {
            let (stack, queue) = (&stack, &queue);
            s.spawn(move || {
                for i in 0..PER_PRODUCER {
                    let v = t * 100 + i;
                    if i % 3 == 0 {
                        let (first, last) = Node::link_chain([node(v), node(v)]).unwrap();
                        stack.push_chain(first, last);
                    } else {
                        stack.push_to_stack(node(v));
                    }
                    let item = SendPtr(node(v));
                    while !queue.push(item.0) {
                        std::thread::yield_now();
                    }
                }
            });
        }

        let (mut from_stack, mut from_queue) = (0, 0);
        while from_stack < STACK_ITEMS || from_queue < QUEUE_ITEMS {
            for p in stack.reap_all().iterator() {
                let n = unsafe { Box::from_raw(p) };
                assert_eq!(n.value.len(), 3);
                from_stack += 1;
            }
            for p in queue.reap_all().iterator() {
                let n = unsafe { Box::from_raw(p) };
                assert_eq!(n.value.len(), 3);
                from_queue += 1;
            }
            std::thread::yield_now();
        }
        assert_eq!((from_stack, from_queue), (STACK_ITEMS, QUEUE_ITEMS));
    });

    // A partly consumed iterator frees the rest of its chain on drop.
    for i in 0..5 {
        stack.push_to_stack(node(i));
    }
    let mut it = stack.reap_all().iterator();
    drop(unsafe { Box::from_raw(it.next().unwrap()) });
}

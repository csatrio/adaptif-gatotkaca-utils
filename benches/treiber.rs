//! N producers pushing into one reaper. Run with `cargo bench`.

use adaptif_gatotkaca_utils::{GkTreiberPtr, GkTreiberStackIterator, TreiberNode};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::ptr;
use std::sync::atomic::AtomicPtr;
use std::thread;

struct Node {
    next: AtomicPtr<Node>,
}

impl TreiberNode for Node {
    type Value = ();
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }
    fn value(&self) -> &() {
        &()
    }
}

fn nodes(n: usize) -> Vec<*mut Node> {
    (0..n)
        .map(|_| {
            Box::into_raw(Box::new(Node {
                next: AtomicPtr::new(ptr::null_mut()),
            }))
        })
        .collect()
}

struct SendPtrs(Vec<*mut Node>);
unsafe impl Send for SendPtrs {}

fn bench(c: &mut Criterion) {
    const PER_PRODUCER: usize = 20_000;
    for producers in [1usize, 4, 8] {
        c.bench_function(&format!("push_to_stack/{producers}_producers"), |b| {
            b.iter_batched(
                || {
                    (0..producers)
                        .map(|_| SendPtrs(nodes(PER_PRODUCER)))
                        .collect::<Vec<_>>()
                },
                |batches| {
                    let stack: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());
                    thread::scope(|s| {
                        for batch in batches {
                            let stack = &stack;
                            s.spawn(move || {
                                let batch = batch;
                                for p in batch.0 {
                                    stack.push_to_stack(p);
                                }
                            });
                        }
                    });
                    // iterator() drops (frees) every node.
                    stack
                        .reap_all()
                        .iterator()
                        .for_each(|p| drop(unsafe { Box::from_raw(p) }));
                },
                BatchSize::LargeInput,
            )
        });
    }
}

/// One thread pushes 10k nodes; then the reaper takes them and walks the chain.
fn walk(c: &mut Criterion) {
    const N: usize = 10_000;
    c.bench_function("reap_and_iterate/10k", |b| {
        b.iter_batched(
            || {
                let stack: AtomicPtr<Node> = AtomicPtr::new(ptr::null_mut());
                nodes(N).into_iter().for_each(|p| stack.push_to_stack(p));
                stack
            },
            |stack| {
                // Walks the whole chain; iterator() frees nothing until a node is dropped here.
                let mut count = 0;
                for p in stack.reap_all().iterator() {
                    count += 1;
                    drop(unsafe { Box::from_raw(p) });
                }
                assert_eq!(count, N);
            },
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(benches, bench, walk);
criterion_main!(benches);

// ==== Import Definition ====

use adaptif_gatotkaca_utils::treiber_channel::{
    GK_GROW_NODES, GK_RETURN_BATCH, GK_SLAB_NODES, GkTreiberChannel, GkTreiberChannelConfig,
    GkTreiberChannelError, GkTrySendError,
};

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

// ==== Helpers ====

/// Runs `f` on a thread and fails the test if it takes longer than `seconds` (every test that waits has a bound).
fn within<R: Send + 'static>(seconds: u64, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(Duration::from_secs(seconds))
        .expect("test timed out")
}

/// A payload that counts its drops.
#[derive(Debug)]
struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

// ==== Tests ====

#[test]
fn fifo_single_producer_beyond_the_slab() {
    within(30, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        let total = (GK_SLAB_NODES * 2 + 123) as u64;
        // BRANCH: all sent before any receive, so the slab runs out and heap nodes are used
        for i in 0..total {
            sender.send(i).unwrap();
        }
        assert!(sender.allocated() > GK_SLAB_NODES);
        for i in 0..total {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        assert!(receiver.try_recv().is_none());
    });
}

#[test]
fn many_producers_keep_per_producer_order() {
    within(60, || {
        const PRODUCERS: usize = 8;
        const PER_PRODUCER: u64 = 20_000;
        let channel = GkTreiberChannel::<(usize, u64)>::new();
        let mut receiver = channel.receiver().unwrap();
        let handles: Vec<_> = (0..PRODUCERS)
            .map(|id| {
                let mut sender = channel.sender().unwrap();
                thread::spawn(move || {
                    for seq in 0..PER_PRODUCER {
                        sender.send((id, seq)).unwrap();
                    }
                })
            })
            .collect();
        let mut next = [0u64; PRODUCERS];
        let mut received = 0u64;
        // BRANCH: receive until every sender is gone and the channel is drained
        loop {
            match receiver.recv() {
                Ok((id, seq)) => {
                    assert_eq!(seq, next[id], "producer {id} out of order");
                    next[id] += 1;
                    received += 1;
                }
                Err(error) => {
                    assert_eq!(error, GkTreiberChannelError::DISCONNECTED);
                    break;
                }
            }
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(received, PRODUCERS as u64 * PER_PRODUCER);
    });
}

#[test]
fn warm_sender_reuses_its_slab() {
    within(30, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for i in 0..100_000u64 {
            sender.send(i).unwrap();
            assert_eq!(receiver.recv().unwrap(), i);
        }
        // BRANCH: lockstep traffic never needs more nodes than the slab, and never allocates
        assert_eq!(sender.allocated(), GK_SLAB_NODES);
    });
}

#[test]
fn unbounded_grows_by_chunks_and_shrink_frees_them() {
    within(30, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        let burst = GK_SLAB_NODES + GK_GROW_NODES * 2 + 500;
        for i in 0..burst as u64 {
            sender.send(i).unwrap();
        }
        // BRANCH: the burst outgrew the slab by three chunks, allocated whole
        assert_eq!(sender.allocated(), GK_SLAB_NODES + GK_GROW_NODES * 3);
        // Nothing is home yet: shrink frees nothing.
        assert_eq!(sender.shrink(), 0);
        for i in 0..burst as u64 {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        // The capacity stays (no churn) until shrink frees the chunks, now all at home.
        sender.send(0).unwrap();
        assert_eq!(sender.allocated(), GK_SLAB_NODES + GK_GROW_NODES * 3);
        assert_eq!(receiver.recv().unwrap(), 0);
        assert_eq!(sender.shrink(), GK_GROW_NODES * 3);
        assert_eq!(sender.allocated(), GK_SLAB_NODES);
        // The slab still serves sends.
        for i in 0..100 {
            sender.send(i).unwrap();
            assert_eq!(receiver.recv().unwrap(), i);
        }
    });
}

#[test]
fn senders_take_turns_one_chain_each() {
    within(10, || {
        let channel = GkTreiberChannel::<(u8, u32)>::new();
        let mut a = channel.sender().unwrap();
        let mut b = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for i in 0..3 {
            a.send((0, i)).unwrap();
            b.send((1, i)).unwrap();
        }
        // Each sender's chain is handed out whole, oldest first, before the next sender's.
        let got: Vec<(u8, u32)> = std::iter::from_fn(|| receiver.try_recv()).collect();
        assert_eq!(got.len(), 6);
        let first = got[0].0;
        assert!(got[..3].iter().all(|&(from, _)| from == first));
        assert_eq!(
            got[..3].iter().map(|&(_, n)| n).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            got[3..].iter().map(|&(_, n)| n).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    });
}

#[test]
fn recv_timeout_returns_early_when_a_job_arrives() {
    within(20, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        let start = std::time::Instant::now();
        let consumer = thread::spawn(move || {
            (
                receiver.recv_timeout(Duration::from_secs(10)).unwrap(),
                receiver,
            )
        });
        // BRANCH: let the consumer park first
        thread::sleep(Duration::from_millis(150));
        sender.send(9).unwrap();
        let (value, _receiver) = consumer.join().unwrap();
        assert_eq!(value, 9);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the unpark did not cut the wait short"
        );
    });
}

#[test]
fn receiver_can_park_on_different_threads() {
    within(30, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        // BRANCH: each round parks the receiver on a fresh thread, so the published handle is a different thread every time
        for round in 0..5u64 {
            let consumer = thread::spawn(move || {
                let value = receiver.recv().unwrap();
                (value, receiver)
            });
            thread::sleep(Duration::from_millis(50));
            sender.send(round).unwrap();
            let (value, back) = consumer.join().unwrap();
            assert_eq!(value, round);
            receiver = back;
        }
    });
}

#[test]
fn one_wakeup_serves_many_senders() {
    within(30, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut receiver = channel.receiver().unwrap();
        let senders: Vec<_> = (0..8).map(|_| channel.sender().unwrap()).collect();
        let consumer = thread::spawn(move || {
            let mut count = 0;
            while receiver.recv().is_ok() {
                count += 1;
            }
            count
        });
        // BRANCH: the consumer parks, then eight producers send at once: all jobs arrive, none lost to the single claim
        thread::sleep(Duration::from_millis(100));
        let handles: Vec<_> = senders
            .into_iter()
            .map(|mut sender| {
                thread::spawn(move || (0..1_000).for_each(|i| sender.send(i).unwrap()))
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(consumer.join().unwrap(), 8_000);
    });
}

#[test]
fn bounded_channel_reports_full_and_never_allocates() {
    within(20, || {
        let channel = GkTreiberChannel::<u64>::new_bounded();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for i in 0..GK_SLAB_NODES as u64 {
            sender.try_send(i).unwrap();
        }
        // BRANCH: every job is in flight, so the next send is refused with the value handed back
        assert_eq!(sender.try_send(99), Err(GkTrySendError::Full(99)));
        assert_eq!(sender.allocated(), GK_SLAB_NODES);
        // Nodes go home in batches of `GK_RETURN_BATCH`: one batch received frees that many sends.
        for i in 0..GK_RETURN_BATCH as u64 {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        for i in 0..GK_RETURN_BATCH as u64 {
            sender.try_send(i).unwrap();
        }
        assert_eq!(sender.try_send(8), Err(GkTrySendError::Full(8)));
        assert_eq!(sender.allocated(), GK_SLAB_NODES);
    });
}

#[test]
fn bounded_send_waits_for_the_receiver() {
    within(60, || {
        const TOTAL: u64 = 200_000;
        let channel = GkTreiberChannel::<u64>::new_bounded();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        let producer = thread::spawn(move || {
            for i in 0..TOTAL {
                sender.send(i).unwrap();
            }
            sender.allocated()
        });
        // BRANCH: the producer outruns the receiver many times over; every message still arrives, in order
        for i in 0..TOTAL {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        assert_eq!(producer.join().unwrap(), GK_SLAB_NODES);
    });
}

#[test]
fn bounded_send_gives_up_when_the_receiver_is_dropped() {
    within(20, || {
        let channel = GkTreiberChannel::<u64>::new_bounded();
        let mut sender = channel.sender().unwrap();
        let receiver = channel.receiver().unwrap();
        for i in 0..GK_SLAB_NODES as u64 {
            sender.try_send(i).unwrap();
        }
        let producer = thread::spawn(move || sender.send(1));
        // BRANCH: let the producer reach the wait, then close the channel under it
        thread::sleep(Duration::from_millis(100));
        drop(receiver);
        assert_eq!(producer.join().unwrap(), Err(1));
    });
}

#[test]
fn recv_timeout_returns_timeout_when_empty() {
    within(10, || {
        let channel = GkTreiberChannel::<u64>::new();
        let _sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_millis(50))
                .unwrap_err(),
            GkTreiberChannelError::TIMEOUT
        );
    });
}

#[test]
fn blocked_receiver_is_woken_by_a_send() {
    within(20, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        let consumer = thread::spawn(move || receiver.recv().unwrap());
        // BRANCH: give the consumer time to park before sending
        thread::sleep(Duration::from_millis(150));
        sender.send(77).unwrap();
        assert_eq!(consumer.join().unwrap(), 77);
    });
}

#[test]
fn blocked_receiver_is_woken_by_the_last_sender_dropping() {
    within(20, || {
        let channel = GkTreiberChannel::<u64>::new();
        let sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        let consumer = thread::spawn(move || receiver.recv().unwrap_err());
        thread::sleep(Duration::from_millis(150));
        drop(sender);
        assert_eq!(
            consumer.join().unwrap(),
            GkTreiberChannelError::DISCONNECTED
        );
    });
}

#[test]
fn sent_jobs_survive_the_sender_and_then_disconnect() {
    within(10, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for i in 0..3 {
            sender.send(i).unwrap();
        }
        drop(sender);
        for i in 0..3 {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        assert_eq!(
            receiver.recv().unwrap_err(),
            GkTreiberChannelError::DISCONNECTED
        );
    });
}

#[test]
fn send_after_receiver_dropped_hands_the_value_back() {
    within(10, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut sender = channel.sender().unwrap();
        drop(channel.receiver().unwrap());
        assert_eq!(sender.send(5), Err(5));
        assert_eq!(channel.sender().err(), Some(GkTreiberChannelError::CLOSED));
    });
}

#[test]
fn only_one_receiver() {
    let channel = GkTreiberChannel::<u64>::new();
    let _receiver = channel.receiver().unwrap();
    assert_eq!(
        channel.receiver().err(),
        Some(GkTreiberChannelError::RECEIVER_TAKEN)
    );
}

#[test]
fn recv_moves_the_payload_out() {
    within(10, || {
        let channel = GkTreiberChannel::<String>::new();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        sender.send("hello".to_string()).unwrap();
        assert_eq!(receiver.recv().unwrap(), "hello");
    });
}

#[test]
fn every_payload_is_dropped_exactly_once() {
    within(30, || {
        let drops = Arc::new(AtomicUsize::new(0));
        let sent = Arc::new(AtomicUsize::new(0));
        let channel = GkTreiberChannel::<Tracked>::new();
        let mut receiver = channel.receiver().unwrap();
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let mut sender = channel.sender().unwrap();
                let (drops, sent) = (drops.clone(), sent.clone());
                thread::spawn(move || {
                    for _ in 0..5_000 {
                        sender.send(Tracked(drops.clone())).unwrap();
                        sent.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        // BRANCH: take only part of the jobs, then drop the receiver with the rest still queued or in the chain
        let mut held = Vec::new();
        for _ in 0..3_000 {
            held.push(receiver.recv().unwrap());
        }
        for handle in handles {
            handle.join().unwrap();
        }
        drop(receiver);
        // Jobs held across the receiver's drop go home to channels whose senders are gone.
        drop(held);
        assert_eq!(drops.load(Ordering::SeqCst), sent.load(Ordering::SeqCst));
        assert_eq!(sent.load(Ordering::SeqCst), 20_000);
    });
}

#[test]
fn producer_thread_may_exit_with_jobs_in_flight() {
    within(30, || {
        let channel = GkTreiberChannel::<u64>::new();
        let mut receiver = channel.receiver().unwrap();
        let handles: Vec<_> = (0..4u64)
            .map(|id| {
                let mut sender = channel.sender().unwrap();
                thread::spawn(move || {
                    for i in 0..10_000u64 {
                        sender.send(id * 1_000_000 + i).unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        // All producers are gone before the first receive; their thread channels live on through the jobs.
        let mut held = Vec::new();
        let mut count = 0;
        while let Ok(job) = receiver.recv_timeout(Duration::from_millis(500)) {
            count += 1;
            // BRANCH: keep some jobs alive past the receiver to return them to dead thread channels later
            if count % 7 == 0 {
                held.push(job);
            }
        }
        assert_eq!(count, 40_000);
        drop(receiver);
        drop(held);
    });
}

#[test]
fn config_defaults_match_the_constants() {
    let config = GkTreiberChannelConfig::default();
    assert_eq!(config, GkTreiberChannelConfig::DEFAULT);
    assert_eq!(config.slab_nodes, GK_SLAB_NODES);
    assert_eq!(config.grow_nodes, GK_GROW_NODES);
    assert_eq!(config.return_batch, GK_RETURN_BATCH);
    assert!(!config.bounded);
    assert!(!config.lifo);
    assert!(GkTreiberChannel::<u64>::new_bounded().config().bounded);
}

#[test]
fn invalid_configs_are_rejected() {
    let base = GkTreiberChannelConfig::DEFAULT;
    // BRANCH: one case per rule
    for config in [
        base.with_bounded(true).with_slab_nodes(0),
        base.with_grow_nodes(0),
        base.with_return_batch(0),
    ] {
        assert_eq!(
            GkTreiberChannel::<u64>::with_config(config).err(),
            Some(GkTreiberChannelError::INVALID_CONFIG)
        );
    }
    // An unbounded sender without a slab grows on demand, one chunk at a time.
    let channel =
        GkTreiberChannel::<u64>::with_config(base.with_slab_nodes(0).with_grow_nodes(8)).unwrap();
    let mut sender = channel.sender().unwrap();
    let mut receiver = channel.receiver().unwrap();
    sender.send(1).unwrap();
    assert_eq!(sender.allocated(), 8);
    assert_eq!(receiver.recv().unwrap(), 1);
}

#[test]
fn custom_slab_bounds_the_sender() {
    within(10, || {
        let config = GkTreiberChannelConfig::DEFAULT
            .with_slab_nodes(8)
            .with_bounded(true);
        let channel = GkTreiberChannel::<u64>::with_config(config).unwrap();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for i in 0..8 {
            sender.try_send(i).unwrap();
        }
        assert_eq!(sender.try_send(8), Err(GkTrySendError::Full(8)));
        // Receiving everything empties the chain, which sends the whole batch home.
        for i in 0..8 {
            assert_eq!(receiver.recv().unwrap(), i);
        }
        for i in 0..8 {
            sender.try_send(i).unwrap();
        }
        assert_eq!(sender.allocated(), 8);
    });
}

#[test]
fn node_goes_home_while_the_receiver_still_owns_the_value() {
    within(10, || {
        let drops = Arc::new(AtomicUsize::new(0));
        let config = GkTreiberChannelConfig::DEFAULT
            .with_slab_nodes(2)
            .with_return_batch(1)
            .with_bounded(true);
        let channel = GkTreiberChannel::<Tracked>::with_config(config).unwrap();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        sender.try_send(Tracked(drops.clone())).unwrap();
        sender.try_send(Tracked(drops.clone())).unwrap();
        assert!(matches!(
            sender.try_send(Tracked(drops.clone())),
            Err(GkTrySendError::Full(_))
        ));
        // The full attempt's value was handed back and dropped here.
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        // BRANCH: the receiver keeps the value; its node is already free for the next send
        let held = receiver.recv().unwrap();
        sender.try_send(Tracked(drops.clone())).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        // The value is dropped on the receiver's side, when it chooses.
        drop(held);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(receiver);
        drop(sender);
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    });
}

#[test]
fn returns_are_flushed_when_the_receiver_runs_dry() {
    within(10, || {
        let config = GkTreiberChannelConfig::DEFAULT
            .with_slab_nodes(4)
            .with_return_batch(1_000)
            .with_bounded(true);
        let channel = GkTreiberChannel::<u64>::with_config(config).unwrap();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for round in 0..1_000u64 {
            // BRANCH: fill the bound, then drain it; a batch larger than the bound must still come home
            for i in 0..4 {
                sender.try_send(round * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(receiver.try_recv(), Some(round * 4 + i));
            }
        }
    });
}

/// Small enough for Miri: `cargo +nightly miri test --test treiber_channel small_`.
#[test]
fn small_channel_many_producers_and_endings() {
    let config = GkTreiberChannelConfig::DEFAULT
        .with_slab_nodes(4)
        .with_grow_nodes(3)
        .with_return_batch(3)
        .with_spin_polls(2);
    // BRANCH: bounded and unbounded, receiver dropped last and first
    for (bounded, receiver_first) in [(false, false), (true, false), (false, true)] {
        let channel =
            GkTreiberChannel::<Box<u64>>::with_config(config.with_bounded(bounded)).unwrap();
        let mut receiver = channel.receiver().unwrap();
        let handles: Vec<_> = (0..3u64)
            .map(|id| {
                let mut sender = channel.sender().unwrap();
                thread::spawn(move || {
                    for i in 0..20u64 {
                        // BRANCH: the receiver may already be gone in the receiver-first ending
                        if sender.send(Box::new(id * 100 + i)).is_err() {
                            break;
                        }
                    }
                })
            })
            .collect();
        let mut next = [0u64; 3];
        let limit = if receiver_first { 10 } else { usize::MAX };
        let mut count = 0;
        while count < limit {
            match receiver.recv() {
                Ok(value) => {
                    let (id, seq) = ((*value / 100) as usize, *value % 100);
                    assert_eq!(seq, next[id]);
                    next[id] += 1;
                    count += 1;
                }
                Err(_) => break,
            }
        }
        // BRANCH: drop the receiver while producers may still be sending
        if receiver_first {
            drop(receiver);
            for handle in handles {
                handle.join().unwrap();
            }
        } else {
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(count, 60);
            drop(receiver);
        }
    }
}

// ==== Lifo ====

fn lifo() -> GkTreiberChannelConfig {
    GkTreiberChannelConfig::DEFAULT.with_lifo(true)
}

#[test]
fn lifo_hands_out_each_chain_newest_first() {
    within(10, || {
        let channel = GkTreiberChannel::<u32>::with_config(lifo()).unwrap();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        for i in 0..5 {
            sender.send(i).unwrap();
        }
        let got: Vec<u32> = std::iter::from_fn(|| receiver.try_recv()).collect();
        assert_eq!(got, vec![4, 3, 2, 1, 0]);
        // A new chain is taken once the current one is done.
        sender.send(9).unwrap();
        assert_eq!(receiver.try_recv(), Some(9));
        assert_eq!(receiver.try_recv(), None);
    });
}

/// Every job arrives exactly once, with senders coming and going while the receiver runs.
fn exactly_once_with_churn(config: GkTreiberChannelConfig, producers: u64, per: u64) {
    let channel = GkTreiberChannel::<u64>::with_config(config).unwrap();
    let mut receiver = channel.receiver().unwrap();
    let handles: Vec<_> = (0..producers)
        .map(|p| {
            let channel = channel.clone();
            // Keeps the sender count above zero between the short-lived senders, so the receiver does not see a
            // disconnect early.
            let keeper = channel.sender().unwrap();
            thread::spawn(move || {
                let _keeper = keeper;
                // BRANCH: several short-lived senders per thread, so registration and retirement happen mid-run
                for part in 0..4 {
                    let mut sender = channel.sender().unwrap();
                    for i in part * per / 4..(part + 1) * per / 4 {
                        sender.send(p * per + i).unwrap();
                    }
                }
            })
        })
        .collect();
    drop(channel);
    let mut seen = vec![false; (producers * per) as usize];
    let mut count = 0;
    while let Ok(value) = receiver.recv() {
        assert!(
            !std::mem::replace(&mut seen[value as usize], true),
            "duplicate {value}"
        );
        count += 1;
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(count, producers * per);
}

#[test]
fn lifo_delivers_every_job_exactly_once() {
    within(60, || {
        exactly_once_with_churn(lifo().with_slab_nodes(64).with_grow_nodes(64), 4, 40_000)
    });
}

#[test]
fn fifo_delivers_every_job_exactly_once_with_churn() {
    within(60, || {
        exactly_once_with_churn(
            GkTreiberChannelConfig::DEFAULT
                .with_slab_nodes(64)
                .with_grow_nodes(64),
            4,
            40_000,
        )
    });
}

#[test]
fn many_senders_each_get_a_turn() {
    within(60, || {
        exactly_once_with_churn(lifo().with_slab_nodes(16).with_grow_nodes(16), 40, 2_000);
        exactly_once_with_churn(
            GkTreiberChannelConfig::DEFAULT
                .with_slab_nodes(16)
                .with_grow_nodes(16),
            40,
            2_000,
        );
    });
}

#[test]
fn lifo_receiver_times_out_and_is_woken_like_fifo() {
    within(10, || {
        let channel =
            GkTreiberChannel::<u64>::with_config(lifo().with_single_producer(true)).unwrap();
        let mut sender = channel.sender().unwrap();
        let mut receiver = channel.receiver().unwrap();
        // A short chain, then idle: the pacing must not keep the receiver from parking or timing out.
        sender.send(1).unwrap();
        assert_eq!(receiver.recv().unwrap(), 1);
        let start = Instant::now();
        assert_eq!(
            receiver.recv_timeout(Duration::from_millis(30)),
            Err(GkTreiberChannelError::TIMEOUT)
        );
        assert!(start.elapsed() >= Duration::from_millis(30));
        // Parked, then woken by a send.
        let producer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            sender.send(2).unwrap();
            sender
        });
        assert_eq!(receiver.recv().unwrap(), 2);
        drop(producer.join().unwrap());
        assert_eq!(receiver.recv(), Err(GkTreiberChannelError::DISCONNECTED));
    });
}

#[test]
fn lifo_single_producer_drops_every_payload_once() {
    within(10, || {
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let channel =
                GkTreiberChannel::<Tracked>::with_config(lifo().with_single_producer(true))
                    .unwrap();
            let mut sender = channel.sender().unwrap();
            let mut receiver = channel.receiver().unwrap();
            for _ in 0..10 {
                sender.send(Tracked(drops.clone())).unwrap();
            }
            drop(receiver.try_recv());
            // The rest stay with the receiver and are dropped with it.
        }
        assert_eq!(drops.load(Ordering::SeqCst), 10);
    });
}

/// Small enough for Miri: lifo with senders coming and going, and both endings.
#[test]
fn small_lifo_channel_with_churn() {
    let config = lifo()
        .with_slab_nodes(4)
        .with_grow_nodes(3)
        .with_return_batch(3)
        .with_spin_polls(2);
    // BRANCH: receiver dropped last and first
    for receiver_first in [false, true] {
        let channel = GkTreiberChannel::<Box<u64>>::with_config(config).unwrap();
        let mut receiver = channel.receiver().unwrap();
        let handles: Vec<_> = (0..3u64)
            .map(|id| {
                let channel = channel.clone();
                let keeper = channel.sender().unwrap();
                thread::spawn(move || {
                    let _keeper = keeper;
                    // BRANCH: two short-lived senders per thread
                    for part in 0..2u64 {
                        // BRANCH: the receiver may already be gone in the receiver-first ending
                        let Ok(mut sender) = channel.sender() else {
                            return;
                        };
                        for i in part * 10..(part + 1) * 10 {
                            // BRANCH: the receiver may already be gone in the receiver-first ending
                            if sender.send(Box::new(id * 100 + i)).is_err() {
                                return;
                            }
                        }
                    }
                })
            })
            .collect();
        drop(channel);
        let mut seen = [false; 300];
        let limit = if receiver_first { 10 } else { usize::MAX };
        let mut count = 0;
        while count < limit {
            match receiver.recv() {
                Ok(value) => {
                    assert!(!std::mem::replace(&mut seen[*value as usize], true));
                    count += 1;
                }
                Err(_) => break,
            }
        }
        // BRANCH: drop the receiver while producers may still be sending
        if receiver_first {
            drop(receiver);
            for handle in handles {
                handle.join().unwrap();
            }
        } else {
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(count, 60);
            drop(receiver);
        }
    }
}

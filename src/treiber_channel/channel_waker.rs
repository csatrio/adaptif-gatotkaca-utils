//! How a [`GkReceiver`](crate::GkReceiver) sleeps and is woken.
//!
//! - [`GkWaker`]: the hook, a generic park / unpark pair. Implement it to fit a thread pool or a runtime.
//! - [`GkThreadWaker`]: the default, `std::thread::park` / `Thread::unpark`.
//! - [`GkAsyncWaker`] and [`GkTaskWaker`]: async receive through `std::task::Waker`
//!   ([`GkReceiver::poll_recv`](crate::GkReceiver::poll_recv), [`GkReceiver::recv_async`](crate::GkReceiver::recv_async)),
//!   which works with tokio, smol, async-std or any executor without depending on one.
//!
//! The receiver owns one waker for its whole life (allocated with it), so sleeping never allocates. A sleeping receiver
//! is woken once, by the first sender that finds it asleep, not once per send.

// ==== Import Definition ====

use std::{
    task::Waker,
    thread::{self, Thread},
    time::Duration,
};

// ==== Hook Definition ====

/// A generic park / unpark pair the receiver sleeps with.
///
/// The receiver calls [`prepare`](Self::prepare), publishes the waker, checks the channel once more, and then calls
/// [`park`](Self::park). The first sender that finds the waker published calls [`unpark`](Self::unpark), from its own
/// thread. `unpark` may run before, during or after `park`: an `unpark` that comes first must make the next `park`
/// return at once (as `std::thread::park` does with its token), or `park` must return on its own within a bounded
/// time.
pub trait GkWaker: Send + Sync + 'static {
    /// Receiver side, before each park: capture what `unpark` needs (for example the current thread).
    fn prepare(&mut self);

    /// Receiver side: blocks until `unpark` is called or `timeout` passes. May return early for any reason; the
    /// receiver checks the channel again and parks again if it is still empty.
    fn park(&self, timeout: Option<Duration>);

    /// Sender side, any thread: wakes the receiver.
    fn unpark(&self);
}

/// A [`GkWaker`] that can also wake an async task, for [`GkReceiver::poll_recv`](crate::GkReceiver::poll_recv).
pub trait GkAsyncWaker: GkWaker {
    /// Receiver side, before returning `Pending`: remember the task to wake.
    fn register(&mut self, waker: &Waker);
}

// ==== Thread Waker Definition ====

/// The default [`GkWaker`]: `std::thread::park` and `Thread::unpark`. Follows the receiver if it moves threads.
#[derive(Debug)]
pub struct GkThreadWaker {
    thread: Thread,
}

impl Default for GkThreadWaker {
    fn default() -> Self {
        Self {
            thread: thread::current(),
        }
    }
}

impl GkWaker for GkThreadWaker {
    #[inline]
    fn prepare(&mut self) {
        let me = thread::current();
        // BRANCH: refresh only if the receiver moved to another thread
        if self.thread.id() != me.id() {
            self.thread = me;
        }
    }

    #[inline]
    fn park(&self, timeout: Option<Duration>) {
        match timeout {
            // BRANCH: until unparked (a stored token returns at once)
            None => thread::park(),
            // BRANCH: bounded sleep
            Some(timeout) => thread::park_timeout(timeout),
        }
    }

    #[inline]
    fn unpark(&self) {
        self.thread.unpark();
    }
}

// ==== Task Waker Definition ====

/// A [`GkAsyncWaker`] for async receivers: `unpark` wakes the task registered by the last `poll_recv`.
///
/// It also implements blocking [`park`](GkWaker::park), for a receiver that mixes `recv` and `poll_recv`: the thread
/// then sleeps in short steps of [`step`](Self::step) instead of being unparked.
#[derive(Debug)]
pub struct GkTaskWaker {
    waker: Option<Waker>,
    /// Longest blocking sleep in `park` (default 200 µs).
    pub step: Duration,
}

impl Default for GkTaskWaker {
    fn default() -> Self {
        Self {
            waker: None,
            step: Duration::from_micros(200),
        }
    }
}

impl GkWaker for GkTaskWaker {
    #[inline]
    fn prepare(&mut self) {}

    fn park(&self, timeout: Option<Duration>) {
        thread::sleep(timeout.map_or(self.step, |timeout| timeout.min(self.step)));
    }

    #[inline]
    fn unpark(&self) {
        // BRANCH: a task registered (always, once `poll_recv` returned `Pending`)
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
}

impl GkAsyncWaker for GkTaskWaker {
    #[inline]
    fn register(&mut self, waker: &Waker) {
        // BRANCH: same task as last time is the common case, and costs no clone
        if !self
            .waker
            .as_ref()
            .is_some_and(|known| known.will_wake(waker))
        {
            self.waker = Some(waker.clone());
        }
    }
}

//! An `Arc`-like smart pointer whose reference count lives inside the object.
//!
//! Implement [`GkIntrinsicRefCounted`] for a type that has its own counter
//! field (usually an `AtomicU32`), then manage it with [`GkRefCounted`].
//! Because the count is part of the object, a handle is a single pointer and
//! can be turned into a raw pointer and back without extra allocation.
//!
//! By default the last handle frees the object with `Box`. Override
//! [`GkIntrinsicRefCounted::release`] to free it some other way, for example to
//! return it to an arena.

// ============================================================================
// Import Definition
// ============================================================================
use std::{ops::Deref, sync::atomic::Ordering};

/// Types that store their own reference count, for use with [`GkRefCounted`].
///
/// The counter is usually an `AtomicU32` field. Its starting value doesn't
/// matter: [`GkRefCounted::new`] sets it to 1.
///
/// Override [`release`](Self::release) if the object is not allocated with `Box`.
///
/// ```
/// use std::sync::atomic::{AtomicU32, Ordering};
/// use adaptif_gatotkaca_utils::{GkIntrinsicRefCounted, GkRefCounted};
///
/// struct Node {
///     ref_count: AtomicU32,
///     value: u64,
/// }
///
/// impl GkIntrinsicRefCounted for Node {
///     fn set_ref_count(&self, val: u32, ordering: Ordering) {
///         self.ref_count.store(val, ordering)
///     }
///     fn inc_ref_count(&self, ordering: Ordering) -> u32 {
///         self.ref_count.fetch_add(1, ordering)
///     }
///     fn dec_ref_count(&self, ordering: Ordering) -> u32 {
///         self.ref_count.fetch_sub(1, ordering)
///     }
///     fn get_ref_count(&self, ordering: Ordering) -> u32 {
///         self.ref_count.load(ordering)
///     }
/// }
///
/// let raw = Box::into_raw(Box::new(Node { ref_count: AtomicU32::new(0), value: 5 }));
/// // Safety: `raw` comes from `Box::into_raw` and is not managed elsewhere.
/// let a = unsafe { GkRefCounted::new(raw) };
/// let b = a.clone();
/// assert_eq!(b.value, 5);
/// assert_eq!(a.get_ref_count(Ordering::Relaxed), 2);
/// ```
pub trait GkIntrinsicRefCounted {
    /// Sets the reference count to `val`.
    fn set_ref_count(&self, val: u32, ordering: Ordering);

    /// Atomically adds 1 to the reference count and returns the old value.
    fn inc_ref_count(&self, ordering: Ordering) -> u32;

    /// Atomically subtracts 1 from the reference count and returns the old value.
    fn dec_ref_count(&self, ordering: Ordering) -> u32;

    /// Returns the current reference count.
    fn get_ref_count(&self, ordering: Ordering) -> u32;

    /// Called by the last handle to drop the object and free its memory.
    ///
    /// The default drops and frees it with `Box::from_raw`. Override this if
    /// the object lives somewhere else, such as an arena or a slab, so the
    /// memory goes back to where it came from.
    ///
    /// ```
    /// # use std::sync::atomic::{AtomicU32, Ordering};
    /// # use adaptif_gatotkaca_utils::{GkIntrinsicRefCounted, GkRefCounted};
    /// struct Slot {
    ///     ref_count: AtomicU32,
    /// }
    ///
    /// impl GkIntrinsicRefCounted for Slot {
    /// #   fn set_ref_count(&self, val: u32, o: Ordering) { self.ref_count.store(val, o) }
    /// #   fn inc_ref_count(&self, o: Ordering) -> u32 { self.ref_count.fetch_add(1, o) }
    /// #   fn dec_ref_count(&self, o: Ordering) -> u32 { self.ref_count.fetch_sub(1, o) }
    /// #   fn get_ref_count(&self, o: Ordering) -> u32 { self.ref_count.load(o) }
    ///     unsafe fn release(ptr: *mut Self) {
    ///         // Return the slot to your arena here instead of freeing it.
    ///         // This example just runs the destructor; the memory is a static.
    ///         unsafe { std::ptr::drop_in_place(ptr) };
    ///     }
    /// }
    ///
    /// let slot = Box::leak(Box::new(Slot { ref_count: AtomicU32::new(0) }));
    /// // Safety: the slot is valid and `release` does not free it with Box.
    /// let handle = unsafe { GkRefCounted::new(slot) };
    /// drop(handle);
    /// ```
    ///
    /// # Safety
    ///
    /// Only called by [`GkRefCounted`] when the count reaches zero. `ptr` is
    /// valid and no other handle exists. The implementation must not use `ptr`
    /// after releasing it.
    unsafe fn release(ptr: *mut Self)
    where
        Self: Sized,
    {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

/// A shared handle to a heap object that counts its own references.
///
/// Cloning a handle increments the object's count, and dropping one
/// decrements it. When the last handle is dropped, the object is released with
/// [`GkIntrinsicRefCounted::release`] (by default, dropped and freed with `Box`).
///
/// Like `Arc`, the handle is `Send` and `Sync` only if `T` is both `Send` and
/// `Sync`, because any handle can reach the object from any thread.
///
/// There is no `DerefMut`. Use [`get_mut`](Self::get_mut) when you hold the
/// only handle, or the unsafe [`as_mut`](Self::as_mut) otherwise. For shared
/// mutation, put atomics or a lock inside `T`.
#[derive(Debug)]
#[repr(transparent)]
pub struct GkRefCounted<T: GkIntrinsicRefCounted> {
    ptr: *const T,
}

unsafe impl<T: GkIntrinsicRefCounted + Send + Sync> Send for GkRefCounted<T> {}
unsafe impl<T: GkIntrinsicRefCounted + Send + Sync> Sync for GkRefCounted<T> {}

impl<T: GkIntrinsicRefCounted> GkRefCounted<T> {
    /// Takes ownership of `ptr` and sets its reference count to 1.
    ///
    /// # Safety
    ///
    /// - `ptr` must point to a valid, initialized `T` that stays valid until
    ///   the last handle is dropped.
    /// - `ptr` must be compatible with [`GkIntrinsicRefCounted::release`]. With
    ///   the default `release`, it must come from `Box::into_raw`.
    /// - `ptr` must not already be managed by another `GkRefCounted`.
    pub unsafe fn new(ptr: *const T) -> Self {
        unsafe {
            (*ptr).set_ref_count(1, Ordering::Release);
        }
        Self { ptr }
    }

    /// Returns the raw pointer to the object without changing the count.
    pub fn as_ptr(&self) -> *const T {
        self.ptr
    }

    /// Returns a mutable reference if this is the only handle, otherwise `None`.
    ///
    /// Safe equivalent of `Arc::get_mut`. Taking `&mut self` also stops this
    /// handle from being cloned while the reference is alive.
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        if this.get_ref_count(Ordering::Acquire) == 1 {
            // Safety: the count is 1 and we hold `&mut` to the only handle.
            Some(unsafe { &mut *(this.ptr as *mut T) })
        } else {
            None
        }
    }

    /// Returns a mutable reference to the object, even if other handles exist.
    ///
    /// Prefer [`get_mut`](Self::get_mut). Use this only when you know the
    /// other handles will not touch the object meanwhile.
    ///
    /// # Safety
    ///
    /// For the lifetime of the returned reference, nothing else may read or
    /// write the object, including through other handles or threads.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn as_mut(&self) -> &mut T {
        unsafe { &mut *(self.ptr as *mut T) }
    }

    /// Creates a handle from a raw pointer without incrementing the count.
    ///
    /// Use this to take back a pointer returned by [`GkRefCounted::into_raw`].
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live object whose count already includes this
    /// handle, as with a pointer from `into_raw`. Creating a handle from any
    /// other pointer makes the count wrong and can free the object too early.
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        Self { ptr }
    }

    /// Consumes the handle and returns its raw pointer without decrementing the count.
    ///
    /// The object stays alive. Pass the pointer to [`GkRefCounted::from_raw`]
    /// later so it can be released.
    pub fn into_raw(self) -> *const T {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }
}

impl<T: GkIntrinsicRefCounted> Clone for GkRefCounted<T> {
    /// Increments the count and returns another handle to the same object.
    fn clone(&self) -> Self {
        unsafe {
            (*self.ptr).inc_ref_count(Ordering::Relaxed);
        }
        Self { ptr: self.ptr }
    }
}

impl<T: GkIntrinsicRefCounted> Drop for GkRefCounted<T> {
    /// Decrements the count. If this was the last handle, calls
    /// [`GkIntrinsicRefCounted::release`] to drop and free the object.
    fn drop(&mut self) {
        unsafe {
            if (*self.ptr).dec_ref_count(Ordering::AcqRel) == 1 {
                T::release(self.ptr as *mut T);
            }
        }
    }
}

impl<T: GkIntrinsicRefCounted> Deref for GkRefCounted<T> {
    type Target = T;

    /// Returns a shared reference to the object.
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::sync::{Arc, Mutex};

    struct Obj {
        ref_count: AtomicU32,
        value: u32,
        log: Arc<Mutex<Vec<&'static str>>>,
        custom_release: bool,
    }

    impl GkIntrinsicRefCounted for Obj {
        fn set_ref_count(&self, v: u32, o: Ordering) {
            self.ref_count.store(v, o)
        }
        fn inc_ref_count(&self, o: Ordering) -> u32 {
            self.ref_count.fetch_add(1, o)
        }
        fn dec_ref_count(&self, o: Ordering) -> u32 {
            self.ref_count.fetch_sub(1, o)
        }
        fn get_ref_count(&self, o: Ordering) -> u32 {
            self.ref_count.load(o)
        }
        unsafe fn release(ptr: *mut Self) {
            let custom = unsafe { (*ptr).custom_release };
            unsafe { (*ptr).log.lock().unwrap().push("release") };
            if custom {
                // Pretend the memory belongs to an arena: only run the destructor.
                // The test frees the allocation itself.
                unsafe { std::ptr::drop_in_place(ptr) };
            } else {
                drop(unsafe { Box::from_raw(ptr) });
            }
        }
    }

    fn make(custom: bool) -> (*mut Obj, Arc<Mutex<Vec<&'static str>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let raw = Box::into_raw(Box::new(Obj {
            ref_count: AtomicU32::new(0),
            value: 1,
            log: log.clone(),
            custom_release: custom,
        }));
        (raw, log)
    }

    #[test]
    fn last_handle_calls_release_once() {
        let (raw, log) = make(false);
        let a = unsafe { GkRefCounted::new(raw) };
        let b = a.clone();
        drop(a);
        assert!(log.lock().unwrap().is_empty());
        drop(b);
        assert_eq!(*log.lock().unwrap(), ["release"]);
    }

    #[test]
    fn custom_release_replaces_box_free() {
        let (raw, log) = make(true);
        let h = unsafe { GkRefCounted::new(raw) };
        drop(h);
        assert_eq!(*log.lock().unwrap(), ["release"]);
        // The default Box free was skipped, so we free the memory ourselves.
        unsafe {
            std::alloc::dealloc(raw as *mut u8, std::alloc::Layout::new::<Obj>());
        }
    }

    #[test]
    fn get_mut_only_when_unique() {
        let (raw, _log) = make(false);
        let mut a = unsafe { GkRefCounted::new(raw) };
        GkRefCounted::get_mut(&mut a).unwrap().value = 9;
        let b = a.clone();
        assert!(GkRefCounted::get_mut(&mut a).is_none());
        drop(b);
        assert_eq!(GkRefCounted::get_mut(&mut a).unwrap().value, 9);
    }

    #[test]
    fn into_raw_from_raw_keeps_count() {
        let (raw, log) = make(false);
        let a = unsafe { GkRefCounted::new(raw) };
        let ptr = a.into_raw();
        assert!(log.lock().unwrap().is_empty());
        let a = unsafe { GkRefCounted::from_raw(ptr) };
        assert_eq!(a.get_ref_count(Ordering::Relaxed), 1);
    }
}

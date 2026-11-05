//! An `Arc`-like smart pointer whose reference count lives inside the object.
//!
//! Implement [`GkIntrinsicRefCounted`] for a type that has its own counter
//! field (usually an `AtomicU32`), then manage it with [`GkRefCounted`].
//! Because the count is part of the object, a handle is a single pointer and
//! can be turned into a raw pointer and back without extra allocation.

// ============================================================================
// Import Definition
// ============================================================================
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::Ordering,
};

/// Types that store their own reference count, for use with [`GkRefCounted`].
///
/// The counter is usually an `AtomicU32` field. Its starting value doesn't
/// matter: [`GkRefCounted::new`] sets it to 1.
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
/// let a = GkRefCounted::new(raw);
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
}

/// A shared handle to a heap object that counts its own references.
///
/// Cloning a handle increments the object's count, and dropping one
/// decrements it. When the last handle is dropped, the object is dropped and
/// its memory freed with `Box`.
#[derive(Debug)]
#[repr(transparent)]
pub struct GkRefCounted<T: GkIntrinsicRefCounted> {
    ptr: *const T,
}

unsafe impl<T: GkIntrinsicRefCounted> Send for GkRefCounted<T> {}
unsafe impl<T: GkIntrinsicRefCounted> Sync for GkRefCounted<T> {}

impl<T: GkIntrinsicRefCounted> GkRefCounted<T> {
    /// Takes ownership of `ptr` and sets its reference count to 1.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `ptr` comes from `Box::into_raw`, because
    /// the last handle frees it with `Box::from_raw`. `ptr` must not already be
    /// managed by another `GkRefCounted`.
    pub fn new(ptr: *const T) -> Self {
        unsafe {
            (*ptr).set_ref_count(1, Ordering::Release);
        }
        Self { ptr }
    }

    /// Returns the raw pointer to the object without changing the count.
    pub fn as_ptr(&self) -> *const T {
        self.ptr
    }

    /// Returns a mutable reference to the object.
    ///
    /// # Safety
    ///
    /// The caller must ensure that nothing else reads or writes the object
    /// while the returned reference is alive, including through other handles.
    pub fn as_mut(&self) -> &mut T {
        unsafe { &mut *(self.ptr as *mut T) }
    }

    /// Creates a handle from a raw pointer without incrementing the count.
    ///
    /// Use this to take back a pointer returned by [`GkRefCounted::into_raw`].
    ///
    /// # Safety
    ///
    /// The caller must ensure that `ptr` points to a live object whose count
    /// includes this handle, as with a pointer from `into_raw`.
    pub fn from_raw(ptr: *const T) -> Self {
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
    /// Decrements the count. If this was the last handle, drops the object
    /// and frees its memory with `Box::from_raw`.
    fn drop(&mut self) {
        unsafe {
            if (*self.ptr).dec_ref_count(Ordering::AcqRel) == 1 {
                drop(Box::from_raw(self.ptr as *mut T));
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

impl<T: GkIntrinsicRefCounted> DerefMut for GkRefCounted<T> {
    /// Returns a mutable reference to the object.
    ///
    /// Other handles to the same object may still exist, so the caller must
    /// make sure none of them are used while this reference is alive.
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.ptr as *mut T) }
    }
}

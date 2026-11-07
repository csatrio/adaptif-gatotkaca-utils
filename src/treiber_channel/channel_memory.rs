//! Where a [`GkTreiberChannel`](crate::GkTreiberChannel) allocates.
//!
//! - [`GkChannelMemory`]: the hook. The channel allocates everything through it: its shared state, every sender's
//!   thread channel and slab, overflow nodes and the receiver's waker.
//! - [`GkHeapMemory`]: the default, the global allocator.
//! - [`GkRegionMemory`]: an allocator over a memory region you map yourself (`mmap`, huge pages, a shared mapping, a
//!   plain buffer). You pass its start and length; the crate never calls an OS API. Many channels can share one region.
//!
//! The hook is only called on slow paths (creating a channel, a sender or the receiver, an unbounded send beyond the
//! slab), never by a warm send or receive.

// ==== Import Definition ====

use crossbeam_utils::Backoff;

use std::{
    alloc::{self, Layout},
    cell::UnsafeCell,
    fmt, mem,
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

// ==== Hook Definition ====

/// Memory a channel lives in.
///
/// Every clone of a handle must allocate from, and free to, the same pool: memory allocated through one clone may be
/// freed through another, on any thread.
///
/// # Safety
/// Implementations must return memory that is valid for reads and writes of `layout.size()` bytes, aligned to
/// `layout.align()`, not used by anything else, and that stays valid until it is passed to
/// [`deallocate`](Self::deallocate) (on any clone of the handle). The channel never asks for zero bytes.
pub unsafe trait GkChannelMemory: Clone + Send + Sync + 'static {
    /// Memory for `layout`, or `None` if there is none left. Must not panic.
    fn allocate(&self, layout: Layout) -> Option<NonNull<u8>>;

    /// Gives back memory that `allocate` returned.
    ///
    /// # Safety
    /// `ptr` must come from `allocate` on a clone of this handle with the same `layout`, and must not be used again.
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout);
}

/// The global allocator. The default memory of a channel.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct GkHeapMemory;

// Safety: `std::alloc` meets the contract; the channel never asks for zero bytes.
unsafe impl GkChannelMemory for GkHeapMemory {
    #[inline]
    fn allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        debug_assert!(layout.size() > 0);
        // Safety: the size is not zero.
        NonNull::new(unsafe { alloc::alloc(layout) })
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // Safety: forwarded from this function's contract.
        unsafe { alloc::dealloc(ptr.as_ptr(), layout) }
    }
}

// ==== Region Definition ====

/// Why a region could not be set up.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GkRegionError {
    /// The start address is not aligned to [`GkRegionMemory::ALIGN`].
    Misaligned,
    /// The region is smaller than [`GkRegionMemory::HEADER_LEN`].
    TooSmall,
}

impl fmt::Display for GkRegionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Misaligned => "region start is misaligned",
            Self::TooSmall => "region is smaller than its header",
        })
    }
}

impl std::error::Error for GkRegionError {}

/// Exact-size free lists kept in the header. A channel uses a handful of block shapes per payload type, so 32 lists
/// cover many channel types sharing one region.
const REGION_CLASSES: usize = 32;

/// One free list: blocks of exactly `size` bytes and `align` alignment. `size == 0` marks an unused slot.
#[derive(Clone, Copy)]
struct GkRegionClass {
    size: usize,
    align: usize,
    /// First free block, or null. A free block holds the next one in its first word.
    head: *mut u8,
}

/// Lives at the start of the region.
#[repr(C)]
struct GkRegionHeader {
    len: usize,
    /// Spin lock over `bump` and `classes`.
    lock: AtomicBool,
    /// Bytes handed out and not given back (rounded block sizes).
    in_use: AtomicUsize,
    /// Offset of the first never-used byte.
    bump: UnsafeCell<usize>,
    classes: UnsafeCell<[GkRegionClass; REGION_CLASSES]>,
}

/// A [`GkChannelMemory`] over a region you supply: its start address and length.
///
/// The region keeps its bookkeeping in a [`HEADER_LEN`](Self::HEADER_LEN)-byte header at the start. Blocks come from a
/// bump pointer and, once freed, are reused by later allocations of the same size and alignment (one free list per
/// shape, up to 32 shapes; a freed block of a 33rd shape is not reused). Allocation takes a spin lock, which suits the
/// slow paths the channel calls it from. The handle is `Copy`.
///
/// ```
/// use adaptif_gatotkaca_utils::{GkRegionMemory, GkThreadWaker, GkTreiberChannel, GkTreiberChannelConfig};
/// use std::ptr::NonNull;
///
/// // Stand-in for an `mmap`: any memory you own that outlives the channels.
/// let mut buffer = vec![0u64; 64 * 1024];
/// let base = NonNull::new(buffer.as_mut_ptr().cast::<u8>()).unwrap();
/// // Safety: the buffer outlives the channels and nothing else uses it.
/// let region = unsafe { GkRegionMemory::new(base, buffer.len() * 8) }.unwrap();
///
/// let config = GkTreiberChannelConfig::DEFAULT.with_slab_nodes(64).with_grow_nodes(64);
/// // Many channels can live in one region.
/// let channels: Vec<_> = (0..8)
///     .map(|_| GkTreiberChannel::<u64, GkThreadWaker, _>::with_memory(config, region).unwrap())
///     .collect();
/// let mut sender = channels[3].sender().unwrap();
/// let mut receiver = channels[3].receiver().unwrap();
/// sender.send(7).unwrap();
/// assert_eq!(receiver.recv().unwrap(), 7);
/// drop((sender, receiver, channels));
/// assert_eq!(region.in_use(), 0);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GkRegionMemory {
    base: NonNull<u8>,
}

// Safety: the handle is an address; every access to the bookkeeping goes through the header's lock or atomics.
unsafe impl Send for GkRegionMemory {}
unsafe impl Sync for GkRegionMemory {}

impl GkRegionMemory {
    /// Required alignment of the region start.
    pub const ALIGN: usize = mem::align_of::<GkRegionHeader>();
    /// Bytes the header takes at the start of the region.
    pub const HEADER_LEN: usize = mem::size_of::<GkRegionHeader>();

    /// Sets up `len` bytes at `base` as an empty region and returns a handle to it. Whatever the memory held is lost.
    ///
    /// # Safety
    /// `base .. base + len` must be valid for reads and writes, must be used only through handles of this region, and
    /// must stay mapped while any handle, or anything allocated from it, is in use.
    pub unsafe fn new(base: NonNull<u8>, len: usize) -> Result<Self, GkRegionError> {
        // BRANCH: the header must fit, aligned, at the start
        if base.as_ptr().addr() % Self::ALIGN != 0 {
            return Err(GkRegionError::Misaligned);
        }
        if len < Self::HEADER_LEN {
            return Err(GkRegionError::TooSmall);
        }
        // Safety: the header fits and is aligned; the caller gives us the region exclusively.
        unsafe {
            base.as_ptr().cast::<GkRegionHeader>().write(GkRegionHeader {
                len,
                lock: AtomicBool::new(false),
                in_use: AtomicUsize::new(0),
                bump: UnsafeCell::new(Self::HEADER_LEN),
                classes: UnsafeCell::new(
                    [GkRegionClass {
                        size: 0,
                        align: 0,
                        head: std::ptr::null_mut(),
                    }; REGION_CLASSES],
                ),
            })
        };
        Ok(Self { base })
    }

    /// Start of the region.
    pub fn base(&self) -> NonNull<u8> {
        self.base
    }

    /// Region length in bytes, header included.
    pub fn len(&self) -> usize {
        self.header().len
    }

    /// Always false: a region holds at least its header.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Bytes currently handed out (block sizes as rounded by the allocator). 0 once everything is freed.
    pub fn in_use(&self) -> usize {
        self.header().in_use.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn header(&self) -> &GkRegionHeader {
        // Safety: a handle exists only for a set-up region that stays mapped (see `new`).
        unsafe { &*self.base.as_ptr().cast::<GkRegionHeader>() }
    }

    /// Size and alignment a block really takes: room and alignment for the free-list link.
    #[inline(always)]
    fn block_of(layout: Layout) -> (usize, usize) {
        let align = layout.align().max(mem::align_of::<usize>());
        let size = layout.size().max(mem::size_of::<usize>());
        (size.next_multiple_of(align), align)
    }

    fn lock(&self) -> GkRegionGuard<'_> {
        let header = self.header();
        let backoff = Backoff::new();
        // BRANCH: retry until the lock is ours
        while header
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            backoff.snooze();
        }
        GkRegionGuard { header }
    }
}

/// Releases the region lock when dropped.
struct GkRegionGuard<'a> {
    header: &'a GkRegionHeader,
}

impl Drop for GkRegionGuard<'_> {
    fn drop(&mut self) {
        self.header.lock.store(false, Ordering::Release);
    }
}

// Safety: blocks come from the region, sized and aligned per `block_of`, and are handed out once until freed; the lock
// serialises every change to the bookkeeping.
unsafe impl GkChannelMemory for GkRegionMemory {
    fn allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        let (size, align) = Self::block_of(layout);
        let guard = self.lock();
        // Safety: `bump` and `classes` are only touched under the lock.
        let (bump, classes) = unsafe { (&mut *guard.header.bump.get(), &mut *guard.header.classes.get()) };
        // BRANCH: reuse a freed block of the same shape first
        if let Some(class) = classes
            .iter_mut()
            .find(|class| class.size == size && class.align == align && !class.head.is_null())
        {
            let block = class.head;
            // Safety: a free block holds the next one in its first word.
            class.head = unsafe { block.cast::<*mut u8>().read() };
            guard.header.in_use.fetch_add(size, Ordering::Relaxed);
            return NonNull::new(block);
        }
        let start = self.base.as_ptr().addr();
        let offset = (start + *bump).next_multiple_of(align) - start;
        let end = offset.checked_add(size)?;
        // BRANCH: the region is full
        if end > guard.header.len {
            return None;
        }
        *bump = end;
        guard.header.in_use.fetch_add(size, Ordering::Relaxed);
        NonNull::new(self.base.as_ptr().wrapping_add(offset))
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let (size, align) = Self::block_of(layout);
        let guard = self.lock();
        // Safety: `classes` is only touched under the lock.
        let classes = unsafe { &mut *guard.header.classes.get() };
        guard.header.in_use.fetch_sub(size, Ordering::Relaxed);
        // BRANCH: the list of this shape, else claim an unused slot for it
        let slot = classes
            .iter()
            .position(|class| class.size == size && class.align == align)
            .or_else(|| classes.iter().position(|class| class.size == 0));
        // BRANCH: no slot left for a 33rd shape; the block is not reused (documented)
        let Some(index) = slot else {
            return;
        };
        let class = &mut classes[index];
        class.size = size;
        class.align = align;
        // Safety: the block is ours again, at least a word long and aligned for one.
        unsafe { ptr.as_ptr().cast::<*mut u8>().write(class.head) };
        class.head = ptr.as_ptr();
    }
}

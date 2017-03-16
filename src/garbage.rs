//! Garbage collection TODO: How about "Deferred free"?
//!
//! TODO: Explain how it relates to `Stash`.
//!
//! # Objects and bags
//!
//! Objects that get unlinked from lock-free data structures (or otherwise become unreachable) are
//! garbage, and they get stashed away until the global epoch sufficiently advances so that they
//! become safe to be freed. Pointers to garbage objects are stored in bags.
//!
//! # Life of a bag
//!
//! Each thread during it's registration creates a new thread-local bag. Any garbage objects the
//! thread produces are added to it's thread-local bag.
//!
//! If a bag gets full, whether because it contains too many objects (`MAX_OBJECTS`) or the objects
//! total too many bytes (`MAX_BYTES`), it is retired and replaced with a new, fresh bag. The old
//! one then gets pushed into one of the two global queues (normal and urgent queue).
//!
//! Threads on exit retire their thread-local bags by pushing them into global queues.
//!
//! # The garbage queues
//!
//! All bags eventually end up in one of the two global garbage queues. Threads call `collect()`
//! from time to time in order to help reduce the amount of accumulated global garbage.
//!
//! There are two global queues:
//!
//! 1. Normal queue: most bags end up here.
//! 2. Urgent queue: bags whose objects total unusually large amounts of bytes end up here.
//!
//! The urgent queue is particularly important when large arrays become garbage, for example when
//! resizing hash tables or arrays backing worker-stealer queues grow or shrink. When that happens,
//! an unusally large bag is detected, which gets pushed into the urgent queue.
//!
//! If the urgent queue is non-empty, the global state is flagged with urgency mode. Every pinning
//! first checks for urgency mode. In case of urgency `collect()` gets called once. This continues
//! until the urgent queue becomes empty and we return back to normal mode.

use std::mem;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

use super::epoch;
use super::{Atomic, Pin};

/// Maximal number of objects a bag can contain.
const MAX_OBJECTS: usize = 64;

/// A bag is full when objects total at least this many bytes.
const FULL_BYTES: usize = 1 << 16; // 16 KiB

/// A bag must be urgently freed when objects total at least this many bytes.
const URGENT_BYTES: usize = 1 << 20; // 1 MiB

/// Number of bags per queue that are freed on each call to `collect`.
const COLLECT_STEPS: usize = 8;

/// The normal garbage queue, where most bags end up.
static NORMAL_QUEUE: AtomicUsize = ATOMIC_USIZE_INIT;

/// The urgent garbage queue, where unusally large bags end up.
static URGENT_QUEUE: AtomicUsize = ATOMIC_USIZE_INIT;

/// Holds unlinked objects that will be eventually freed.
pub struct Bag {
    /// Number of objects in the bag.
    len: AtomicUsize,

    /// Total memory in bytes occupied by the objects in the bag.
    total_bytes: usize,

    /// Unlinked objects.
    ///
    /// The tuples consist of:
    ///
    /// 1. Function that reclaims memory.
    /// 2. Pointer to allocated array of objects.
    /// 3. Number of objects in the array.
    objects: [(unsafe fn(*mut u8, usize), *mut u8, usize); MAX_OBJECTS],

    /// The global epoch at the moment when this bag got pushed into the queue.
    epoch: usize,

    /// The next bag in the garbage queue.
    next: Atomic<Bag>,
}

impl Bag {
    /// Returns a new, empty bag.
    pub fn new() -> Self {
        Bag {
            len: AtomicUsize::new(0),
            total_bytes: 0,
            objects: unsafe { mem::uninitialized() },
            epoch: 0,
            next: Atomic::null(),
        }
    }

    /// Attempts to insert an object into the bag, and returns `true` on success.
    ///
    /// The object is stored in memory at address `ptr` and consists of `count` elements. The
    /// attempt might fail because the bag is full (already holds many or large objects).
    pub fn try_insert<T>(&mut self, ptr: *mut T, count: usize) -> bool {
        if self.is_full() {
            return false;
        }

        unsafe fn free<T>(ptr: *mut u8, count: usize) {
            // Free the memory, but don't execute destructors.
            drop(Vec::from_raw_parts(ptr as *mut T, 0, count));
        }

        self.objects[self.len.fetch_add(1, Relaxed)] = (free::<T>, ptr as *mut u8, count);
        self.total_bytes += mem::size_of::<T>() * count;
        true
    }

    /// Returns true if the bag is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len.load(Relaxed) == self.objects.len() || self.total_bytes >= FULL_BYTES
    }

    /// Frees the memory occupied by all objects stored in the bag.
    ///
    /// Note: can be called only once!
    unsafe fn free_all_objects(&self) {
        for &(free, ptr, count) in self.objects.iter().take(self.len.load(Relaxed)) {
            free(ptr, count);
        }
    }
}

/// A garbage queue.
///
/// This is a global queue where thread-local bags of unlinked objects eventually end up.
/// The implementation is based on the typical Michael-Scott queue.
#[repr(C)]
struct Queue {
    /// Head of the queue (always a sentinel entry).
    head: Atomic<Bag>,
    /// Padding to avoid false sharing.
    _pad: [u8; 64],
    /// Tail of the queue.
    tail: Atomic<Bag>,
}

impl Queue {
    /// Returns a new, empty garbage queue.
    ///
    /// This function is only called when initializing the singleton.
    fn new() -> Self {
        let queue = Queue {
            head: Atomic::null(),
            _pad: unsafe { mem::uninitialized() },
            tail: Atomic::null(),
        };

        // This code is executing while a thread harness is initializing, so normal pinning would
        // try to access it while it is being initialized. Such accesses fail with a panic.
        // We cheat our way around this by creating a fake pin.
        let pin = unsafe { &mem::zeroed::<Pin>() };

        // The head of the queue is always a sentinel entry.
        let sentinel = queue.head.store_box(Box::new(Bag::new()), Relaxed, pin);
        queue.tail.store(sentinel, Relaxed);

        queue
    }

    /// Returns true if the queue is empty.
    fn is_empty(&self, pin: &Pin) -> bool {
        self.head.load(Acquire, pin).unwrap().next.load(Relaxed, pin).is_null()
    }

    /// Pushes a bag into the queue.
    ///
    /// The bag must be marked with an epoch beforehand.
    fn push(&self, mut bag: Box<Bag>, pin: &Pin) {
        let mut tail = self.tail.load(Acquire, pin);
        loop {
            let next = tail.unwrap().next.load(Acquire, pin);

            if next.is_null() {
                // Try installing the new bag.
                match tail.unwrap().next.cas_box_weak(next, bag, AcqRel) {
                    Ok(bag) => {
                        // Tail pointer shouldn't fall behind. Let's move it forward.
                        self.tail.cas(tail, bag, Release);
                        break;
                    }
                    Err((t, b)) => {
                        tail = t;
                        bag = b;
                    }
                }
            } else {
                // This is not the actual tail. Move the tail pointer forward.
                match self.tail.cas_weak(tail, next, AcqRel) {
                    Ok(()) => tail = next,
                    Err(t) => tail = t,
                }
            }
        }
    }

    /// Attempts to pop a bag from the front of the queue and returns it if `condition` is met.
    ///
    /// If the bag in the front doesn't meet it or if the queue is empty, `None` is returned.
    fn try_pop_if<'p, F>(&self, condition: F, pin: &'p Pin) -> Option<&'p Bag>
        where F: Fn(&Bag) -> bool
    {
        let mut head = self.head.load(Acquire, pin);
        loop {
            let next = head.unwrap().next.load(Acquire, pin);

            match next.as_ref() {
                Some(n) if condition(n) => {
                    // Try unlinking the head by moving it forward.
                    match self.head.cas_weak(head, next, AcqRel) {
                        Ok(()) => {
                            unsafe { head.defer_free(pin) }
                            // The unlinked head was just a sentinel.
                            // It's successor is the real head.
                            return Some(n);
                        }
                        Err(h) => head = h,
                    }
                }
                None | Some(_) => return None,
            }
        }
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        // This code is executing while a thread harness is initializing, so normal pinning would
        // try to access it while it is being initialized. Such accesses fail with a panic.
        // We cheat our way around this by creating a fake pin.
        let pin = unsafe { &mem::zeroed::<Pin>() };

        let mut head = self.head.load(Acquire, pin);

        while let Some(h) = head.as_ref() {
            let next = h.next.load(Relaxed, pin);

            if let Some(n) = next.as_ref() {
                // Because the head of the queue is a sentinel entry, we only free garbage
                // contained by successors.
                unsafe { n.free_all_objects() }
            }

            // Deallocate and move forward.
            unsafe { drop(Vec::from_raw_parts(h as *const _ as *mut Bag, 0, 1)) }
            head = next;
        }
    }
}

/// Returns a reference to a global garbage queue stored at `atomic`.
///
/// The queue is lazily initialized on the first call to this function.
fn singleton(atomic: &'static AtomicUsize) -> &'static Queue {
    let current = atomic.load(Acquire);

    let queue = if current == 0 {
        // Initialize the singleton.
        let raw = Box::into_raw(Box::new(Queue::new()));
        let new = raw as usize;
        let previous = atomic.compare_and_swap(0, new, AcqRel);

        if previous == 0 {
            // Ok, we initialized it.
            new
        } else {
            // Another thread has already initialized it.
            unsafe { drop(Box::from_raw(raw)); }
            previous
        }
    } else {
        current
    };

    unsafe { &*(queue as *const Queue) }
}

/// Pushes a bag into one of the global queues.
pub fn push(mut bag: Box<Bag>, pin: &Pin) {
    bag.epoch = epoch::load().0;

    if bag.total_bytes < URGENT_BYTES {
        singleton(&NORMAL_QUEUE).push(bag, pin);
    } else {
        singleton(&URGENT_QUEUE).push(bag, pin);
        epoch::set_urgency(true);
    }
}

/// Frees several bags from the queue.
///
/// This function should be called when we have some cycles to spare, and it must be called at
/// least as often as `push`. Because it collects more than one bag of garbage, the speed of
/// collection is thus faster than the speed of garbage generation.
#[cold]
pub fn collect(pin: &Pin) {
    let (epoch, is_urgent) = epoch::load();

    let condition = |bag: &Bag| {
        // A pinned thread can witness at most two epoch advancements. Therefore, any bag that is
        // within two epochs of the current one cannot be freed yet.
        epoch::distance(epoch, bag.epoch) > 2
    };

    let normal = singleton(&NORMAL_QUEUE);
    let urgent = singleton(&URGENT_QUEUE);

    for queue in &[normal, urgent] {
        // Collect several bags.
        for _ in 0..COLLECT_STEPS {
            match normal.try_pop_if(&condition, pin) {
                None => break,
                Some(bag) => unsafe { bag.free_all_objects() },
            }
        }
    }

    // Did we just resolve the urgency? If so, clear the urgency flag.
    if is_urgent && urgent.is_empty(pin) {
        epoch::set_urgency(false);

        // Did some other thread just push a new bag into the urgent queue? If so, switch back to
        // urgency mode.
        if !urgent.is_empty(pin) {
            epoch::set_urgency(true);
        }
    }
}

//! Garbage collection
//!
//! # Bags and garbage queues
//!
//! Objects that get removed from concurrent data structures must be stashed away until the global
//! epoch sufficiently advances so that they become safe for destruction. Pointers to such garbage
//! objects are kept in bags.
//!
//! When a bag becomes full, it is marked with the current global epoch pushed into a `Garbage`
//! queue. Usually each instance of concurrent data structure has it's own `Garbage` queue that
//! gets fully destroyed as soon as the data structure gets dropped.
//!
//! Whenever a bag is pushed into the queue, some garbage is collected and destroyed along the way.
//! Garbage collection can also be manually triggered by calling method `collect`.
//!
//! # The global garbage queue
//!
//! Some data structures don't own objects but merely transfer them between threads, e.g. queues.
//! As such, queues don't execute destructors - they only allocate and free some memory. it would
//! be costly for each queue to handle it's own `Garbage`, so there is a special global queue all
//! data structures can share.
//!
//! The global garbage queue is very efficient. Each thread has a thread-local bag that is
//! populated with garbage, and when it becomes full, it is finally pushed into queue. This design
//! reduces contention on data structures. The global queue cannot be explicitly accessed - the
//! only way to interact with it is by calling function `defer_free`.

use std::cell::UnsafeCell;
use std::cmp;
use std::mem;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};

use {Atomic, Pin, defer_free};

/// Maximum number of objects a bag can contain.
const MAX_OBJECTS: usize = 64;

/// Number of bags to destroy when collecting garbage.
const COLLECT_STEPS: usize = 8;

/// The global epoch.
///
/// The last bit in this number is unused and is always zero. Every so often the global epoch is
/// incremented, i.e. we say it "advances". A pinned thread may advance the global epoch only if
/// all currently pinned threads have been pinned in the current epoch.
///
/// If an object became garbage in some epoch, then we can be sure that after two advancements no
/// thread will hold a reference to it. This is the crux of safe memory reclamation.
pub static EPOCH: AtomicUsize = ATOMIC_USIZE_INIT;

/// Returns the distance between two epochs.
///
/// For example, distance between adjacent epochs is 1.
#[inline]
pub fn distance(epoch1: usize, epoch2: usize) -> usize {
    let diff = epoch1.wrapping_sub(epoch2);
    cmp::min(diff, 0usize.wrapping_sub(diff)) / 2
}

/// Holds removed objects that will be eventually destroyed.
pub struct Bag {
    /// Number of objects in the bag.
    len: AtomicUsize,
    /// Removed objects.
    objects: [UnsafeCell<(unsafe fn(*mut u8), *mut u8)>; MAX_OBJECTS],
    /// The global epoch at the moment when this bag got pushed into the queue.
    epoch: usize,
    /// The next bag in the queue.
    next: Atomic<Bag>,
}

impl Bag {
    /// Returns a new, empty bag.
    pub fn new() -> Self {
        Bag {
            len: AtomicUsize::new(0),
            objects: unsafe { mem::uninitialized() },
            epoch: unsafe { mem::uninitialized() },
            next: Atomic::null(),
        }
    }

    /// Attempts to insert a garbage object into the bag and returns `true` if succeeded.
    pub fn try_insert<T>(&self, destroy: unsafe fn(*mut T), object: *mut T) -> bool {
        // If the object is null, just pretend it was successfully inserted.
        if object.is_null() {
            return true;
        }

        // Erase type `*mut T` and use `*mut u8` instead.
        let destroy: unsafe fn(*mut u8) = unsafe { mem::transmute(destroy) };
        let object = object as *mut u8;

        let mut len = self.len.load(Acquire);
        loop {
            // Is the bag full?
            if len == self.objects.len() {
                return false;
            }

            // Try incrementing `len`.
            match self.len.compare_exchange_weak(len, len + 1, AcqRel, Acquire) {
                Ok(_) => {
                    // Success! Now store the garbage object into the array. The current thread
                    // will synchronize with the thread that destroys it through epoch advancement.
                    unsafe { *self.objects[len].get() = (destroy, object) }
                    return true;
                }
                Err(l) => len = l,
            }
        }
    }

    /// Destroys all objects in the bag.
    ///
    /// Note: can be called only once!
    unsafe fn destroy_all_objects(&self) {
        for cell in self.objects.iter().take(self.len.load(Relaxed)) {
            let (destroy, object) = *cell.get();
            unsafe { destroy(object) }
        }
    }
}

/// A garbage queue.
///
/// This is where a concurrent data structure can store removed objects for deferred destruction.
pub struct Garbage {
    /// Head of the queue (always a sentinel entry).
    head: Atomic<Bag>,
    /// Tail of the queue.
    tail: Atomic<Bag>,
    /// The next bag that will be pushed into the queue, as soon as it gets full.
    pending: Atomic<Bag>,
}

impl Garbage {
    /// Returns a new, empty garbage queue.
    pub fn new() -> Self {
        let garbage = Garbage {
            head: Atomic::null(),
            tail: Atomic::null(),
            pending: Atomic::null(),
        };

        // This code may be executing while a thread harness is initializing, so normal pinning
        // would try to access it while it is being initialized. Such accesses fail with a panic.
        // We cheat our way around this by creating a fake pin.
        let pin = unsafe { &mem::zeroed::<Pin>() };

        // The head of the queue is always a sentinel entry.
        let sentinel = garbage.head.store_box(Box::new(Bag::new()), Relaxed, pin);
        garbage.tail.store(sentinel, Relaxed);

        garbage
    }

    /// Adds an `object` that will later be freed.
    pub unsafe fn defer_free<T>(&self, object: *mut T, pin: &Pin) {
        unsafe fn free<T>(ptr: *mut T) {
            // Free the memory, but don't run the destructor.
            drop(Vec::from_raw_parts(ptr, 0, 1));
        }
        self.defer_destroy(free, object, pin);
    }

    /// Adds an `object` that will later be dropped and freed.
    ///
    /// Note: The object must be `Send + Sync + 'self`.
    pub unsafe fn defer_drop<T>(&self, object: *mut T, pin: &Pin) {
        unsafe fn destruct<T>(ptr: *mut T) {
            // Run the destructor and free the memory.
            drop(Vec::from_raw_parts(ptr, 1, 1));
        }
        self.defer_destroy(destruct, object, pin);
    }

    /// Adds an `object` that will later be destroyed using `destroy`.
    ///
    /// Note: The object must be `Send + Sync + 'self`.
    pub unsafe fn defer_destroy<T>(
        &self,
        destroy: unsafe fn(*mut T),
        object: *mut T,
        pin: &Pin
    ) {
        loop {
            let pending = self.pending.load(Acquire, pin);
            match pending.as_ref() {
                None => {
                    // There is no pending bag. Try installing a fresh one.
                    self.pending.cas_box(pending, Box::new(Bag::new()), Release);
                }
                Some(p) => {
                    if p.try_insert(destroy, object) {
                        // Successfully inserted the object.
                        break;
                    } else {
                        // We couldn't insert, the bag is full. Try installing a fresh bag and
                        // pushing the old one into the queue.
                        if self.pending.cas_box(pending, Box::new(Bag::new()), AcqRel).is_ok() {
                            // Success! Push the bag into the queue and collect some garbage.
                            let mut bag = unsafe { Box::from_raw(pending.as_raw()) };
                            self.push(bag, pin);
                            self.collect(pin);
                        }
                    }
                }
            }
        }
    }

    /// Collects some garbage and destroys it.
    ///
    /// Generally speaking, it's not necessary to call this method because garbage production
    /// already triggers garbage destruction. However, if there are long periods without garbage
    /// production, it might be a good idea to call this method from time to time.
    pub fn collect(&self, pin: &Pin) {
        let epoch = EPOCH.load(SeqCst);
        let condition = |bag: &Bag| {
            // A pinned thread can witness at most two epoch advancements. Therefore, any bag that
            // is within two epochs of the current one cannot be destroyed yet.
            distance(epoch, bag.epoch) > 2
        };

        for _ in 0..COLLECT_STEPS {
            match self.try_pop_if(&condition, pin) {
                None => break,
                Some(bag) => unsafe { bag.destroy_all_objects() },
            }
        }
    }

    /// Pushes a bag into the queue.
    fn push(&self, mut bag: Box<Bag>, pin: &Pin) {
        // Mark the bag with the current epoch.
        bag.epoch = EPOCH.load(SeqCst);

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
                    // Try moving the head forward.
                    match self.head.cas_weak(head, next, AcqRel) {
                        Ok(()) => {
                            // The old head may later be freed.
                            unsafe { defer_free(head.as_raw(), pin) }
                            // The new head holds the popped value (heads are sentinels!).
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

impl Drop for Garbage {
    fn drop(&mut self) {
        // This code may be executing while a thread harness is initializing, so normal pinning
        // would try to access it while it is being initialized. Such accesses fail with a panic.
        // We cheat our way around this by creating a fake pin.
        let pin = unsafe { &mem::zeroed::<Pin>() };

        // TODO: pending bag (may be null) - CAS it!
        // TODO: go in windows of 1 nodes
        // TODO: CAS the head!
    }
}

/// Returns a reference to a global garbage, which is lazily initialized.
fn global() -> &'static Garbage {
    static GLOBAL: AtomicUsize = ATOMIC_USIZE_INIT;

    let current = GLOBAL.load(Acquire);

    let garbage = if current == 0 {
        // Initialize the singleton.
        let raw = Box::into_raw(Box::new(Garbage::new()));
        let new = raw as usize;
        let previous = GLOBAL.compare_and_swap(0, new, AcqRel);

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

    unsafe { &*(garbage as *const Garbage) }
}

/// Pushes a bag into the global garbage.
pub fn push(bag: Box<Bag>, pin: &Pin) {
    global().push(bag, pin);
}

/// Collects several bags from the global queue and destroys their objects.
#[cold]
pub fn collect(pin: &Pin) {
    global().collect(pin);
}

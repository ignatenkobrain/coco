//! TODO: Explain how global garbage and bags work
//!
//! # Life of a bag
//!
//! TODO
//!
//! # The garbage queue
//!
//! TODO

use super::Atomic;
use super::Pin;
use super::thread::dummy_pin;

use std::cell::Cell;
use std::mem;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};

// TODO: place here all the magic numbers (constants)

/// TODO: document
static GARBAGE: AtomicUsize = ATOMIC_USIZE_INIT;

/// Contains unlinked objects that will be eventually freed.
pub struct Bag {
    /// Number of objects in the bag.
    len: Cell<usize>,

    /// Total memory in bytes occupied by the objects in the bag.
    total_bytes: Cell<usize>,

    /// Unlinked objects.
    ///
    /// The tuples consist of:
    ///
    /// 1. Function that reclaims memory.
    /// 2. Pointer to allocated array of objects.
    /// 3. Number of objects in the array.
    objects: [(unsafe fn(*mut u8, usize), *mut u8, usize); 64],

    /// The global epoch at a moment after all contained objects were unlinked.
    epoch: usize,

    /// The next bag in the garbage queue.
    next: Atomic<Bag>,
}

impl Bag {
    /// Returns a new, empty bag.
    pub fn new() -> Self {
        Bag {
            len: Cell::new(0),
            total_bytes: Cell::new(0),
            objects: unsafe { mem::uninitialized() },
            epoch: 0,
            next: Atomic::null(),
        }
    }

    /// Attempts to insert an object into the bag, and returns `true` on success.
    ///
    /// The object is stored in memory at address `ptr` and consists of `count` elements. The
    /// attempt might fail because the bag is full or already holds too much garbage.
    pub fn try_insert<T>(&mut self, ptr: *mut T, count: usize) -> bool {
        let len = self.len.get();
        let total_bytes = self.total_bytes.get();

        if len == self.objects.len() || total_bytes >= 1 << 16 {
            false
        } else {
            unsafe fn free<T>(ptr: *mut u8, count: usize) {
                // Free the memory, but don't execute destructors.
                drop(Vec::from_raw_parts(ptr as *mut T, 0, count));
            }
            self.objects[len] = (free::<T>, ptr as *mut u8, count);

            self.len.set(len + 1);
            self.total_bytes.set(total_bytes + mem::size_of::<T>() * count);

            true
        }
    }

    /// Frees the memory occupied by all objects stored in the bag and then empties the bag.
    unsafe fn free_all_objects(&self) {
        for &(free, ptr, count) in self.objects.iter().take(self.len.get()) {
            free(ptr, count);
        }
        self.len.set(0);
        self.total_bytes.set(0);
    }
}

/// A garbage queue.
///
/// This is the global queue where thread-local bags of unlinked objects eventually end up.
/// The implementation is based on the typical Michael-Scott queue.
#[repr(C)]
struct Garbage {
    /// Head of the queue.
    ///
    /// The head is always a sentinel entry.
    head: Atomic<Bag>,

    /// Padding to avoid false sharing.
    _pad: [u8; 64],

    /// Tail of the queue.
    tail: Atomic<Bag>,
}

impl Garbage {
    /// Returns a new, empty garbage queue.
    ///
    /// This function is only called when initializing the singleton.
    fn new() -> Self {
        let garbage = Garbage {
            head: Atomic::null(),
            _pad: unsafe { mem::uninitialized() },
            tail: Atomic::null(),
        };

        unsafe {
            // This code is executing while a thread harness is initializing, so normal pinning
            // would try to access it while it is being initialized. Such accesses fail with a
            // panic.
            dummy_pin(|pin| {
                // The head of the queue is always a sentinel entry.
                let sentinel = garbage.head.store_box(Box::new(Bag::new()), Relaxed, pin);
                garbage.tail.store(sentinel, Relaxed);
            });
        }

        garbage
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

    /// Pops a bag from the front of the queue and returns it if the `condition` is met.
    ///
    /// If the bag on the front doesn't meet it or if the queue is empty, `None` is returned.
    fn pop_if<'p, F>(&self, condition: F, pin: &'p Pin) -> Option<&'p Bag>
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
                            unsafe { head.unlinked(pin) }
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

impl Drop for Garbage {
    fn drop(&mut self) {
        unsafe {
            // This code is executing while a thread harness is initializing, so normal pinning
            // would try to access it while it is being initialized. Such accesses fail with a
            // panic.
            dummy_pin(|pin| {
                let mut head = self.head.load(Acquire, pin);

                while let Some(h) = head.as_ref() {
                    let next = h.next.load(Relaxed, pin);

                    if let Some(n) = next.as_ref() {
                        // Because the head of the queue is a sentinel entry, we only free garbage
                        // contained by successors.
                        n.free_all_objects();
                    }

                    // Deallocate and move forward.
                    drop(Vec::from_raw_parts(h as *const _ as *mut Bag, 0, 1));
                    head = next;
                }
            })
        }
    }
}

/// Returns a reference to the global garbage queue.
///
/// The queue is lazily initialized on the first call to this function.
fn garbage() -> &'static Garbage {
    let current = GARBAGE.load(Acquire);

    let garbage = if current == 0 {
        // Initialize garbage.
        let raw = Box::into_raw(Box::new(Garbage::new()));
        let new = raw as usize;
        let previous = GARBAGE.compare_and_swap(0, new, AcqRel);

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

/// Pushes a bag into the global garbage queue and marks it with the current global epoch.
pub fn push(mut bag: Box<Bag>, epoch: usize, pin: &Pin) {
    let garbage = garbage();
    bag.epoch = epoch;
    garbage.push(bag, pin);
}

/// Collects several bags of garbage from the queue and frees them.
///
/// The argument `epoch` is the current global epoch.
///
/// This function should be called when we have some cycles to spare, and it must be called at
/// least as often as `push`. Because it collects more than one bag of garbage, the speed of
/// collection is thus faster than the speed of garbage generation.
pub fn collect(epoch: usize, pin: &Pin) {
    let garbage = garbage();

    let condition = |bag: &Bag| {
        // A pinned thread can witness at most two epoch advancements. Therefore, any bag that is
        // within two epochs of the current one cannot be freed yet.
        let diff = epoch.wrapping_sub(bag.epoch);
        diff > 4 && diff < 0usize.wrapping_sub(4)
    };

    // Collect several bags.
    for _ in 0..8 {
        match garbage.pop_if(&condition, pin) {
            None => break,
            Some(bag) => unsafe { bag.free_all_objects() },
        }
    }
}

//! Thread synchronization and pinning.
//!
//! # Registration
//!
//! In order to track all threads in one place, we need some form of thread registration. Every
//! thread has a thread-local so-called "harness" that registers it the first time it is pinned,
//! and unregisters when it exits.
//!
//! Registered threads are tracked in a global lock-free singly-linked list of thread entries. The
//! head of this list is accessed by calling the `participants` function.
//!
//! # Thread entries
//!
//! Thread entries are implemented as the `Thread` data type. Every entry contains an integer that
//! tells whether the thread is pinned and if so, what was the global epoch at the time it was
//! pinned. Entries also hold a pin counter that aids in periodic global epoch advancement.

use std::cell::Cell;
use std::mem;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};

use epoch::TaggedAtomic;
use epoch::garbage::{self, Bag, EPOCH};

thread_local! {
    /// The thread registration harness.
    ///
    /// The harness is lazily initialized on it's first use. Initialization performs registration.
    /// If initialized, the harness will get destructed on thread exit, which in turn unregisters
    /// the thread.
    static HARNESS: Harness = Harness {
        thread: Thread::register(),
        is_pinned: Cell::new(false),
        pin_count: Cell::new(0),
        bag: Cell::new(Box::into_raw(Box::new(Bag::new()))),
    };
}

/// Holds thread-local data and unregisters the thread when dropped.
struct Harness {
    /// This thread's entry in the participants list.
    thread: *const Thread,
    /// Whether the thread is currently pinned.
    is_pinned: Cell<bool>,
    /// Total number of pinnings performed.
    pin_count: Cell<usize>,
    /// The local bag of objects that will be later freed.
    bag: Cell<*mut Bag>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Now that the thread is exiting, we must move the local bag into the global garbage
        // queue. Also, let's try advancing the epoch and help free some garbage.
        let thread = unsafe { &*self.thread };

        // If we called `pin()` here, it would try to access `HARNESS` and then panic.
        // To work around the problem, we manually pin the thread.
        let pin = &Pin { bag: &self.bag };
        thread.set_pinned(pin);

        // Spare some cycles on garbage collection.
        // Note: This may itself produce garbage and in turn allocate new bags.
        try_advance(pin);
        garbage::collect(pin);

        // Push the local bag into the global garbage queue.
        let bag = unsafe { Box::from_raw(self.bag.get()) };
        garbage::push(bag, pin);

        // Manually unpin the thread.
        thread.set_unpinned();

        // Mark the thread entry as deleted.
        thread.unregister();
    }
}

/// An entry in the linked list of participating threads.
struct Thread {
    /// The least significant bit is set if the thread is currently pinned. The rest of the bits
    /// encode the current epoch.
    state: AtomicUsize,
    /// The next thread in the linked list of participants. If the tag is 1, this entry is deleted
    /// and can be unlinked from the list.
    next: TaggedAtomic<Thread>,
}

impl Thread {
    /// Marks the thread as pinned.
    ///
    /// Must not be called if the thread is already pinned!
    #[inline]
    fn set_pinned(&self, _pin: &Pin) {
        let epoch = EPOCH.load(Relaxed);
        // Now we must store `epoch` into `self.state`. It's important that any succeeding loads
        // don't get reordered with this store. In order words, this thread's epoch must be fully
        // announced to other threads. Only then it becomes safe to load from the shared memory.
        store_with_fence(&self.state, epoch | 1);

        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        fn store_with_fence(dest: &AtomicUsize, value: usize) {
            dest.store(value, Relaxed);
            ::std::sync::atomic::fence(SeqCst);
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        fn store_with_fence(dest: &AtomicUsize, value: usize) {
            // On x86 architectures we have a choice:
            // 1. `atomic::fence(SeqCst)`, which compiles to a `mfence` instruction.
            // 2. `compare_and_swap(_, _, SeqCst)`, which compiles to a `lock cmpxchg` instruction.
            //
            // Both instructions have the effect of a full barrier, but the second one seems to be
            // faster in this particular case.
            let old = dest.load(Relaxed);
            dest.compare_and_swap(old, value, SeqCst);
        }
    }

    /// Marks the thread as unpinned.
    #[inline]
    fn set_unpinned(&self) {
        // Clear the last bit.
        // We don't need to preserve the epoch, so just store the number zero.
        self.state.store(0, Release);
    }

    /// Registers a thread by adding a new entry to the list of participanting threads.
    ///
    /// Returns a pointer to the newly allocated entry.
    fn register() -> *mut Thread {
        let list = participants();

        let mut new = Box::new(Thread {
            state: AtomicUsize::new(0),
            next: TaggedAtomic::null(0),
        });

        // This code is executing while the thread harness is initializing, so normal pinning would
        // try to access it while it is being initialized. Such accesses fail with a panic. We must
        // therefore cheat by creating a fake pin.
        let pin = unsafe { &mem::zeroed::<Pin>() };

        let mut head = list.load(Acquire, pin);
        loop {
            new.next.store(head, Relaxed);

            // Try installing this thread's entry as the new head.
            match list.cas_box_weak(head, new, 0, AcqRel) {
                Ok(n) => return n.as_raw(),
                Err((h, n)) => {
                    head = h;
                    new = n;
                }
            }
        }
    }

    /// Unregisters the thread by marking it's entry as deleted.
    ///
    /// This function doesn't physically remove the entry from the linked list, though. That will
    /// do a future call to `try_advance`.
    fn unregister(&self) {
        // This code is executing while the thread harness is initializing, so normal pinning would
        // try to access it while it is being initialized. Such accesses fail with a panic. We must
        // therefore cheat by creating a fake pin.
        let pin = unsafe { &mem::zeroed::<Pin>() };

        // Simply mark the next-pointer in this thread's entry.
        let mut next = self.next.load(Acquire, pin);
        while next.tag() == 0 {
            match self.next.cas_weak(next, next.with_tag(1), AcqRel) {
                Ok(()) => break,
                Err(n) => next = n,
            }
        }
    }
}

/// Returns a reference to the head pointer of the list of participating threads.
fn participants() -> &'static TaggedAtomic<Thread> {
    static PARTICIPANTS: AtomicUsize = ATOMIC_USIZE_INIT;
    unsafe { &*(&PARTICIPANTS as *const _ as *const _) }
}

/// Attempts to advance the global epoch.
///
/// The global epoch can advance only if all currently pinned threads have been pinned in the
/// current epoch.
#[cold]
fn try_advance(pin: &Pin) {
    let epoch = EPOCH.load(SeqCst);

    // Traverse the linked list of participating threads.
    let mut pred = participants();
    let mut curr = pred.load(Acquire, pin);

    while let Some(c) = curr.as_ref() {
        let succ = c.next.load(Acquire, pin);

        if succ.tag() == 1 {
            // This thread has exited. Try unlinking it from the list.
            let succ = succ.with_tag(0);

            if pred.cas(curr, succ, Release).is_err() {
                // We lost the race to unlink the thread. Usually that means we should traverse the
                // list again from the beginning, but since another thread trying to advance the
                // epoch has won the race, we leave the job to that one.
                return;
            }

            // The unlinked entry can be later freed.
            unsafe { defer_free(c as *const _ as *mut Thread, pin) }

            // Move forward, but don't change the predecessor.
            curr = succ;
        } else {
            let thread_state = c.state.load(SeqCst);
            let thread_is_pinned = thread_state & 1 == 1;
            let thread_epoch = thread_state & !1;

            // If the thread was pinned in a different epoch, we cannot advance the global epoch
            // just yet.
            if thread_is_pinned && thread_epoch != epoch {
                return;
            }

            // Move one step forward.
            pred = &c.next;
            curr = succ;
        }
    }

    // All pinned threads were pinned in the current global epoch.
    // Finally, try advancing the epoch. We increment by 2 and simply wrap around on overflow.
    EPOCH.compare_and_swap(epoch, epoch.wrapping_add(2), SeqCst);
}

/// A witness that the current thread is pinned.
///
/// A reference to `Pin` is proof that the current thread is pinned. Lots of methods that interact
/// with [`Atomic`]s can safely be called only while the thread is pinned so they often require a
/// reference to `Pin`.
///
/// This data type is inherently bound to the thread that created it, therefore it does not
/// implement `Send` nor `Sync`.
///
/// [`Atomic`]: struct.Atomic.html
#[derive(Debug)]
pub struct Pin {
    /// A pointer to the cell within the harness, which holds a pointer to the local bag.
    ///
    /// This pointer is kept within `Pin` as a matter of convenience. It could also be reached
    /// through the harness itself, but that doesn't work if we're in the process of it's
    /// destruction.
    bag: *const Cell<*mut Bag>, // !Send + !Sync
}

/// Pins the current thread.
///
/// The provided function takes a reference to a `Pin`, which can be used to interact with
/// [`Atomic`]s. The pin serves as a proof that whatever data you load from an [`Atomic`] will not
/// be concurrently deleted by another thread while the pin is alive.
///
/// Note that keeping a thread pinned for a long time prevents memory reclamation of any newly
/// deleted objects protected by [`Atomic`]s. The provided function should be very quick -
/// generally speaking, it shouldn't take more than 100 ms.
///
/// Pinning is reentrant. There is no harm in pinning a thread while it's already pinned (repinning
/// is essentially a noop).
///
/// Pinning itself comes with a price: it begins with a `SeqCst` fence and performs a few other
/// atomic operations. However, this mechanism is designed to be as performant as possible, so it
/// can be used pretty liberally. On a modern machine pinning takes 10 to 15 nanoseconds.
///
/// [`Atomic`]: struct.Atomic.html
pub fn pin<F, T>(f: F) -> T
    where F: FnOnce(&Pin) -> T
{
    /// Number of pinnings after which a thread will collect some global garbage.
    const PINS_BETWEEN_COLLECT: usize = 128;

    HARNESS.with(|harness| {
        let thread = unsafe { &*harness.thread };
        let pin = &Pin { bag: &harness.bag };

        let was_pinned = harness.is_pinned.get();
        if !was_pinned {
            // Pin the thread.
            harness.is_pinned.set(true);
            thread.set_pinned(pin);

            // Increment the pin counter.
            let count = harness.pin_count.get();
            harness.pin_count.set(count.wrapping_add(1));

            // If the counter progressed enough, try advancing the epoch and collecting garbage.
            if count % PINS_BETWEEN_COLLECT == 0 {
                try_advance(pin);
                garbage::collect(pin);
            }
        }

        // This will unpin the thread even if `f` panics.
        defer! {
            if !was_pinned {
                // Unpin the thread.
                thread.set_unpinned();
                harness.is_pinned.set(false);
            }
        }

        f(pin)
    })
}

/// Stashes away an object that will later be freed.
///
/// This function simply inserts the object into a globally shared [`Garbage`] instance.
///
/// [`Garbage`]: struct.Garbage.html
pub unsafe fn defer_free<T>(object: *mut T, pin: &Pin) {
    unsafe fn free<T>(ptr: *mut T) {
        // Free the memory, but don't run the destructor.
        drop(Vec::from_raw_parts(ptr, 0, 1));
    }

    loop {
        // Get the thread-local bag.
        let cell = &*pin.bag;
        let bag = cell.get();

        // Try inserting the object into the bag.
        if (*bag).try_insert(free::<T>, object) {
            // Success! We're done.
            break;
        }

        // The bag is full. We must replace it with a fresh one.
        cell.set(Box::into_raw(Box::new(Bag::new())));

        // Spare some cycles on garbage collection.
        // Note: This may itself produce garbage and in turn allocate new bags.
        try_advance(pin);
        garbage::collect(pin);

        // Finally, push the old bag into the garbage queue.
        let bag = Box::from_raw(bag);
        garbage::push(bag, pin);
    }
}

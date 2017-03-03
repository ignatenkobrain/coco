//! Thread synchronization and pinning
//!
//! # The global epoch
//!
//! The global `EPOCH` number represents the current epoch. Every so often this number is
//! incremented. In other words, the epoch advances. The epoch can advance only if all currently
//! pinned threads have been pinned in the current epoch.
//!
//! If an object became unreachable in some epoch, then no thread will hold a reference to it after
//! two epoch advancements - that is the moment when it will be safe to free it's memory.
//!
//! # Registration
//!
//! In order to track all threads in one place, we need some form of thread registration. Every
//! thread has a thread-local so called `HARNESS` that registers it the first time it is pinned,
//! and unregisters when it exits.
//!
//! Registered threads are tracked in a global lock-free singly-linked list of thread entries. The
//! head of this list is stored in the `PARTICIPANTS` global and accessed by calling the helper
//! `participants` function.
//!
//! Thread entries are implemented as the `Thread` data type. Every entry contains an integer that
//! tells whether the thread is pinned and if so, what was the global epoch at the time it was
//! pinned.
//!
//! # Stashing garbage
//!
//! If a pinned thread wants to stash away an unlinked object to free it's memory at a later safe
//! time, it will store it in it's thread-local bag. If the local bag is full, it must first be
//! replaced with a fresh one. The old bag is then pushed into the global garbage queue and marked
//! with the current epoch.
//!
//! The global garbage queue lives in the `garbage` module and is the place where local bags go to
//! die. Threads are good citizens, so they sometimes pop a few bags of garbage from the queue in
//! order to free memory and thus help reduce the amount of accumulated garbage.

use super::{Atomic, Ptr, TaggedAtomic, TaggedPtr};
use super::garbage::{self, Bag};

use std::cell::{Cell, UnsafeCell};
use std::mem;
use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{self, AtomicUsize, ATOMIC_USIZE_INIT};

// TODO: if we unlink a huge object, mark a bool in the pin and on unpinning yield until the epoch
// advances enough so that we can empty the whole bag.
// TODO: an unsafe unlinked method that does not require pinning
// TODO: verify that there is no leaking

/// The global epoch number.
///
/// This number is always even because the last bit is reserved for threads to signify that they
/// are not pinned.
///
/// The global epoch is advanced by increasing the number by 2, and wrapping it around on overflow.
/// A pinned thread may advance the epoch only if all pinned threads have been pinned with the
/// current epoch.
static EPOCH: AtomicUsize = ATOMIC_USIZE_INIT;

/// Head pointer to the singly-linked list of participating threads.
///
/// This `AtomicUsize` is actually a `TaggedAtomic<Thread>`. Until we get const functions in Rust,
/// this is an easy zero-cost method of initializing it. This head pointer must be accessed using
/// the `participants` function.
///
/// Each thread is registered on it's first call to `pin` by adding it's own newly allocated entry
/// to the head of this list. Unregistration is triggered by destruction of the thread-local
/// `Harness`, which happens on thread exit.
static PARTICIPANTS: AtomicUsize = ATOMIC_USIZE_INIT;

thread_local! {
    /// The thread registration harness.
    ///
    /// The harness is lazily initialized on it's first use. Initialization performs registration.
    /// If initialized, the harness will get destructed on thread exit, which in turn unregisters
    /// the thread.
    static HARNESS: Harness = Harness {
        thread: Thread::register(),
        is_pinned: Cell::new(false),
        bag: Cell::new(Box::into_raw(Box::new(Bag::new()))),
    };
}

/// Holds thread-local data and unregisters the thread when dropped.
struct Harness {
    /// This thread's entry in the participants list.
    thread: *const Thread,

    /// Whether the thread is currently pinned.
    is_pinned: Cell<bool>,

    /// The local bag of unlinked objects.
    bag: Cell<*mut Bag>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Now that the thread is exiting, we must move the local bag into the global garbage
        // queue. Also, let's try advancing the epoch and help free some garbage.
        let thread = unsafe { &*self.thread };

        // If we called the `pin` function here, it would try to access `HARNESS` and then panic.
        // To work around the problem, we manually pin the thread.
        let pin = &Pin { bag: &self.bag };
        thread.set_pinned();

        // Spare some cycles on garbage collection.
        atomic::fence(AcqRel);
        let epoch = EPOCH.load(Relaxed);
        advance(epoch, pin);
        garbage::collect(epoch, pin);

        // Push the local bag into the garbage queue.
        let bag = unsafe { Box::from_raw(self.bag.get()) };
        garbage::push(bag, epoch, pin);

        thread.set_unpinned();
        thread.unregister();
    }
}

/// An entry in the linked list of participanting threads.
struct Thread {
    /// The global epoch just before the thread got pinned.
    ///
    /// If this number is odd, the thread is not pinned. In other words, the least significant bit
    /// signifies that the thread is unpinned. Epochs are always even numbers so that they fit into
    /// the upper bits.
    epoch: AtomicUsize,

    /// The next thread in the linked list of participants.
    ///
    /// If the tag is 1, that signifies this entry is deleted and can be freely removed from the
    /// list. Every participanting thread sets the tag to 1 when it exits.
    next: TaggedAtomic<Thread>,
}

impl Thread {
    /// Marks the thread as pinned.
    ///
    /// Must not be called if the thread is already pinned!
    fn set_pinned(&self) {
        self.epoch.store(EPOCH.load(Relaxed), Relaxed);

        // Any further loads must not precede the store. In order words, this thread's epoch must
        // be fully announced before we load anything from the memory shared throught `Atomic`s.
        atomic::fence(AcqRel);
    }

    /// Marks the thread as unpinned.
    fn set_unpinned(&self) {
        // Nothing special about number 1, any odd number marks the thread as unpinned.
        self.epoch.store(1, Release);
    }

    /// Registers a thread by adding it's entry to the list of participanting threads.
    ///
    /// Returns a pointer to the newly allocated entry.
    fn register() -> *mut Thread {
        let list = participants();
        let mut new = Box::new(Thread {
            // Nothing special about number 1, any odd number marks the thread as unpinned.
            epoch: AtomicUsize::new(1),
            next: TaggedAtomic::null(0),
        });

        // This code is executing while the thread harness is initializing, so normal pinning would
        // try to access it while it is being initialized. Such accesses fail with a panic.
        unsafe {
            dummy_pin(|pin| {
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
            })
        }
    }

    /// Unregisters the thread by marking it's entry as deleted.
    ///
    /// This function doesn't physically remove the entry from the linked list, though. That will
    /// do any future call to `advance`.
    fn unregister(&self) {
        // This code is executing while the thread harness is destructing, so normal pinning would
        // try to access it while it is being destructed. Such accesses fail with a panic.
        unsafe {
            dummy_pin(|pin| {
                // Simply mark the next-pointer in this thread's entry.
                let mut next = self.next.load(Acquire, pin);
                while next.tag() == 0 {
                    match self.next.cas_weak(next, next.with_tag(1), AcqRel) {
                        Ok(()) => break,
                        Err(n) => next = n,
                    }
                }
            })
        }
    }
}

/// Returns a reference to the head pointer of the list of participating threads.
fn participants() -> &'static TaggedAtomic<Thread> {
    // Simply cast the `&'static AtomicUsize` to a `&'static TaggedAtomic<Thread>`.
    unsafe { &*(&PARTICIPANTS as *const _ as *const _) }
}

/// Attempts to advance the global epoch.
///
/// The global epoch can advance only if all currently pinned threads have been pinned in the
/// current epoch.
fn advance(epoch: usize, pin: &Pin) {
    // Traverse the linked list of participating threads.
    let mut pred = participants();
    let mut curr = pred.load(Acquire, pin);

    while let Some(c) = curr.as_ref() {
        let succ = c.next.load(Acquire, pin);

        if succ.tag() == 1 {
            // This thread has exited. Try unlinking it from the list.
            let succ = succ.with_tag(0);

            if pred.cas(curr, succ, Release).is_err() {
                // We lost the race to unlink the thread. Usually this means we should traverse the
                // list again from the beginning, but since another thread trying to advance the
                // epoch has won the race, we leave the job to that one.
                return;
            }

            // Free the entry allocated by the unlinked thread.
            unsafe { unlinked(c as *const _ as *mut Thread, 1, pin) }

            // Predecessor doesn't change.
            curr = succ;
        } else {
            // If the thread was pinned in a different epoch, we cannot advance the global epoch
            // just yet.
            let e = c.epoch.load(Acquire);
            if e % 2 == 0 && e != epoch {
                return;
            }

            pred = &c.next;
            curr = succ;
        }
    }

    // All pinned threads were pinned in the current global epoch.
    // Finally, try advancing the epoch. We increment by 2 because epochs are even numbers, and
    // simply wrap around on overflow.
    EPOCH.compare_and_swap(epoch, epoch.wrapping_add(2), AcqRel);
}

/// A witness that the current thread is pinned.
///
/// A reference to `Pin` is proof that the current thread is pinned. Interaction with `Atomic` is
/// safe only while the thread is pinned so it's methods often require such references.
///
/// This data type is inherently bound to the thread that created it, therefore it does not
/// implement `Send` nor `Sync`.
///
/// # Examples
///
/// ```
/// use epoch::{self, Pin, Atomic};
/// use std::sync::atomic::Ordering::SeqCst;
///
/// struct Foo(Atomic<String>);
///
/// impl Foo {
///     fn get<'p>(&self, pin: &'p Pin) -> &'p str {
///         self.0.load(SeqCst, pin).unwrap()
///     }
/// }
///
/// let foo = Foo(Atomic::new("hello".to_string()));
///
/// epoch::pin(|pin| assert_eq!(foo.get(pin), "hello"));
/// ```
pub struct Pin {
    /// A pointer to the cell within `HARNESS`, which holds a pointer to the local bag.
    ///
    /// This pointer is kept within `Pin` as a matter of convenience. It could also be reached
    /// through `HARNESS` itself, but that doesn't work if we in the process of it's destruction.
    /// Anyways, this pointer makes some things a lot simpler, and possibly also slightly more
    /// performant.
    bag: *const Cell<*mut Bag>, // !Send + !Sync
}

/// Pins the current thread.
///
/// The provided function takes a reference to a `Pin`, which can be used to interact with
/// `Atomic`s. The pin serves as a proof that whatever data you load from an `Atomic` will not be
/// concurrently deleted by another thread while the pin is alive.
///
/// Note that keeping a thread pinned for a long time prevents memory reclamation of any newly
/// deleted objects protected by `Atomic`s. The provided function should be very quick - generally
/// speaking, it shouldn't take more than 100 ms.
///
/// Pinning itself executes some memory barriers and performs several atomic operations. However,
/// this mechanism is designed to be as performant as possible, so it can be used pretty liberally.
/// On a modern machine a single thread can perform around 200 million pinnings per second.
///
/// Pinning is reentrant. There is no harm in pinning a thread while it's already pinned (repinning
/// is essentially a noop).
///
/// # Examples
///
/// ```
/// use epoch::Atomic;
/// use std::sync::Arc;
/// use std::sync::atomic::Ordering::Relaxed;
/// use std::thread;
///
/// // Create a shared heap-allocated integer.
/// let a = Atomic::new(10);
///
/// epoch::pin(|pin| {
///     // Load the atomic.
///     let old = a.load(Relaxed, pin);
///     assert_eq!(*old.unwrap(), 10);
///
///     // Store a new heap-allocated integer in it's place.
///     a.store_box(Box::new(20), Relaxed, pin);
///
///     // The old value is not reachable anymore.
///     // The piece of memory it owns will be reclaimed at a later time.
///     unsafe { old.unlinked(pin) }
///
///     // Load the atomic again.
///     let new = a.load(Relaxed, pin);
///     assert_eq!(*new.unwrap(), 20);
/// });
///
/// // When `Atomic` gets destructed, it doesn't do anything with the object it references.
/// // We must announce that it got unlinked, otherwise it will leak memory.
/// unsafe { epoch::pin(|pin| a.load(Relaxed, pin).unlinked(pin)) }
/// ```
pub fn pin<F, T>(f: F) -> T
    where F: FnOnce(&Pin) -> T
{
    HARNESS.with(|harness| {
        let thread = unsafe { &*harness.thread };

        let was_pinned = harness.is_pinned.get();
        if !was_pinned {
            harness.is_pinned.set(true);
            thread.set_pinned();
        }

        // This will unpin the thread even in an unfortunate event of `f` panicking.
        defer! {
            if !was_pinned {
                thread.set_unpinned();
                harness.is_pinned.set(false);
            }
        }

        f(&Pin { bag: &harness.bag })
    })
}

/// Pretends that it pins the current thread.
///
/// This function is used in very exceptional situations only. In particular, when registering and
/// unregistering threads, we can't really pin a thread because accessing thread-locals would cause
/// a panic. But we still need to perform some atomic operations that require a `&Pin`, so this
/// function comes in as a handy cheat.
pub unsafe fn dummy_pin<F, T>(f: F) -> T
    where F: FnOnce(&Pin) -> T
{
    // We don't even bother with bags.
    // This is really unsafe. Use with caution.
    f(&Pin { bag: ptr::null() })
}

/// TODO: documentation
pub unsafe fn unlinked<T>(value: *mut T, count: usize, pin: &Pin) {
    let size = ::std::mem::size_of::<T>();

    let cell = &*pin.bag;
    let bag = cell.get();

    if !(*bag).try_insert(value, count) {
        // Replace the bag with a fresh one.
        let mut new = Box::new(Bag::new());
        new.try_insert(value, count);
        cell.set(Box::into_raw(new));

        // Spare some cycles on garbage collection.
        atomic::fence(AcqRel);
        let epoch = EPOCH.load(Relaxed);
        advance(epoch, pin);
        garbage::collect(epoch, pin);

        // Finally, push the old bag into the garbage queue.
        garbage::push(Box::from_raw(bag), epoch, pin);
    }
}

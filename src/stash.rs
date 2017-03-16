//! TODO
//!

use std::cell::Cell;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{self, AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

use super::epoch;
use super::{Atomic, Pin, Ptr};

/// TODO
const MAX_OBJECTS: usize = 64;

struct Node {
    /// Number of objects in the node.
    len: AtomicUsize,
    /// Unlinked objects.
    objects: [Cell<(unsafe fn(*mut u8), *mut u8)>; MAX_OBJECTS],
    /// The global epoch at the moment when this node got pushed into the queue.
    epoch: AtomicUsize,
    /// The next node in the queue.
    next: Atomic<Node>,
}

impl Node {
    /// TODO
    fn new() -> Self {
        Node {
            len: AtomicUsize::new(0),
            objects: unsafe { mem::uninitialized() },
            epoch: AtomicUsize::new(0),
            next: Atomic::null(),
        }
    }

    /// TODO
    fn try_put(&self, dtor: unsafe fn(*mut u8), object: *mut u8) -> bool {
        let mut len = self.len.load(Relaxed);
        loop {
            if len == self.objects.len() {
                return false;
            }

            match self.len.compare_exchange_weak(len, len + 1, Relaxed, Relaxed) {
                Ok(_) => {
                    self.objects[len].set((dtor, object));
                    atomic::fence(Release); // Includes StoreStore barrier.
                    return true;
                }
                Err(l) => len = l,
            }
        }
    }
}

/// TODO: docs
#[repr(C)]
pub struct Stash {
    /// Head of the queue (always a sentinel entry).
    head: Atomic<Node>,
    // /// Padding to avoid false sharing.
    // _pad1: [u8; 64],
    /// Tail of the queue.
    tail: Atomic<Node>,
    // /// Padding to avoid false sharing.
    // _pad2: [u8; 64],
    /// TODO
    pending: Atomic<Node>,
}

impl Stash {
    pub fn new() -> Self {
        let stash = Stash {
            head: Atomic::null(),
            // _pad1: unsafe { mem::uninitialized() },
            tail: Atomic::null(),
            // _pad2: unsafe { mem::uninitialized() },
            pending: Atomic::null(),
        };

        let guard = super::pin();
        let sentinel = stash.head.store_box(Box::new(Node::new()), Relaxed, &guard);
        stash.tail.store(sentinel, Relaxed);

        stash
    }

    /// TODO: a note on lifetime of T
    // TODO: a note on send + sync
    pub unsafe fn defer_drop<T>(&self, object: *mut T, guard: &Pin)
    {
        unsafe fn dtor<T>(ptr: *mut u8) {
            // Execute destructor and free the memory.
            drop(Vec::from_raw_parts(ptr as *mut T, 1, 1));
        }
        self.defer_destroy(dtor::<T>, object as *mut u8, guard);
    }

    /// TODO
    // TODO: a note on send + sync
    pub unsafe fn defer_destroy<T>(
        &self,
        dtor: unsafe fn(*mut T),
        object: *mut T,
        guard: &Pin
    ) {
        let dtor: unsafe fn(*mut u8) = unsafe { mem::transmute(dtor) };
        let object = object as *mut u8;

        loop {
            let pending = self.pending.load(Acquire, guard);

            match pending.as_ref() {
                None => {
                    let mut node = Node::new();
                    assert!(node.try_put(dtor, object));

                    if self.pending.cas_box(pending, Box::new(node), Release).is_ok() {
                        break;
                    }
                }
                Some(p) => {
                    if p.try_put(dtor, object) {
                        break;
                    } else {
                        if self.pending.cas_box(pending, Box::new(Node::new()), Release).is_ok() {
                            let node = unsafe { Box::from_raw(pending.as_raw()) };

                            let epoch = epoch::load().0;
                            node.epoch.store(epoch, Relaxed);

                            self.push_node(node, guard);
                            self.collect(guard);
                        }
                    }
                }
            }
        }
    }

    /// TODO: how often should we call this? (in theory, never)
    pub fn collect(&self, guard: &Pin) {
        let epoch = epoch::load().0;

        let condition = |node: &Node| {
            // A pinned thread can witness at most two epoch advancements. Therefore, any node that
            // is within two epochs of the current one cannot be freed yet.
            epoch::distance(epoch, node.epoch.load(Relaxed)) > 2
        };

        if let Some(node) = self.try_pop_if(condition, &guard) {
            for cell in node.objects.iter() {
                let (dtor, object) = cell.get();
                unsafe { dtor(object) }
            }
        }
    }

    /// Pushes a node into the queue.
    ///
    /// The bag must be marked with an epoch beforehand. TODO
    fn push_node(&self, mut node: Box<Node>, guard: &Pin) {
        let mut tail = self.tail.load(Acquire, guard);
        loop {
            let next = tail.unwrap().next.load(Acquire, guard);

            if next.is_null() {
                // Try installing the new node.
                match tail.unwrap().next.cas_box_weak(next, node, AcqRel) {
                    Ok(node) => {
                        // Tail pointer shouldn't fall behind. Let's move it forward.
                        self.tail.cas(tail, node, Release);
                        break;
                    }
                    Err((t, n)) => {
                        tail = t;
                        node = n;
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

    /// Attempts to pop a node from the front of the queue and returns it if `condition` is met.
    ///
    /// If the node in the front doesn't meet it or if the queue is empty, `None` is returned.
    fn try_pop_if<'p, F>(&self, condition: F, guard: &'p Pin) -> Option<&'p Node>
        where F: Fn(&Node) -> bool
    {
        let mut head = self.head.load(Acquire, guard);
        loop {
            let next = head.unwrap().next.load(Acquire, guard);

            match next.as_ref() {
                Some(n) if condition(n) => {
                    // Try unlinking the head by moving it forward.
                    match self.head.cas_weak(head, next, AcqRel) {
                        Ok(()) => {
                            unsafe { head.defer_free(guard) }
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

impl Drop for Stash {
    fn drop(&mut self) {
        // TODO
    }
}

use std::fmt;
use std::mem;
use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release, Relaxed};

use epoch::{self, Atomic};

/// A single node in a queue.
struct Node<T> {
    /// The payload.
    data: T,
    /// The next node in the queue.
    next: Atomic<Node<T>>,
}

/// The inner representation of a queue.
///
/// It consists of a head and tail pointer, with some padding in-between to avoid false sharing.
#[repr(C)]
struct Inner<T> {
    head: Atomic<Node<T>>,
    _pad: [u8; 64 - 4],
    tail: Atomic<Node<T>>,
}

/// A concurrent queue.
///
/// It can be used with multiple producers and multiple consumers at the same time.
pub struct Queue<T>(Inner<T>);

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    /// Returns a new, empty queue.
    pub fn new() -> Queue<T> {
        let inner = Inner {
            head: Atomic::from_box(Box::new(Node {
                data: unsafe { mem::uninitialized() },
                next: Atomic::null(),
            })),
            _pad: unsafe { mem::uninitialized() },
            tail: Atomic::null(),
        };

        // Copy the head pointer into the tail pointer.
        epoch::pin(|pin| inner.tail.store(inner.head.load(Relaxed, pin), Relaxed));

        Queue(inner)
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        let inner = &self.0;
        epoch::pin(|pin| inner.head.load(Acquire, pin).unwrap().next.load(Relaxed, pin).is_null())
    }

    /// Pushes a new element into the queue.
    pub fn push(&self, data: T) {
        let inner = &self.0;

        let mut new = Box::new(Node {
            data: data,
            next: Atomic::null(),
        });

        epoch::pin(|pin| {
            let mut tail = inner.tail.load(Acquire, pin);
            loop {
                let next = tail.unwrap().next.load(Acquire, pin);
                if next.is_null() {
                    // Try installing the new item.
                    match tail.unwrap().next.cas_box_weak(next, new, AcqRel) {
                        Ok(new) => {
                            // Tail pointer shouldn't fall behind. Let's move it forward.
                            let _ = inner.tail.cas(tail, new, Release);
                            break;
                        }
                        Err((t, n)) => {
                            tail = t;
                            new = n;
                        }
                    }
                } else {
                    // This is not the actual tail. Move the tail pointer forward.
                    match inner.tail.cas_weak(tail, next, AcqRel) {
                        Ok(()) => tail = next,
                        Err(t) => tail = t,
                    }
                }
            }
        })
    }

    /// Attempts to pop an element from the queue.
    pub fn try_pop(&self) -> Option<T> {
        let inner = &self.0;
        epoch::pin(|pin| {
            let mut head = inner.head.load(Acquire, pin);
            loop {
                let next = head.unwrap().next.load(Acquire, pin);
                match next.as_ref() {
                    None => return None,
                    Some(n) => {
                        // Try unlinking the head by moving it forward.
                        match inner.head.cas_weak(head, next, AcqRel) {
                            Ok(_) => unsafe {
                                // The old head may be later freed.
                                epoch::defer_free(head.as_raw(), pin);
                                // The new head holds the popped value (heads are sentinels!).
                                return Some(ptr::read(&n.data));
                            },
                            Err(h) => head = h,
                        }
                    }
                }
            }
        })
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let inner = &self.0;

        // Destruct all nodes in the queue.
        let mut head = inner.head.load_raw(Relaxed);
        loop {
            // Load the next node and destruct the current one.
            let next = unsafe { (*head).next.load_raw(Relaxed) };
            unsafe { drop(Box::from_raw(head)) }

            // If the next node is null, we've reached the end of the queue.
            if next.is_null() {
                break;
            }

            // Move one step forward.
            head = next;
        }
    }
}

impl<T> fmt::Debug for Queue<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Queue {{ ... }}")
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;

    use std::thread;
    use std::sync::Arc;

    use self::rand::Rng;

    use super::Queue;

    #[test]
    fn simple() {
        let s = Arc::new(Queue::<i32>::new());

        let mut handles = vec![];

        for _ in 0..4 {
            let s = s.clone();
            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..1_000_000 {
                    if rng.gen::<usize>() % 2 == 0 {
                        let x = rng.gen::<i32>();
                        s.push(x);
                    } else {
                        s.try_pop();
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

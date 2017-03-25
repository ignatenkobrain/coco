use std::fmt;
use std::mem;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release, Relaxed};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use either::Either;

use epoch::{self, Atomic, Pin, Ptr};

enum Payload<T> {
    Data(T),
    Request(*mut Request<T>),
}

struct Request<T> {
    thread: Thread,
    ready: AtomicBool,
    data: Option<T>,
}

/// A single node in a queue.
struct Node<T> {
    /// The payload.
    payload: Payload<T>,
    /// The next node in the queue.
    next: Atomic<Node<T>>,
}

/// The inner representation of a queue.
///
/// It consists of a head and tail pointer, with some padding in-between to avoid false sharing.
#[repr(C)]
struct Inner<T> {
    head: Atomic<Node<T>>,
    _pad: [u8; 64],
    tail: Atomic<Node<T>>,
}

/// A lock-free multi-producer multi-consumer queue.
pub struct Queue<T>(Inner<T>);

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    /// Returns a new, empty queue.
    pub fn new() -> Queue<T> {
        let inner = Inner {
            head: Atomic::from_box(Box::new(Node {
                payload: unsafe { mem::uninitialized() },
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

        epoch::pin(|pin| {
            let head = inner.head.load(Acquire, pin);
            let next = head.unwrap().next.load(Acquire, pin);

            match next.as_ref() {
                None => true,
                Some(n) => {
                    match n.payload {
                        Payload::Data(_) => false,
                        Payload::Request(_) => true,
                    }
                }
            }
        })
    }

    fn push_weak<'p>(&self, tail: Ptr<'p, Node<T>>, new: Box<Node<T>>, pin: &'p Pin)
                     -> Result<(), (Ptr<'p, Node<T>>, Box<Node<T>>)>
    {
        let inner = &self.0;
        let next = tail.unwrap().next.load(Acquire, pin);

        if next.is_null() {
            match tail.unwrap().next.cas_box_weak(next, new, AcqRel) {
                Ok(new) => {
                    // Tail pointer shouldn't fall behind. Let's move it forward.
                    let _ = inner.tail.cas(tail, new, Release);
                    Ok(())
                }
                Err((t, n)) => Err((t, n)),
            }
        } else {
            // This is not the actual tail. Move the tail pointer forward.
            match inner.tail.cas_weak(tail, next, AcqRel) {
                Ok(()) => Err((next, new)),
                Err(t) => Err((t, new)),
            }
        }
    }

    /// Pushes a new element into the queue.
    pub fn push(&self, data: T) {
        let inner = &self.0;
        let mut data = Either::Left(data);

        epoch::pin(|pin| {
            let mut tail = inner.tail.load(Acquire, pin);
            loop {
                // Does the queue contain a request node?
                let has_request = match tail.unwrap().payload {
                    Payload::Data(_) => false,
                    Payload::Request(_) => inner.head.load(Relaxed, pin).as_raw() != tail.as_raw(),
                };

                if has_request {
                    // The request node should be the one following the head.
                    let head = inner.head.load(Acquire, pin);
                    let next = head.unwrap().next.load(Acquire, pin);

                    match next.unwrap().payload {
                        Payload::Data(_) => {}
                        Payload::Request(req) => {
                            // Try moving the head forward.
                            if inner.head.cas_weak(head, next, Release).is_ok() {
                                unsafe {
                                    // The old head may be later freed.
                                    epoch::defer_free(head.as_raw(), pin);

                                    let data = data.either(|l| l, |r: Box<Node<T>>| {
                                        match r.payload {
                                            Payload::Data(d) => d,
                                            Payload::Request(_) => unreachable!(),
                                        }
                                    });

                                    // Fulfill the request.
                                    (*req).data = Some(data);
                                    (*req).ready.store(true, Release);
                                    (*req).thread.unpark();
                                    break;
                                }
                            }
                        }
                    }

                    // We failed to fulfill a request. Load the current tail and try again.
                    tail = inner.tail.load(Acquire, pin);
                } else {
                    // There is no request node. Try pushing a data node.
                    let new = match data {
                        Either::Left(d) => Box::new(Node {
                            payload: Payload::Data(d),
                            next: Atomic::null(),
                        }),
                        Either::Right(new) => new,
                    };

                    match self.push_weak(tail, new, pin) {
                        Ok(()) => break,
                        Err((t, n)) => {
                            tail = t;
                            data = Either::Right(n);
                        }
                    }
                }
            }
        })
    }

    fn pop_weak<'p>(&self, head: Ptr<'p, Node<T>>, pin: &'p Pin)
                    -> Result<Option<T>, Ptr<'p, Node<T>>>
    {
        let inner = &self.0;
        let next = head.unwrap().next.load(Acquire, pin);

        match next.as_ref() {
            None => return Ok(None),
            Some(n) => {
                match n.payload {
                    Payload::Data(ref data) => {
                        // Try unlinking the head by moving it forward.
                        match inner.head.cas_weak(head, next, AcqRel) {
                            Ok(_) => unsafe {
                                // The old head may be later freed.
                                epoch::defer_free(head.as_raw(), pin);
                                // The new head holds the popped value.
                                return Ok(Some(ptr::read(data)));
                            },
                            Err(h) => return Err(h),
                        }
                    }
                    Payload::Request(_) => return Err(inner.head.load(Acquire, pin)),
                }
            }
        }
    }

    /// Attempts to pop an element from the queue.
    ///
    /// Returns `None` if the queue is empty.
    pub fn try_pop(&self) -> Option<T> {
        let inner = &self.0;

        epoch::pin(|pin| {
            let mut head = inner.head.load(Acquire, pin);
            loop {
                match self.pop_weak(head, pin) {
                    Ok(r) => return r,
                    Err(h) => head = h,
                }
            }
        })
    }

    fn pop_until(&self, deadline: Option<Instant>) -> Option<T> {
        let inner = &self.0;

        let mut req = Request {
            thread: thread::current(),
            ready: AtomicBool::new(false),
            data: None,
        };
        let req = &mut req;

        epoch::pin(|pin| {
            let mut new = Box::new(Node {
                payload: Payload::Request(req),
                next: Atomic::null(),
            });

            let mut tail = inner.tail.load(Acquire, pin);
            loop {
                let head = inner.head.load(Acquire, pin);
                if let Ok(Some(r)) = self.pop_weak(head, pin) {
                    return Some(r);
                }

                match tail.unwrap().payload {
                    Payload::Data(_) if head.as_raw() != tail.as_raw() => {
                        tail = inner.tail.load(Acquire, pin);
                    },
                    _ => {
                        match self.push_weak(tail, new, pin) {
                            Ok(()) => return None,
                            Err((t, n)) => {
                                tail = t;
                                new = n;
                            }
                        }
                    }
                }
            }
        }).or_else(|| {
            while !req.ready.load(Acquire) {
                match deadline {
                    None => thread::park(),
                    Some(deadline) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return None;
                        }
                        thread::park_timeout(deadline - now);
                    }
                }
            }
            Some(req.data.take().unwrap())
        })
    }

    pub fn pop(&self) -> T {
        self.try_pop().or_else(|| self.pop_until(None)).unwrap()
    }

    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        let inner = &self.0;

        epoch::pin(|pin| {
            let head = inner.head.load(Acquire, pin);
            self.pop_weak(head, pin).unwrap_or(None)
        }).or_else(|| {
            let deadline = Instant::now() + timeout;
            self.pop_until(Some(deadline))
        })
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let inner = &self.0;

        // Destruct all nodes in the queue.
        let mut head = inner.head.load_raw(Relaxed);
        loop {
            // Load the next node and destroy the current one.
            let next = unsafe { (*head).next.load_raw(Relaxed) };
            unsafe { drop(Box::from_raw_parts(head, 0, 1)) }

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
    fn push_pop() {
        let s = Queue::new();
        s.push(10);
        s.push(20);
        assert_eq!(s.try_pop(), Some(10));
        assert_eq!(s.try_pop(), Some(20));
        assert_eq!(s.try_pop(), None);
    }

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

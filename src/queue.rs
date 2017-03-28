use std::fmt;
use std::mem;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release, Relaxed};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use either::Either;

use epoch::{self, TaggedAtomic, Pin, TaggedPtr};

/// Payload every node is carrying.
enum Payload<T> {
    /// Pushed value.
    Value(T),
    /// Request for a value - a waiting pop operation.
    Request(*mut Request<T>),
}

impl<T> Payload<T> {
    /// Returns `true` if the payload is a value.
    fn is_value(&self) -> bool {
        match *self {
            Payload::Value(_) => true,
            Payload::Request(_) => false,
        }
    }

    /// Returns `true` if the payload is a request.
    fn is_request(&self) -> bool {
        match *self {
            Payload::Value(_) => false,
            Payload::Request(_) => true,
        }
    }
}

/// Request for a value by a blocking pop operation.
struct Request<T> {
    /// The thread that requests a value.
    thread: Thread,
    /// A push operation sets this to `true` after it passes a value over.
    ready: AtomicBool,
    /// The slot through which a value will be passed over.
    value: Option<T>,
}

/// A single node in a queue.
struct Node<T> {
    /// The payload.
    payload: Payload<T>,
    /// The next node in the queue.
    next: TaggedAtomic<Node<T>>,
}

/// The inner representation of a queue.
///
/// It consists of a head and tail pointer, with some padding in-between to avoid false sharing.
/// A queue is a singly linked list of value nodes. There is always one sentinel value node, and
/// that is the head. If both head and tail point to the sentinel node, the queue is empty.
///
/// If the queue is empty, there might be a list of request nodes following the tail, which
/// represent threads blocked on future push operations. The tail never moves onto a request node.
///
/// To summarize, the structure of the queue is always one of these two:
///
/// 1. Sentinel node (head), followed by a number of value nodes (the last one is tail).
/// 2. Sentinel node (head and tail), followed by a number of request nodes.
///
/// Requests are fulfilled by marking the next-pointer of it's node, then copying a value into the
/// slot, and finally signalling that the blocked thread is ready to be woken up. Nodes with marked
/// next-pointers are considered to be deleted and can always be unlinked from the list. A request
/// can cancel itself simply by marking the next-pointer of it's node.
#[repr(C)]
struct Inner<T> {
    /// Head of the queue.
    head: TaggedAtomic<Node<T>>,
    /// Some padding to avoid false sharing.
    _pad: [u8; 64],
    /// Tail ofthe queue.
    tail: TaggedAtomic<Node<T>>,
}

/// A lock-free multi-producer multi-consumer queue.
pub struct Queue<T>(Inner<T>);

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    /// Returns a new, empty queue.
    pub fn new() -> Queue<T> {
        // Create a sentinel node.
        let node = Box::new(Node {
            payload: Payload::Value(unsafe { mem::uninitialized() }),
            next: TaggedAtomic::null(0),
        });

        // Initialize the internal representation of the queue.
        let inner = Inner {
            head: TaggedAtomic::from_box(node, 0),
            _pad: unsafe { mem::uninitialized() },
            tail: TaggedAtomic::null(0),
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

            // Check whether there is a value node following the head.
            match next.as_ref() {
                None => return true,
                Some(n) => return n.payload.is_request(),
            }
        })
    }

    /// Pushes a new value into the queue.
    pub fn push(&self, value: T) {
        let inner = &self.0;
        let mut value = Either::Left(value);

        epoch::pin(|pin| {
            let mut steps = 0usize;
            let mut tail = inner.tail.load(Acquire, pin);

            loop {
                steps = steps.wrapping_add(1);

                // Load the node following the tail.
                let t = tail.unwrap();
                let next = t.next.load(Acquire, pin);

                match next.as_ref() {
                    None => {
                        // There is no request node. Do a normal push.
                        let new = match value {
                            Either::Left(v) => Box::new(Node {
                                payload: Payload::Value(v),
                                next: TaggedAtomic::null(0),
                            }),
                            Either::Right(new) => new,
                        };

                        // Try installing the new node.
                        match t.next.cas_box_weak(next, new, 0, AcqRel) {
                            Ok(new) => {
                                // Successfully pushed the node!
                                // Tail pointer mustn't fall behind. Move it forward.
                                let _ = inner.tail.cas(tail, new, Release);
                                break;
                            }
                            Err((next, v)) => {
                                // Failed. The node that acutally follows `t` is `next`.
                                match next.as_ref() {
                                    // If this is a value node and we didn't already retry too many
                                    // times, it is probably the current tail.
                                    Some(n) if n.payload.is_value() && steps < 5 => tail = next,
                                    // Otherwise, load a fresh tail.
                                    _ => tail = inner.tail.load(Acquire, pin),
                                }
                                value = Either::Right(v);
                            }
                        }
                    }
                    Some(n) => {
                        match n.payload {
                            Payload::Value(_) => {
                                // Tail pointer fell behind. Move it forward.
                                match inner.tail.cas_weak(tail, next, AcqRel) {
                                    Ok(()) => tail = next,
                                    Err(t) => tail = t,
                                }
                            }
                            Payload::Request(req) => {
                                // Try fulfilling this request.
                                let succ = n.next.load(Acquire, pin);

                                if succ.tag() == 0 {
                                    // Try marking the node as deleted.
                                    if n.next.cas_weak(succ, succ.with_tag(1), Release).is_ok() {
                                        // Prepare the value.
                                        let value = value.either(|l| l, |r: Box<Node<T>>| {
                                            match r.payload {
                                                Payload::Value(v) => v,
                                                Payload::Request(_) => unreachable!(),
                                            }
                                        });

                                        unsafe {
                                            let thread = (*req).thread.clone();

                                            // Pass `value` over and wake up the waiting thread.
                                            (*req).value = Some(value);
                                            (*req).ready.store(true, Release);

                                            // Because we stored `true`, the thread is ready to
                                            // pick up the value. Before we unpark it, the thread
                                            // might even wake up by itself, pick up the value, and
                                            // destruct `req` from it's own stack. It's very
                                            // important that we don't touch `req` from now on.
                                            thread.unpark();

                                            // Finally, try unlinking the node.
                                            if t.next.cas_weak(next, succ, Release).is_ok() {
                                                epoch::defer_free(next.as_raw(), pin);
                                            }
                                            break;
                                        }
                                    }
                                } else {
                                    // This request node is deleted. Try unlinking it.
                                    let succ = succ.with_tag(0);
                                    if t.next.cas_weak(next, succ, Release).is_ok() {
                                        unsafe { epoch::defer_free(next.as_raw(), pin) }
                                    }
                                }

                                // We didn't make any progress.
                                // Reload the tail pointer and try again.
                                tail = inner.tail.load(Acquire, pin);
                            }
                        }
                    }
                }
            }
        })
    }

    /// Attempts to pop a value from the queue.
    ///
    /// Returns `None` if the queue is empty.
    pub fn try_pop(&self) -> Option<T> {
        let inner = &self.0;

        epoch::pin(|pin| {
            let mut head = inner.head.load(Acquire, pin);
            loop {
                let next = head.unwrap().next.load(Acquire, pin);
                match next.as_ref() {
                    None => return None,
                    Some(n) => {
                        match n.payload {
                            Payload::Value(ref value) => {
                                // Try unlinking the head by moving it forward.
                                match inner.head.cas_weak(head, next, AcqRel) {
                                    Ok(_) => unsafe {
                                        // The old head may be later freed.
                                        epoch::defer_free(head.as_raw(), pin);
                                        // The new head holds the popped value.
                                        return Some(ptr::read(value));
                                    },
                                    Err(h) => head = h,
                                }
                            }
                            Payload::Request(_) => return None,
                        }
                    }
                }
            }
        })
    }

    /// Attempts to cancel a request by finding and deleting it's node.
    ///
    /// Returns `true` if this method deleted the node, and `false` if it was already deleted.
    fn cancel_request(&self, req: *mut Request<T>, pin: &Pin) -> bool {
        let inner = &self.0;

        'retry: loop {
            let head = inner.head.load(Acquire, pin);
            let mut pred = &head.unwrap().next;
            let mut curr = pred.load(Acquire, pin);

            // If there are no request nodes, there is nothing to cancel.
            if let Some(c) = curr.as_ref() {
                if c.payload.is_value() {
                    return false;
                }
            }

            // Find the request node that contains `req`.
            while let Some(c) = curr.as_ref() {
                let succ = c.next.load(Acquire, pin);

                if succ.tag() == 1 {
                    // This request node is deleted. Try unlinking it.
                    let succ = succ.with_tag(0);
                    match pred.cas_weak(curr, succ, Release) {
                        Ok(_) => unsafe { epoch::defer_free(curr.as_raw(), pin) },
                        Err(_) => continue 'retry,
                    }

                    // Update the current node.
                    curr = succ;
                } else {
                    // If this is the request that needs to be cancelled...
                    if let Payload::Request(r) = c.payload {
                        if r == req {
                            // Try marking the node as deleted.
                            match c.next.cas_weak(succ, succ.with_tag(1), Release) {
                                Ok(_) => return true,
                                Err(_) => continue 'retry,
                            }
                        }
                    }

                    // Move one node forward.
                    pred = &c.next;
                    curr = succ;
                }
            }

            // Reached the end of the list.
            return false;
        }
    }

    /// Attempts to pop a value until the specified deadline.
    ///
    /// This method blocks the current thread until a value is available, or the deadline is
    /// exceeded.
    fn pop_until(&self, deadline: Option<Instant>) -> Option<T> {
        let inner = &self.0;

        // Try immediately popping a value.
        if let Some(r) = self.try_pop() {
            return Some(r);
        }

        // Allocate a request on the stack.
        let mut req = Request {
            thread: thread::current(),
            ready: AtomicBool::new(false),
            value: None,
        };
        let req = &mut req;

        // Since the queue is empty, attempt to install a new request node.
        epoch::pin(|pin| {
            let mut new = Box::new(Node {
                payload: Payload::Request(req),
                next: TaggedAtomic::null(0),
            });

            'retry: loop {
                let head = inner.head.load(Acquire, pin);
                let mut pred = &head.unwrap().next;
                let mut curr = pred.load(Acquire, pin);

                // If there is a value node, try popping a value.
                if let Some(c) = curr.as_ref() {
                    match c.payload {
                        Payload::Value(ref value) => {
                            // Try unlinking the head by moving it forward.
                            match inner.head.cas_weak(head, curr, AcqRel) {
                                Ok(_) => unsafe {
                                    // The old head may be later freed.
                                    epoch::defer_free(head.as_raw(), pin);
                                    // The new head holds the popped value.
                                    return Some(ptr::read(value));
                                },
                                Err(_) => continue 'retry,
                            }
                        }
                        Payload::Request(_) => {}
                    }
                }

                // Find the end of the list.
                while let Some(c) = curr.as_ref() {
                    let succ = c.next.load(Acquire, pin);

                    if succ.tag() == 1 {
                        // This request node is deleted. Try unlinking it.
                        let succ = succ.with_tag(0);
                        match pred.cas_weak(curr, succ, Release) {
                            Ok(_) => unsafe { epoch::defer_free(curr.as_raw(), pin) },
                            Err(_) => continue 'retry,
                        }

                        // Update the current node.
                        curr = succ;
                    } else {
                        // Move one node forward.
                        pred = &c.next;
                        curr = succ;
                    }
                }

                // Try installing the new request node.
                match pred.cas_box(TaggedPtr::null(0), new, 0, Release) {
                    Ok(_) => return None,
                    Err((_, n)) => new = n,
                }
            }
        }).or_else(|| {
            // Wait until the request is fulfilled or the deadline is exceeded.
            while !req.ready.load(Acquire) {
                match deadline {
                    None => thread::park(),
                    Some(deadline) => {
                        // Have we reached the deadline?
                        let now = Instant::now();

                        if now >= deadline {
                            // Yeah. Try cancelling the request.
                            if epoch::pin(|pin| self.cancel_request(req, pin)) {
                                // Successfully cancelled.
                                return None;
                            } else {
                                // A thread is about to fulfill the request in a moment - it just
                                // has to copy it's value into the slot.
                                thread::park();
                            }
                        } else {
                            // Wait until the deadline.
                            thread::park_timeout(deadline - now);
                        }
                    }
                }
            }

            // The request has been fulfilled.
            // Return the popped value.
            Some(req.value.take().unwrap())
        })
    }

    /// Pops a value from the queue, potentially blocking the current thread.
    pub fn pop(&self) -> T {
        self.pop_until(None).unwrap()
    }

    /// Attempts to pop a value from the queue, potentially blocking the current thread.
    ///
    /// If the thread waits for more than `timeout`, `None` is returned.
    pub fn pop_timeout(&self, timeout: Duration) -> Option<T> {
        self.pop_until(Some(Instant::now() + timeout))
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let inner = &self.0;
        let head = inner.head.load_raw(Relaxed).0;

        // Destruct all nodes in the queue.
        let mut curr = head;
        loop {
            // Load the next node and destroy the current one.
            let next = unsafe { (*curr).next.load_raw(Relaxed).0 };

            unsafe {
                if curr == head || (*curr).payload.is_request() {
                    // The sentinel node and request nodes must be freed.
                    drop(Vec::from_raw_parts(curr, 0, 1));
                } else {
                    // Other nodes are destructed.
                    drop(Box::from_raw(curr));
                }
            }

            // If the next node is null, we've reached the end of the queue.
            if next.is_null() {
                break;
            }

            // Move one step forward.
            curr = next;
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

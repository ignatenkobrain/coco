use std::ptr;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

use epoch::{self, Atomic};

/// A single node in a stack.
struct Node<T> {
    /// The payload.
    data: T,
    /// The next node in the stack.
    next: Atomic<Node<T>>,
}

/// A concurrent stack.
///
/// It can be used with multiple producers and multiple consumers at the same time.
pub struct Stack<T> {
    head: Atomic<Node<T>>,
}

unsafe impl<T: Send> Send for Stack<T> {}
unsafe impl<T: Send> Sync for Stack<T> {}

impl<T> Stack<T> {
    /// Returns a new, empty stack.
    pub fn new() -> Self {
        Stack { head: Atomic::null() }
    }

    /// Returns `true` if the stack is empty.
    pub fn is_empty(&self) -> bool {
        epoch::pin(|pin| self.head.load(Relaxed, pin).is_null())
    }

    /// Pushes a new element onto the stack.
    pub fn push(&self, data: T) {
        let mut node = Box::new(Node {
            data: data,
            next: Atomic::null(),
        });

        epoch::pin(|pin| {
            let mut head = self.head.load(Acquire, pin);
            loop {
                node.next.store(head, Relaxed);
                match self.head.cas_box_weak(head, node, AcqRel) {
                    Ok(_) => break,
                    Err((h, n)) => {
                        head = h;
                        node = n;
                    }
                }
            }
        })
    }

    /// Attemps to pop an element from the stack.
    ///
    /// Returns `None` if the stack is empty.
    pub fn try_pop(&self) -> Option<T> {
        epoch::pin(|pin| {
            let mut head = self.head.load(Acquire, pin);
            loop {
                match head.as_ref() {
                    Some(h) => {
                        let next = h.next.load(Acquire, pin);
                        match self.head.cas_weak(head, next, AcqRel) {
                            Ok(_) => unsafe {
                                epoch::defer_free(head.as_raw(), pin);
                                return Some(ptr::read(&h.data));
                            },
                            Err(h) => head = h,
                        }
                    }
                    None => return None,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    // TODO
}

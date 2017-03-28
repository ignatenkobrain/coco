use std::mem;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, fence};
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed, SeqCst};

use epoch;
use epoch::Atomic;

// TODO: Do we need something like `Stealer::steal_half`?
// https://github.com/crossbeam-rs/crossbeam/pull/72/files

// TODO: A `Stealer::steal_weak` method that may fail?

// 1. Dynamic Circular Work-Stealing Deque
// 2. Correct and Efficient Work-Stealing for Weak Memory Models

// CDSChecker: Checking Concurrent Data Structures Written with C/C++ Atomics
// http://plrg.eecs.uci.edu/publications/c11modelcheck.pdf
// https://github.com/computersforpeace/model-checker-benchmarks/tree/master/chase-lev-deque-bugfix

const MIN_LENGTH: usize = 16;

struct Array<T> {
    ptr: *mut T,
    len: usize,
}

impl<T> Array<T> {
    fn new(len: usize) -> Self {
        let mut v = Vec::with_capacity(len);
        let ptr = v.as_mut_ptr();
        mem::forget(v);
        Array {
            ptr: ptr,
            len: len,
        }
    }

    unsafe fn at(&self, index: isize) -> *mut T {
        self.ptr.offset(index & (self.len - 1) as isize)
    }

    unsafe fn write(&self, index: isize, value: T) {
        ptr::write(self.at(index), value)
    }

    unsafe fn read(&self, index: isize) -> T {
        ptr::read(self.at(index))
    }

    unsafe fn resize(&self, b: isize, t: isize, new_len: usize) -> Array<T> {
        let new = Array::new(new_len);
        let mut i = t;
        while i != b {
            ptr::copy_nonoverlapping(self.at(i), new.at(i), 1);
            i = i.wrapping_add(1);
        }
        new
    }
}

struct Deque<T> {
    bottom: AtomicIsize,
    top: AtomicIsize,
    array: Atomic<Array<T>>,
}

impl<T> Deque<T> {
    fn new() -> Self {
        Deque {
            bottom: AtomicIsize::new(0),
            top: AtomicIsize::new(0),
            array: Atomic::new(Array::new(MIN_LENGTH)),
        }
    }

    unsafe fn replace(&self, old: *mut Array<T>, new: Array<T>) -> *mut Array<T> {
        let new = Box::into_raw(Box::new(new));
        self.array.store_raw(new, Release);
        epoch::pin(|pin| epoch::defer_free(old, pin));
        new
    }

    fn push(&self, value: T) {
        unsafe {
            let mut b = self.bottom.load(Relaxed);
            let t = self.top.load(Acquire);
            let mut a = self.array.load_raw(Relaxed);

            let size = b.wrapping_sub(t);
            if size == (*a).len as isize {
                a = self.replace(a, (*a).resize(b, t, (*a).len * 2));
                b = self.bottom.load(Relaxed);
            }

            (*a).write(b, value);
            fence(Release);
            self.bottom.store(b.wrapping_add(1), Relaxed);
        }
    }

    fn pop(&self) -> Option<T> {
        let b = self.bottom.load(Relaxed);
        let t = self.top.load(Relaxed);
        if b.wrapping_sub(t) <= 0 {
            return None;
        }

        let b = b.wrapping_sub(1);
        self.bottom.store(b, Relaxed);
        let a = self.array.load_raw(Relaxed);
        fence(SeqCst);
        let t = self.top.load(Relaxed);

        let size = b.wrapping_sub(t);

        if size >= 0 {
            unsafe {
                let mut value = Some((*a).read(b));
                if t == b {
                    if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) != t {
                        mem::forget(value.take());
                    }
                    self.bottom.store(b.wrapping_add(1), Relaxed);
                } else {
                    if (*a).len > MIN_LENGTH && size < (*a).len as isize / 4 {
                        self.replace(a, (*a).resize(b, t, (*a).len / 2));
                    }
                }
                value
            }
        } else {
            self.bottom.store(b.wrapping_add(1), Relaxed);
            None
        }
    }

    fn steal(&self) -> Option<T> {
        epoch::pin(|pin| {
            loop {
                let t = self.top.load(Acquire);
                fence(SeqCst);
                let b = self.bottom.load(Acquire);

                if b.wrapping_sub(t) <= 0 {
                    return None;
                }

                unsafe {
                    let a = self.array.load(Acquire, pin).unwrap();
                    let value = a.read(t);

                    if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) == t {
                        return Some(value);
                    }
                    mem::forget(value);
                }
            }
        })
    }
}

impl<T> Drop for Deque<T> {
    fn drop(&mut self) {
        let t = self.top.load(Relaxed);
        let b = self.bottom.load(Relaxed);
        let a = self.array.load_raw(Relaxed);

        unsafe {
            let a = Box::from_raw(a);
            let mut i = t;
            while i != b {
                drop(a.read(i));
                i = i.wrapping_add(1);
            }
        }
    }
}

// TODO: Sync and Send (use PhantomData!)

pub struct Worker<T> {
    deque: Arc<Deque<T>>,
}

impl<T> Worker<T> {
    fn push(&self, value: T) {
        self.deque.push(value);
    }

    fn pop(&self) -> Option<T> {
        self.deque.pop()
    }
}

pub struct Stealer<T> {
    deque: Arc<Deque<T>>,
}

impl<T> Stealer<T> {
    fn steal(&self) -> Option<T> {
        self.deque.steal()
    }
}

pub fn deque<T>() -> (Worker<T>, Stealer<T>) {
    let d = Arc::new(Deque::new());
    let worker = Worker { deque: d.clone() };
    let stealer = Stealer { deque: d };
    (worker, stealer)
}

#[cfg(test)]
mod tests {
    extern crate rand;

    use std::thread;
    use std::sync::Arc;

    use self::rand::Rng;

    use super::deque;

    #[test]
    fn simple() {
    }
}

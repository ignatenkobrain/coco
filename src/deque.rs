//! TODO

use std::cmp;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, fence};
use std::sync::atomic::Ordering::{Acquire, Release, Relaxed, SeqCst};

use epoch::{self, Atomic, Pin};

/// Minimum array length for a deque.
const MIN_LENGTH: usize = 16;

/// A buffer where deque elements get stored.
struct Array<T> {
    /// Pointer to the allocated memory.
    ptr: *mut T,
    /// Length of the array. Always a power of two.
    len: usize,
}

impl<T> Array<T> {
    /// Returns a new array with the specified length.
    fn new(len: usize) -> Self {
        let mut v = Vec::with_capacity(len);
        let ptr = v.as_mut_ptr();
        mem::forget(v);
        Array {
            ptr: ptr,
            len: len,
        }
    }

    /// Returns a pointer to the element at the specified `index`.
    unsafe fn at(&self, index: isize) -> *mut T {
        self.ptr.offset(index & (self.len - 1) as isize)
    }

    /// Writes `value` into the specified `index`.
    unsafe fn write(&self, index: isize, value: T) {
        ptr::write(self.at(index), value)
    }

    /// Reads the value from the specified `index`.
    unsafe fn read(&self, index: isize) -> T {
        ptr::read(self.at(index))
    }
}

fn pin_with_fence<F, T>(f: F) -> T
    where F: FnOnce(&Pin) -> T
{
    if epoch::is_pinned() {
        fence(SeqCst);
    }
    epoch::pin(|pin| f(pin))
}

struct Deque<T> {
    bottom: AtomicIsize,
    top: AtomicIsize,
    array: Atomic<Array<T>>,
}

/// A work-stealing deque.
///
/// TODO
/// 1. Dynamic Circular Work-Stealing Deque
/// 2. Correct and Efficient Work-Stealing for Weak Memory Models
/// CDSChecker: Checking Concurrent Data Structures Written with C/C++ Atomics
/// http://plrg.eecs.uci.edu/publications/c11modelcheck.pdf
/// https://github.com/computersforpeace/model-checker-benchmarks/tree/master/chase-lev-deque-bugfix
impl<T> Deque<T> {
    /// Returns a new, empty deque.
    fn new() -> Self {
        Deque {
            bottom: AtomicIsize::new(0),
            top: AtomicIsize::new(0),
            array: Atomic::new(Array::new(MIN_LENGTH)),
        }
    }

    /// Returns the number of elements in the deque.
    fn len(&self) -> usize {
        let b = self.bottom.load(Relaxed);
        let t = self.top.load(Relaxed);
        cmp::max(b.wrapping_sub(t), 0) as usize
    }

    #[cold]
    unsafe fn resize(&self, old: *mut Array<T>, b: isize, t: isize, len: usize) -> *mut Array<T> {
        let new = Array::new(len);
        let mut i = t;
        while i != b {
            ptr::copy_nonoverlapping((*old).at(i), new.at(i), 1);
            i = i.wrapping_add(1);
        }
        epoch::pin(|pin| {
            epoch::defer_free(old, pin);
            self.array.store_box(Box::new(new), Release, pin).as_raw()
        })
    }

    /// Pushes an element onto the bottom of the deque.
    fn push(&self, value: T) {
        unsafe {
            let b = self.bottom.load(Relaxed);
            let t = self.top.load(Acquire);
            let mut a = self.array.load_raw(Relaxed);

            let width = b.wrapping_sub(t);
            let len = (*a).len;

            if width == len as isize {
                a = self.resize(a, b, t, 2 * len);
            }

            (*a).write(b, value);
            fence(Release);
            self.bottom.store(b.wrapping_add(1), Relaxed);
        }
    }

    /// Pops an element fron the bottom of the deque.
    fn pop(&self) -> Option<T> {
        let b = self.bottom.load(Relaxed);
        let t = self.top.load(Relaxed);
        let width = b.wrapping_sub(t);

        if width <= 0 {
            return None;
        }

        let b = b.wrapping_sub(1);
        self.bottom.store(b, Relaxed);
        let a = self.array.load_raw(Relaxed);

        fence(SeqCst);

        let t = self.top.load(Relaxed);
        let width = b.wrapping_sub(t);

        if width >= 0 {
            let mut value = unsafe { Some((*a).read(b)) };

            if t == b {
                if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) != t {
                    mem::forget(value.take());
                }
                self.bottom.store(b.wrapping_add(1), Relaxed);
            } else {
                unsafe {
                    let len = (*a).len;
                    if len > MIN_LENGTH && width < len as isize / 4 {
                        self.resize(a, b, t, len / 2);
                    }
                }
            }
            value
        } else {
            self.bottom.store(b.wrapping_add(1), Relaxed);
            None
        }
    }

    fn steal_helper(&self, b: isize, t: isize, pin: &Pin) -> Result<Option<T>, ()> {
        if b.wrapping_sub(t) <= 0 {
            Ok(None)
        } else {
            let a = self.array.load(Acquire, pin).unwrap();
            let value = unsafe { a.read(t) };

            if self.top.compare_and_swap(t, t.wrapping_add(1), SeqCst) == t {
                Ok(Some(value))
            } else {
                mem::forget(value);
                Err(())
            }
        }
    }

    /// Steals an element fron the top of the deque.
    fn steal(&self) -> Option<T> {
        let mut t = self.top.load(Acquire);
        pin_with_fence(|pin| {
            loop {
                let b = self.bottom.load(Acquire);

                if let Ok(v) = self.steal_helper(b, t, pin) {
                    return v;
                }

                t = self.top.load(Acquire);
                fence(SeqCst);
            }
        })
    }

    /// Attempts to steal an element fron the top of the deque.
    ///
    /// Returns the stolen value (if there is any) on success, or an error if another thread
    /// concurrently took the element from the top.
    fn steal_weak(&self) -> Result<Option<T>, ()> {
        let mut t = self.top.load(Acquire);
        pin_with_fence(|pin| {
            let b = self.bottom.load(Acquire);
            self.steal_helper(b, t, pin)
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

pub struct Worker<T> {
    deque: Arc<Deque<T>>,
    _marker: PhantomData<*mut ()>, // !Send + !Sync
}

unsafe impl<T: Send> Send for Worker<T> {}

impl<T> Worker<T> {
    /// Returns the number of elements in the deque.
    pub fn len(&self) -> usize {
        self.deque.len()
    }

    /// Pushes an element onto the bottom of the deque.
    pub fn push(&self, value: T) {
        self.deque.push(value);
    }

    /// Pops an element fron the bottom of the deque.
    pub fn pop(&self) -> Option<T> {
        self.deque.pop()
    }
}

impl<T> fmt::Debug for Worker<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Worker {{ ... }}")
    }
}

pub struct Stealer<T> {
    deque: Arc<Deque<T>>,
    _marker: PhantomData<*mut ()>, // !Send + !Sync
}

unsafe impl<T: Send> Send for Stealer<T> {}
unsafe impl<T: Send> Sync for Stealer<T> {}

impl<T> Stealer<T> {
    /// Returns the number of elements in the deque.
    pub fn len(&self) -> usize {
        self.deque.len()
    }

    /// Steals an element fron the top of the deque.
    pub fn steal(&self) -> Option<T> {
        self.deque.steal()
    }

    /// Attempts to steal an element fron the top of the deque.
    ///
    /// Returns the stolen value (if there is any) on success, or an error if another thread
    /// concurrently took the element from the top.
    pub fn steal_weak(&self) -> Result<Option<T>, ()> {
        self.deque.steal_weak()
    }
}

impl<T> Clone for Stealer<T> {
    fn clone(&self) -> Self {
        Stealer {
            deque: self.deque.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for Stealer<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stealer {{ ... }}")
    }
}

/// Returns a new work-stealing deque.
///
/// The worker is unique, while stealers can be cloned and distributed among multiple threads.
/// The deque will be destructed as soon as it's worker and all it's stealers get dropped.
pub fn new<T>() -> (Worker<T>, Stealer<T>) {
    let d = Arc::new(Deque::new());
    let worker = Worker {
        deque: d.clone(),
        _marker: PhantomData,
    };
    let stealer = Stealer {
        deque: d,
        _marker: PhantomData,
    };
    (worker, stealer)
}

#[cfg(test)]
mod tests {
    extern crate rand;

    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::atomic::Ordering::SeqCst;
    use std::thread;

    use self::rand::Rng;

    #[test]
    fn simple() {
        let (w, s) = super::new();
        assert_eq!(w.pop(), None);
        assert_eq!(s.steal(), None);
        assert_eq!(w.len(), 0);
        assert_eq!(s.len(), 0);

        w.push(1);
        assert_eq!(w.len(), 1);
        assert_eq!(s.len(), 1);
        assert_eq!(w.pop(), Some(1));
        assert_eq!(w.pop(), None);
        assert_eq!(s.steal(), None);
        assert_eq!(w.len(), 0);
        assert_eq!(s.len(), 0);

        w.push(2);
        assert_eq!(s.steal(), Some(2));
        assert_eq!(s.steal(), None);
        assert_eq!(w.pop(), None);
    }

    #[test]
    fn steal_push() {
        const STEPS: usize = 50_000;

        let (w, s) = super::new();
        let t = thread::spawn(move || {
            for i in 0..STEPS {
                loop {
                    if let Some(v) = s.steal() {
                        assert_eq!(i, v);
                        break;
                    }
                }
            }
        });

        for i in 0..STEPS {
            w.push(i);
        }
        t.join().unwrap();
    }

    #[test]
    fn stampede() {
        const COUNT: usize = 50_000;

        let (w, s) = super::new();

        for i in 0..COUNT {
            w.push(Box::new(i + 1));
        }
        let remaining = Arc::new(AtomicUsize::new(COUNT));

        let threads = (0..8).map(|_| {
            let s = s.clone();
            let remaining = remaining.clone();

            thread::spawn(move || {
                let mut last = 0;
                while remaining.load(SeqCst) > 0 {
                    if let Some(x) = s.steal() {
                        assert!(last < *x);
                        last = *x;
                        remaining.fetch_sub(1, SeqCst);
                    }
                }
            })
        }).collect::<Vec<_>>();

        let mut last = COUNT + 1;
        while remaining.load(SeqCst) > 0 {
            if let Some(x) = w.pop() {
                assert!(last > *x);
                last = *x;
                remaining.fetch_sub(1, SeqCst);
            }
        }

        for t in threads {
            t.join().unwrap();
        }
    }

    #[test]
    fn stress() {
        const COUNT: usize = 50_000;

        let (w, s) = super::new();
        let done = Arc::new(AtomicBool::new(false));
        let hits = Arc::new(AtomicUsize::new(0));

        let threads = (0..8).map(|_| {
            let s = s.clone();
            let done = done.clone();
            let hits = hits.clone();

            thread::spawn(move || {
                while !done.load(SeqCst) {
                    if let Some(_) = s.steal() {
                        hits.fetch_add(1, SeqCst);
                    }
                }
            })
        }).collect::<Vec<_>>();

        let mut rng = rand::thread_rng();
        let mut expected = 0;
        while expected < COUNT {
            if rng.gen_range(0, 3) == 0 {
                if w.pop().is_some() {
                    hits.fetch_add(1, SeqCst);
                }
            } else {
                w.push(expected);
                expected += 1;
            }
        }

        while hits.load(SeqCst) < COUNT {
            if w.pop().is_some() {
                hits.fetch_add(1, SeqCst);
            }
        }
        done.store(true, SeqCst);

        for t in threads {
            t.join().unwrap();
        }
    }

    #[test]
    fn no_starvation() {
        const COUNT: usize = 50_000;

        let (w, s) = super::new();
        let done = Arc::new(AtomicBool::new(false));

        let (threads, hits): (Vec<_>, Vec<_>) = (0..8).map(|_| {
            let s = s.clone();
            let done = done.clone();
            let hits = Arc::new(AtomicUsize::new(0));

            let t = {
                let hits = hits.clone();
                thread::spawn(move || {
                    while !done.load(SeqCst) {
                        if let Some(_) = s.steal() {
                            hits.fetch_add(1, SeqCst);
                        }
                    }
                })
            };

            (t, hits)
        }).unzip();

        let mut rng = rand::thread_rng();
        let mut my_hits = 0;
        loop {
            for i in 0..rng.gen_range(0, COUNT) {
                if rng.gen_range(0, 3) == 0 && my_hits == 0 {
                    if w.pop().is_some() {
                        my_hits += 1;
                    }
                } else {
                    w.push(i);
                }
            }

            if my_hits > 0 && hits.iter().all(|h| h.load(SeqCst) > 0) {
                break;
            }
        }
        done.store(true, SeqCst);

        for t in threads {
            t.join().unwrap();
        }
    }

    #[test]
    fn destructors() {
        const COUNT: usize = 50_000;

        struct Elem(usize, Arc<Mutex<Vec<usize>>>);

        impl Drop for Elem {
            fn drop(&mut self) {
                self.1.lock().unwrap().push(self.0);
            }
        }

        let (w, s) = super::new();

        let dropped = Arc::new(Mutex::new(Vec::new()));
        let remaining = Arc::new(AtomicUsize::new(COUNT));
        for i in 0..COUNT {
            w.push(Elem(i, dropped.clone()));
        }

        let threads = (0..8).map(|_| {
            let s = s.clone();
            let remaining = remaining.clone();

            thread::spawn(move || {
                for _ in 0..1000 {
                    if s.steal().is_some() {
                        remaining.fetch_sub(1, SeqCst);
                    }
                }
            })
        }).collect::<Vec<_>>();

        for _ in 0..1000 {
            if w.pop().is_some() {
                remaining.fetch_sub(1, SeqCst);
            }
        }

        for t in threads {
            t.join().unwrap();
        }

        let rem = remaining.load(SeqCst);
        assert!(rem > 0);
        assert_eq!(w.len(), rem);
        assert_eq!(s.len(), rem);

        {
            let mut v = dropped.lock().unwrap();
            assert_eq!(v.len(), COUNT - rem);
            v.clear();
        }

        drop(w);
        drop(s);

        {
            let mut v = dropped.lock().unwrap();
            assert_eq!(v.len(), rem);
            v.sort();
            for w in v.windows(2) {
                assert_eq!(w[0] + 1, w[1]);
            }
        }
    }
}

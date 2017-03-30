use std::fmt;
use std::marker::PhantomData;
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

    #[cold]
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

pub struct Worker<T> {
    deque: Arc<Deque<T>>,
    _marker: PhantomData<*mut ()>, // !Send + !Sync
}

unsafe impl<T: Send> Send for Worker<T> {}

impl<T> Worker<T> {
    pub fn push(&self, value: T) {
        self.deque.push(value);
    }

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
    pub fn steal(&self) -> Option<T> {
        self.deque.steal()
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

pub fn deque<T>() -> (Worker<T>, Stealer<T>) {
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

    use super::deque;

    #[test]
    fn simple() {
        let (w, s) = deque();
        assert_eq!(w.pop(), None);
        assert_eq!(s.steal(), None);

        w.push(1);
        assert_eq!(w.pop(), Some(1));
        assert_eq!(w.pop(), None);
        assert_eq!(s.steal(), None);

        w.push(2);
        assert_eq!(s.steal(), Some(2));
        assert_eq!(s.steal(), None);
        assert_eq!(w.pop(), None);
    }

    #[test]
    fn steal_push() {
        const STEPS: usize = 50_000;

        let (w, s) = deque();
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

        let (w, s) = deque();

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

        let (w, s) = deque();
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

        let (w, s) = deque();
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

        let (w, s) = deque();

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

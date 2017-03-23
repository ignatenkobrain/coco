//! TODO
//!

use std::ptr;
use std::marker::PhantomData;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};

use Pin;

/// An atomic nullable pointer.
#[derive(Debug)]
pub struct Atomic<T> {
    raw: AtomicPtr<T>,
    _marker: PhantomData<*const ()>, // !Send + !Sync
}

unsafe impl<T: Send + Sync> Send for Atomic<T> {}
unsafe impl<T: Send + Sync> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    /// Returns a new, null atomic pointer.
    pub fn null() -> Self {
        unsafe { Self::from_raw(ptr::null_mut()) }
    }

    /// Allocates `data` on the heap and returns a new atomic pointer that points to it.
    pub fn new(data: T) -> Self {
        unsafe { Self::from_raw(Box::into_raw(Box::new(data))) }
    }

    /// Returns a new atomic pointer initialized with `ptr`.
    pub fn from_ptr(ptr: Ptr<T>) -> Self {
        unsafe { Self::from_raw(ptr.raw) }
    }

    /// Returns a new atomic pointer initialized with `b`.
    pub fn from_box(b: Box<T>) -> Self {
        unsafe { Self::from_raw(Box::into_raw(b)) }
    }

    /// Returns a new atomic pointer initialized with `raw`.
    pub unsafe fn from_raw(raw: *mut T) -> Self {
        Atomic {
            raw: AtomicPtr::new(raw),
            _marker: PhantomData,
        }
    }

    /// Loads the atomic pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn load<'p>(&self, order: Ordering, _: &'p Pin) -> Ptr<'p, T> {
        unsafe { Ptr::from_raw(self.raw.load(order)) }
    }

    /// Loads the atomic pointer as a raw pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn load_raw(&self, order: Ordering) -> *mut T {
        self.raw.load(order) as *mut T
    }

    /// Stores `new` into the atomic.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn store<'p>(&self, new: Ptr<'p, T>, order: Ordering) {
        self.raw.store(new.raw, order);
    }

    /// Stores `new` into the atomic and returns it.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn store_box<'p>(&self, new: Box<T>, order: Ordering, _: &'p Pin) -> Ptr<'p, T> {
        let ptr = unsafe { Ptr::from_raw(Box::into_raw(new)) };
        self.raw.store(ptr.raw, order);
        ptr
    }

    /// Stores `new` into the atomic.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn store_raw(&self, new: *mut T, order: Ordering) {
        self.raw.store(new, order);
    }

    /// Stores `new` into the atomic, returning the old pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn swap<'p>(&self, new: Ptr<'p, T>, order: Ordering) -> Ptr<'p, T> {
        unsafe { Ptr::from_raw(self.raw.swap(new.raw, order)) }
    }

    /// Stores `new` into the atomic, returning the old pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn swap_box<'p>(&self, new: Box<T>, order: Ordering) -> Ptr<'p, T> {
        unsafe { Ptr::from_raw(self.raw.swap(Box::into_raw(new), order)) }
    }

    /// Stores `new` into the atomic, returning the old pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn swap_raw<'p>(&self, new: *mut T, order: Ordering) -> Ptr<'p, T> {
        Ptr::from_raw(self.raw.swap(new, order))
    }

    /// If the atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the atomic pointer is returned.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn cas<'p>(&self, current: Ptr<'p, T>, new: Ptr<'p, T>, order: Ordering)
                   -> Result<(), Ptr<'p, T>> {
        let previous = self.raw.compare_and_swap(current.raw, new.raw, order);
        if previous == current.raw {
            Ok(())
        } else {
            unsafe { Err(Ptr::from_raw(previous)) }
        }
    }

    /// If the atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the atomic pointer is returned.
    ///
    /// This method can sometimes spuriously fail even when comparison succeeds, which can result
    /// in more efficient code on some platforms.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn cas_weak<'p>(&self, current: Ptr<'p, T>, new: Ptr<'p, T>, order: Ordering)
                        -> Result<(), Ptr<'p, T>> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        match self.raw.compare_exchange_weak(current.raw, new.raw, order, failure_order) {
            Ok(_) => Ok(()),
            Err(previous) => unsafe { Err(Ptr::from_raw(previous)) },
        }
    }

    /// If the atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On success the
    /// new pointer is returned. On failure the current value of the atomic pointer and `new` are
    /// returned.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn cas_box<'p>(&self, current: Ptr<'p, T>, mut new: Box<T>, order: Ordering)
                       -> Result<Ptr<'p, T>, (Ptr<'p, T>, Box<T>)> {
        let previous = self.raw.compare_and_swap(current.raw, new.as_mut(), order);
        if previous == current.raw {
            unsafe { Ok(Ptr::from_raw(Box::into_raw(new))) }
        } else {
            unsafe { Err((Ptr::from_raw(previous), new)) }
        }
    }

    /// If the atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On success the
    /// new pointer is returned. On failure the current value of the atomic pointer and `new` are
    /// returned.
    ///
    /// This method can sometimes spuriously fail even when comparison succeeds, which can result
    /// in more efficient code on some platforms.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn cas_box_weak<'p>(&self, current: Ptr<'p, T>, mut new: Box<T>, order: Ordering)
                            -> Result<Ptr<'p, T>, (Ptr<'p, T>, Box<T>)> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        match self.raw.compare_exchange_weak(current.raw, new.as_mut(), order, failure_order) {
            Ok(_) => unsafe { Ok(Ptr::from_raw(Box::into_raw(new))) },
            Err(previous) => unsafe { Err((Ptr::from_raw(previous), new)) },
        }
    }

    /// If the atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the atomic pointer is returned.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn cas_raw(&self, current: *mut T, new: *mut T, order: Ordering)
                          -> Result<(), *mut T> {
        let previous = self.raw.compare_and_swap(current, new, order);
        if previous == current {
            Ok(())
        } else {
            Err(previous)
        }
    }

    /// If the atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the atomic pointer is returned.
    ///
    /// This method can sometimes spuriously fail even when comparison succeeds, which can result
    /// in more efficient code on some platforms.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn cas_raw_weak(&self, current: *mut T, new: *mut T, order: Ordering)
                               -> Result<(), *mut T> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        match self.raw.compare_exchange_weak(current, new, order, failure_order) {
            Ok(_) => Ok(()),
            Err(previous) => Err(previous),
        }
    }
}

/// A nullable pointer.
#[derive(Debug)]
pub struct Ptr<'p, T: 'p> {
    raw: *mut T, // !Send + !Sync
    _marker: PhantomData<&'p T>,
}

impl<'p, T> Clone for Ptr<'p, T> {
    fn clone(&self) -> Self {
        Ptr {
            raw: self.raw,
            _marker: PhantomData,
        }
    }
}

impl<'p, T> Copy for Ptr<'p, T> {}

impl<'p, T> Ptr<'p, T> {
    /// Returns a null pointer.
    pub fn null() -> Self {
        unsafe { Self::from_raw(ptr::null_mut()) }
    }

    /// Constructs a pointer from a raw pointer.
    pub unsafe fn from_raw(raw: *mut T) -> Self {
        Ptr {
            raw: raw,
            _marker: PhantomData,
        }
    }

    /// Returns `true` if the pointer is null.
    pub fn is_null(&self) -> bool {
        self.raw.is_null()
    }

    /// Converts the pointer to a reference.
    pub fn as_ref(&self) -> Option<&'p T> {
        unsafe { self.raw.as_ref() }
    }

    /// Converts the pointer to a raw pointer.
    pub fn as_raw(&self) -> *mut T {
        self.raw
    }

    /// Returns a reference to the pointing object.
    ///
    /// # Panics
    ///
    /// Panics if the pointer is null.
    pub fn unwrap(&self) -> &'p T {
        self.as_ref().unwrap()
    }
}

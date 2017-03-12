use std::mem;
use std::ptr;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};
use std::ops::Deref;

use super::Guard;

// TODO: derive Debug on public structs
// TODO: impl Default

pub struct Ptr<'g, T: 'g> {
    ptr: *mut T, // !Send + !Sync
    _marker: PhantomData<&'g T>,
}

impl<'g, T> Clone for Ptr<'g, T> {
    fn clone(&self) -> Self {
        Ptr {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<'g, T> Copy for Ptr<'g, T> {}

impl<'g, T> Ptr<'g, T> {
    pub fn null() -> Self {
        unsafe { Self::from_raw(ptr::null_mut()) }
    }

    pub unsafe fn from_raw(raw: *mut T) -> Self {
        Ptr {
            ptr: raw,
            _marker: PhantomData,
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn as_ref(&self) -> Option<&'g T> {
        unsafe { self.ptr.as_ref() }
    }

    pub fn as_raw(&self) -> *mut T {
        self.ptr
    }

    pub fn unwrap(&self) -> &'g T {
        self.as_ref().unwrap()
    }

    pub unsafe fn unlinked(&self, guard: &'g Guard) {
        if !self.ptr.is_null() {
            super::unlinked(self.ptr, 1, guard);
        }
    }
}

pub struct Atomic<T> {
    ptr: AtomicPtr<T>,
    _marker: PhantomData<*const ()>, // !Send + !Sync
}

unsafe impl<T: Send + Sync> Send for Atomic<T> {}
unsafe impl<T: Send + Sync> Sync for Atomic<T> {}

impl<T> Atomic<T> {
    pub fn null() -> Self {
        unsafe { Self::from_raw(ptr::null_mut()) }
    }

    pub fn new(data: T) -> Self {
        unsafe { Self::from_raw(Box::into_raw(Box::new(data))) }
    }

    pub unsafe fn from_raw(raw: *mut T) -> Self {
        Atomic {
            ptr: AtomicPtr::new(raw),
            _marker: PhantomData,
        }
    }

    pub fn load<'g>(&self, order: Ordering, _: &'g Guard) -> Ptr<'g, T> {
        unsafe { Ptr::from_raw(self.ptr.load(order)) }
    }

    pub fn load_raw(&self, order: Ordering) -> *mut T {
        self.ptr.load(order) as *mut T
    }

    pub fn store<'g>(&self, new: Ptr<'g, T>, order: Ordering) {
        self.ptr.store(new.ptr, order);
    }

    pub fn store_box<'g>(&self, new: Box<T>, order: Ordering, guard: &'g Guard) -> Ptr<'g, T> {
        let r = unsafe { Ptr::from_raw(Box::into_raw(new)) };
        self.ptr.store(r.ptr, order);
        r
    }

    pub fn cas<'g>(&self, current: Ptr<'g, T>, new: Ptr<'g, T>, order: Ordering)
                   -> Result<(), Ptr<'g, T>> {
        let previous = self.ptr.compare_and_swap(current.ptr, new.ptr, order);
        if previous == current.ptr {
            Ok(())
        } else {
            unsafe { Err(Ptr::from_raw(previous)) }
        }
    }

    pub fn cas_weak<'g>(&self, current: Ptr<'g, T>, new: Ptr<'g, T>, order: Ordering)
                        -> Result<(), Ptr<'g, T>> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        match self.ptr.compare_exchange_weak(current.ptr, new.ptr, order, failure_order) {
            Ok(_) => Ok(()),
            Err(previous) => unsafe { Err(Ptr::from_raw(previous)) },
        }
    }

    pub fn cas_box<'g>(&self, current: Ptr<'g, T>, mut new: Box<T>, order: Ordering)
                       -> Result<Ptr<'g, T>, (Ptr<'g, T>, Box<T>)> {
        let previous = self.ptr.compare_and_swap(current.ptr, new.as_mut(), order);
        if previous == current.ptr {
            unsafe { Ok(Ptr::from_raw(Box::into_raw(new))) }
        } else {
            unsafe { Err((Ptr::from_raw(previous), new)) }
        }
    }

    pub fn cas_box_weak<'g>(&self, current: Ptr<'g, T>, mut new: Box<T>, order: Ordering)
                            -> Result<Ptr<'g, T>, (Ptr<'g, T>, Box<T>)> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        match self.ptr.compare_exchange_weak(current.ptr, new.as_mut(), order, failure_order) {
            Ok(_) => unsafe { Ok(Ptr::from_raw(Box::into_raw(new))) },
            Err(previous) => unsafe { Err((Ptr::from_raw(previous), new)) },
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO
}

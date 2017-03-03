use super::Pin;

use std::mem;
use std::ptr;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};
use std::ops::Deref;

pub struct TaggedPtr<'p, T: 'p> {
    data: usize,
    _marker: PhantomData<(*mut T, &'p T)>, // !Send + !Sync
}

impl<'a, T> Clone for TaggedPtr<'a, T> {
    fn clone(&self) -> Self {
        TaggedPtr {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<'a, T> Copy for TaggedPtr<'a, T> {}

fn low_bits<T>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

fn raw_and_tag<T>(raw: *mut T, tag: usize) -> usize {
    let mask = low_bits::<T>();
    assert!(tag <= mask, "tag too large to fit into the unused bits: {} > {}", tag, mask);
    raw as usize | tag
}

impl<'p, T: 'p> TaggedPtr<'p, T> {
    unsafe fn from_data(data: usize) -> Self {
        TaggedPtr {
            data: data,
            _marker: PhantomData,
        }
    }

    pub fn null(tag: usize) -> Self {
        unsafe { Self::from_data(raw_and_tag::<T>(ptr::null_mut(), tag)) }
    }

    pub unsafe fn from_raw(raw: *mut T, tag: usize) -> Self {
        Self::from_data(raw_and_tag(raw, tag))
    }

    pub fn is_null(&self) -> bool {
        self.as_raw().is_null()
    }

    pub fn as_ref(&self) -> Option<&'p T> {
        unsafe { self.as_raw().as_ref() }
    }

    pub fn as_raw(&self) -> *mut T {
        (self.data & !low_bits::<T>()) as *mut T
    }

    pub fn unwrap(&self) -> &'p T {
        self.as_ref().unwrap()
    }

    pub fn tag(&self) -> usize {
        self.data & low_bits::<T>()
    }

    pub fn with_tag(&self, tag: usize) -> Self {
        unsafe { Self::from_raw(self.as_raw(), tag) }
    }

    pub unsafe fn unlinked(&self, pin: &'p Pin) {
        self.as_ref().map(|r| super::unlinked(r as *const _ as *mut T, 1, pin));
    }
}

pub struct TaggedAtomic<T> {
    data: AtomicUsize,
    _marker: PhantomData<*mut T>, // !Send + !Sync
}

unsafe impl<T: Sync> Send for TaggedAtomic<T> {}
unsafe impl<T: Sync> Sync for TaggedAtomic<T> {}

impl<T> TaggedAtomic<T> {
    unsafe fn from_data(data: usize) -> Self {
        TaggedAtomic {
            data: AtomicUsize::new(data),
            _marker: PhantomData,
        }
    }

    pub fn null(tag: usize) -> Self {
        unsafe { Self::from_raw(ptr::null_mut(), tag) }
    }

    pub fn new(data: T, tag: usize) -> Self {
        unsafe { Self::from_raw(Box::into_raw(Box::new(data)), tag) }
    }

    pub unsafe fn from_raw(raw: *mut T, tag: usize) -> Self {
        Self::from_data(raw_and_tag(raw, tag))
    }

    pub fn load<'p>(&self, order: Ordering, _: &'p Pin) -> TaggedPtr<'p, T> {
        unsafe { TaggedPtr::from_data(self.data.load(order)) }
    }

    pub fn load_tag(&self, order: Ordering) -> usize {
        unsafe { TaggedPtr::<T>::from_data(self.data.load(order)).tag() }
    }

    pub fn store<'p>(&self, new: TaggedPtr<'p, T>, order: Ordering) {
        self.data.store(new.data, order);
    }

    pub fn store_box<'p>(&self, new: Box<T>, tag: usize, order: Ordering, pin: &'p Pin)
                         -> TaggedPtr<'p, T> {
        let r = unsafe { TaggedPtr::from_raw(Box::into_raw(new), tag) };
        self.data.store(r.data, order);
        r
    }

    pub fn cas<'p>(&self, current: TaggedPtr<'p, T>, new: TaggedPtr<'p, T>, order: Ordering)
                   -> Result<(), TaggedPtr<'p, T>> {
        let previous = self.data.compare_and_swap(current.data, new.data, order);
        if previous == current.data {
            Ok(())
        } else {
            unsafe { Err(TaggedPtr::from_data(previous)) }
        }
    }

    pub fn cas_weak<'p>(&self, current: TaggedPtr<'p, T>, new: TaggedPtr<'p, T>, order: Ordering)
                        -> Result<(), TaggedPtr<'p, T>> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        match self.data.compare_exchange_weak(current.data, new.data, order, failure_order) {
            Ok(_) => Ok(()),
            Err(previous) => unsafe { Err(TaggedPtr::from_data(previous)) },
        }
    }

    pub fn cas_box<'p>(&self, current: TaggedPtr<'p, T>, mut new: Box<T>, tag: usize,
                       order: Ordering)
                       -> Result<TaggedPtr<'p, T>, (TaggedPtr<'p, T>, Box<T>)> {
        let new_data = raw_and_tag(new.as_mut(), tag);
        let previous = self.data.compare_and_swap(current.data, new_data, order);
        if previous == current.data {
            mem::forget(new);
            unsafe { Ok(TaggedPtr::from_data(new_data)) }
        } else {
            unsafe { Err((TaggedPtr::from_data(previous), new)) }
        }
    }

    pub fn cas_box_weak<'p>(&self, current: TaggedPtr<'p, T>, mut new: Box<T>, tag: usize,
                            order: Ordering)
                            -> Result<TaggedPtr<'p, T>, (TaggedPtr<'p, T>, Box<T>)> {
        let failure_order = match order {
            AcqRel => Acquire,
            Release => Relaxed,
            order => order,
        };
        let new_data = raw_and_tag(new.as_mut(), tag);
        match self.data.compare_exchange_weak(current.data, new_data, order, failure_order) {
            Ok(_) => {
                mem::forget(new);
                unsafe { Ok(TaggedPtr::from_data(new_data)) }
            }
            Err(previous) => unsafe { Err((TaggedPtr::from_data(previous), new)) },
        }
    }
}

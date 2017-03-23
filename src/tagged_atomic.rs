//! TODO
//!

use std::mem;
use std::ptr;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release};

use Pin;

/// Returns the number of unused least significant bits in a pointer to `T`.
fn low_bits<T>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

/// Tags the unused least significant bits in `raw` with `tag`.
///
/// # Panics
///
/// Panics if the tag doesn't fit into the unused bits in the pointer.
fn raw_and_tag<T>(raw: *mut T, tag: usize) -> usize {
    let mask = low_bits::<T>();
    assert!(tag <= mask, "tag too large to fit into the unused bits: {} > {}", tag, mask);
    raw as usize | tag
}

/// A tagged atomic nullable pointer.
#[derive(Debug)]
pub struct TaggedAtomic<T> {
    data: AtomicUsize,
    _marker: PhantomData<*mut T>, // !Send + !Sync
}

unsafe impl<T: Send + Sync> Send for TaggedAtomic<T> {}
unsafe impl<T: Send + Sync> Sync for TaggedAtomic<T> {}

impl<T> TaggedAtomic<T> {
    /// Constructs a tagged atomic pointer from raw data.
    unsafe fn from_data(data: usize) -> Self {
        TaggedAtomic {
            data: AtomicUsize::new(data),
            _marker: PhantomData,
        }
    }

    /// Returns a new, null atomic pointer tagged with `tag`.
    pub fn null(tag: usize) -> Self {
        unsafe { Self::from_raw(ptr::null_mut(), tag) }
    }

    /// Allocates `data` on the heap and returns a new atomic pointer that points to it and is
    /// tagged with `tag`.
    pub fn new(data: T, tag: usize) -> Self {
        unsafe { Self::from_raw(Box::into_raw(Box::new(data)), tag) }
    }

    /// Returns a new atomic pointer initialized with `ptr`.
    pub fn from_ptr(ptr: TaggedPtr<T>) -> Self {
        unsafe { Self::from_data(ptr.data) }
    }

    /// Returns a new atomic pointer initialized with `b` and `tag`.
    pub fn from_box(b: Box<T>, tag: usize) -> Self {
        unsafe { Self::from_raw(Box::into_raw(b), tag) }
    }

    /// Returns a new atomic pointer initialized with `raw` and `tag`.
    pub unsafe fn from_raw(raw: *mut T, tag: usize) -> Self {
        Self::from_data(raw_and_tag(raw, tag))
    }

    /// Loads the tagged atomic pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn load<'p>(&self, order: Ordering, _: &'p Pin) -> TaggedPtr<'p, T> {
        unsafe { TaggedPtr::from_data(self.data.load(order)) }
    }

    /// Loads the tagged atomic pointer as a raw pointer and a tag.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn load_raw(&self, order: Ordering) -> (*mut T, usize) {
        let p = unsafe { TaggedPtr::<T>::from_data(self.data.load(order)) };
        (p.as_raw(), p.tag())
    }

    /// Stores `new` tagged with `tag` into the atomic.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn store<'p>(&self, new: TaggedPtr<'p, T>, order: Ordering) {
        self.data.store(new.data, order);
    }

    /// Stores `new` tagged with `tag` into the atomic and returns it.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn store_box<'p>(&self, new: Box<T>, tag: usize, order: Ordering, _: &'p Pin)
                         -> TaggedPtr<'p, T> {
        let ptr = unsafe { TaggedPtr::from_raw(Box::into_raw(new), tag) };
        self.data.store(ptr.data, order);
        ptr
    }

    /// Stores `new` tagged with `tag` into the atomic.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn store_raw<'p>(&self, new: *mut T, tag: usize, order: Ordering, _: &'p Pin)
                         -> TaggedPtr<'p, T> {
        let ptr = unsafe { TaggedPtr::from_raw(new, tag) };
        self.data.store(ptr.data, order);
        ptr
    }

    /// Stores `new` into the atomic, returning the old tagged pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn swap<'p>(&self, new: TaggedPtr<'p, T>, order: Ordering) -> TaggedPtr<'p, T> {
        unsafe { TaggedPtr::from_data(self.data.swap(new.data, order)) }
    }

    /// Stores `new` tagged with `tag` into the atomic, returning the old tagged pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn swap_box<'p>(&self, new: Box<T>, tag: usize, order: Ordering) -> TaggedPtr<'p, T> {
        let data = unsafe { TaggedPtr::from_raw(Box::into_raw(new), tag).data };
        unsafe { TaggedPtr::from_data(self.data.swap(data, order)) }
    }

    /// Stores `new` tagged with `tag` into the atomic, returning the old tagged pointer.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn swap_raw<'p>(&self, new: *mut T, tag: usize, order: Ordering)
                               -> TaggedPtr<'p, T> {
        let data = TaggedPtr::from_raw(new, tag).data;
        TaggedPtr::from_data(self.data.swap(data, order))
    }

    /// If the tagged atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the tagged atomic pointer is returned.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub fn cas<'p>(&self, current: TaggedPtr<'p, T>, new: TaggedPtr<'p, T>, order: Ordering)
                   -> Result<(), TaggedPtr<'p, T>> {
        let previous = self.data.compare_and_swap(current.data, new.data, order);
        if previous == current.data {
            Ok(())
        } else {
            unsafe { Err(TaggedPtr::from_data(previous)) }
        }
    }

    /// If the tagged atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the tagged atomic pointer is returned.
    ///
    /// This method can sometimes spuriously fail even when comparison succeeds, which can result
    /// in more efficient code on some platforms.
    ///
    /// Argument `order` describes the memory ordering of this operation.
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

    /// If the tagged atomic pointer is equal to `current`, stores `new` tagged with `tag`.
    ///
    /// The return value is a result indicating whether the new value was stored. On success the
    /// new pointer is returned. On failure the current value of the tagged atomic pointer and
    /// `new` are returned.
    ///
    /// Argument `order` describes the memory ordering of this operation.
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

    /// If the tagged atomic pointer is equal to `current`, stores `new` tagged with `tag`.
    ///
    /// The return value is a result indicating whether the new value was stored. On success the
    /// new pointer is returned. On failure the current value of the tagged atomic pointer and
    /// `new` are returned.
    ///
    /// This method can sometimes spuriously fail even when comparison succeeds, which can result
    /// in more efficient code on some platforms.
    ///
    /// Argument `order` describes the memory ordering of this operation.
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

    /// If the tagged atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the tagged atomic pointer is returned.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn cas_raw(&self, current: (*mut T, usize), new: (*mut T, usize), order: Ordering)
                          -> Result<(), (*mut T, usize)> {
        let current_data = raw_and_tag(current.0, current.1);
        let new_data = raw_and_tag(new.0, new.1);
        let previous = self.data.compare_and_swap(current_data, new_data, order);
        if previous == current_data {
            Ok(())
        } else {
            let ptr = TaggedPtr::from_data(previous);
            Err((ptr.as_raw(), ptr.tag()))
        }
    }

    /// If the tagged atomic pointer is equal to `current`, stores `new`.
    ///
    /// The return value is a result indicating whether the new value was stored. On failure the
    /// current value of the tagged atomic pointer is returned.
    ///
    /// This method can sometimes spuriously fail even when comparison succeeds, which can result
    /// in more efficient code on some platforms.
    ///
    /// Argument `order` describes the memory ordering of this operation.
    pub unsafe fn cas_raw_weak(&self, current: (*mut T, usize), new: (*mut T, usize),
                               order: Ordering)
                               -> Result<(), (*mut T, usize)> {
        let current_data = raw_and_tag(current.0, current.1);
        let new_data = raw_and_tag(new.0, new.1);
        let previous = self.data.compare_and_swap(current_data, new_data, order);
        if previous == current_data {
            Ok(())
        } else {
            let ptr = TaggedPtr::from_data(previous);
            Err((ptr.as_raw(), ptr.tag()))
        }
    }
}

/// A tagged nullable pointer.
#[derive(Debug)]
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

impl<'p, T: 'p> TaggedPtr<'p, T> {
    /// Constructs a nullable pointer from raw data.
    unsafe fn from_data(data: usize) -> Self {
        TaggedPtr {
            data: data,
            _marker: PhantomData,
        }
    }

    /// Returns a null pointer with a tag.
    pub fn null(tag: usize) -> Self {
        unsafe { Self::from_data(raw_and_tag::<T>(ptr::null_mut(), tag)) }
    }

    /// Constructs a tagged pointer from a raw pointer and tag.
    pub unsafe fn from_raw(raw: *mut T, tag: usize) -> Self {
        Self::from_data(raw_and_tag(raw, tag))
    }

    /// Returns `true` if the pointer is null.
    pub fn is_null(&self) -> bool {
        self.as_raw().is_null()
    }

    /// Converts the pointer to a reference.
    pub fn as_ref(&self) -> Option<&'p T> {
        unsafe { self.as_raw().as_ref() }
    }

    /// Converts the pointer to a raw pointer.
    pub fn as_raw(&self) -> *mut T {
        (self.data & !low_bits::<T>()) as *mut T
    }

    /// Returns a reference to the pointing object.
    ///
    /// # Panics
    ///
    /// Panics if the pointer is null.
    pub fn unwrap(&self) -> &'p T {
        self.as_ref().unwrap()
    }

    /// Returns the tag.
    pub fn tag(&self) -> usize {
        self.data & low_bits::<T>()
    }

    /// Constructs a new tagged pointer with a different tag.
    pub fn with_tag(&self, tag: usize) -> Self {
        unsafe { Self::from_raw(self.as_raw(), tag) }
    }
}

//! The representation of the key and value in the in-memory phase.

use core::hash;
use std::cmp;

// Bytes is a struct that implement cheap clone
// and can be safely transfer between threads.
// Bytes control the lifetime of its value.
pub struct Bytes {
    ptr: *const u8,
    len: usize,
    cap: usize,
}

const EMPTY: &[u8] = &[];

impl Bytes {
    pub fn new() -> Self {
        Self::from_static(EMPTY)
    }

    #[inline]
    pub const fn from_static(bytes: &'static [u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
            cap: 0,
        }
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        // SAFETY:
        // `self.ptr` points to valid memory for at least `self.len` bytes.
        // `self.ptr` is properly aligned for `u8`
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(mut vec: Vec<u8>) -> Bytes {
        let ptr = vec.as_mut_ptr();
        let len = vec.len();
        let cap = vec.capacity();

        if len == 0 {
            return Bytes::new();
        }

        // Prevent Vec from deallocating.
        std::mem::forget(vec);
        Bytes { ptr, len, cap }
    }
}

impl From<&[u8]> for Bytes {
    fn from(slices: &[u8]) -> Bytes {
        let vec = slices.to_vec();
        vec.into()
    }
}

// SAFETY:
// 1. `self.ptr` was originally obtained from a heap allocation (via `Vec<u8>`)
//    and has not been moved or deallocated elsewhere before this `Drop` call.
// 2. The alignment of `u8` is 1, so using `Layout::from_size_align(self.cap, 1)`
//    is valid and matches the allocation made by the original `Vec<u8>`.
impl Drop for Bytes {
    fn drop(&mut self) {
        if self.cap != 0 {
            unsafe {
                std::alloc::dealloc(
                    self.ptr as *mut u8,
                    std::alloc::Layout::from_size_align(self.cap, 1).unwrap(),
                )
            }
        }
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialOrd for Bytes {
    fn partial_cmp(&self, other: &Bytes) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Bytes {
    fn cmp(&self, other: &Bytes) -> cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl hash::Hash for Bytes {
    fn hash<H>(&self, state: &mut H)
    where
        H: hash::Hasher,
    {
        self.as_slice().hash(state);
    }
}

impl Eq for Bytes {}

impl Clone for Bytes {
    #[inline]
    fn clone(&self) -> Bytes {
        Self {
            ptr: self.ptr,
            len: self.len,
            // Set the capacity to zero to prevent double free.
            cap: 0,
        }
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bytes")
            .field("actual value", &self.as_slice())
            .field("ptr", &format_args!("0x{:x}", self.ptr as usize))
            .field("len", &self.len)
            .field("cap", &self.cap)
            .finish()
    }
}

unsafe impl Send for Bytes {}
unsafe impl Sync for Bytes {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_drop() {
        let mut v = Vec::with_capacity(1000);
        v.push(1);
        v.push(2);
        v.push(3);

        let _: Bytes = v.into();

        let _ = Bytes::from_static(b"hello");
    }

    #[test]
    fn test_bytes_double_free() {
        let b1 = Bytes::new();
        let b2 = Bytes::new();

        drop(b1);
        drop(b2);
    }

    #[test]
    fn test_bytes_read() {
        let mut v = Vec::with_capacity(1000);
        v.push(1);
        v.push(2);
        v.push(3);

        let b: Bytes = v.into();
        assert_eq!([1, 2, 3], *b.as_ref());

        let b1 = Bytes::new();
        assert_eq!(EMPTY, b1.as_ref());
    }

    #[test]
    fn test_bytes_align() {
        let mut v: Vec<u8> = Vec::with_capacity(1000);
        let p = v.as_mut_ptr();
        let alignment = unsafe { std::mem::align_of_val(&*p) };
        assert_eq!(alignment, 1);
    }

    #[test]
    fn test_bytes_ord() {
        let b1 = Bytes::from(vec![1, 2, 3]);
        let b2 = Bytes::from(vec![1, 2, 3, 4]);
        let b3 = Bytes::from(vec![1, 2, 4]);
        assert!(b1 < b2);
        assert!(b1 < b3);
        assert!(b2 < b3);
    }

    #[test]
    fn test_bytes_clone() {
        let b1 = Bytes::from(vec![1, 2, 3]);
        let b2 = b1.clone();
        assert_eq!(*b1.as_ref(), *b2.as_ref());
        drop(b2);
        assert_eq!(b1.as_ref(), [1, 2, 3]);
    }
}

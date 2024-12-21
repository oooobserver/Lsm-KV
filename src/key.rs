use std::cmp::Reverse;

use crate::byte::Bytes;

/// The key contains the actual key value's u8 array format and the version number.
pub struct Key<T: AsRef<[u8]>>(T, u64);

// Use Bytes as the inner struct.
pub type KeyBytes = Key<Bytes>;

// Use array reference as the inner struct.
pub type KeySlice<'a> = Key<&'a [u8]>;

const DEFAULT_VERSION: u64 = 0;

impl KeyBytes {
    pub fn new(bytes: Bytes, version: u64) -> Self {
        Self(bytes, version)
    }
}

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.0.as_ref().len() + std::mem::size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    pub fn version(&self) -> u64 {
        self.1
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn from_slice(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice, DEFAULT_VERSION)
    }

    pub fn to_key_bytes(&self) -> KeyBytes {
        let bytes = Bytes::from(self.0.to_vec());
        Key(bytes, self.1)
    }
}

impl<T: AsRef<[u8]> + std::fmt::Debug> std::fmt::Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("bytes", &self.0)
            .field("version", &self.1)
            .finish()
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default(), DEFAULT_VERSION)
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ref(), self.1).eq(&(other.0.as_ref(), other.1))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        println!("actual clone");
        Self(self.0.clone(), self.1)
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

// Key's comparison:
// First compare the actual value.
// If the value is the same, then compare the version number.
// The bigger the version number is, the newer the key is and
// the smaller it is.
impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.0.as_ref(), Reverse(self.1)).partial_cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.as_ref(), Reverse(self.1)).cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_skiplist::SkipMap;

    use crate::byte::Bytes;

    use super::Key;
    #[test]
    fn test_key_order() {
        let vals = vec!["1", "2", "3", "4"];
        let mut orders = vec![];
        let map = SkipMap::new();
        for val in vals {
            for version in 0..4 {
                let key = Key::new(Bytes::from_static(val.as_bytes()), version);
                map.insert(key.clone(), 0);
                orders.push(key);
            }
        }
    }
}

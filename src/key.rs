use crate::byte::Bytes;

/// The key contians the actual key value's u8 array format and the version number.
pub struct Key<T: AsRef<[u8]>>(T, u64);

pub type KeyBytes = Key<Bytes>;

const DEFAULT_VERSION: u64 = 0;

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

    fn version(&self) -> u64 {
        self.1
    }
}

impl<'a> Key<&'a [u8]> {
    // pub fn to_key_vec(self) -> KeyVec {
    //     Key(self.0.to_vec(), self.1)
    // }

    pub fn from_slice(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice, DEFAULT_VERSION)
    }
}

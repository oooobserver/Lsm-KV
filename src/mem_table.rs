use std::{
    path::Path,
    sync::{atomic::AtomicUsize, Arc},
};

use crossbeam_skiplist::SkipMap;

use anyhow::Result;

use crate::{
    byte::Bytes,
    key::{KeyBytes, KeySlice},
    wal::Wal,
};

pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn new_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::new(path.as_ref())?),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key_bytes = KeyBytes::new(
            // SAFETY:
            // Here transfer the lifetime of &[u8] to 'static
            // This is safe because this reference only used while getting value
            // and drop after used. And because it receive the shared reference,
            // can't use exclusive reference during it lifetime.
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key.key_ref()) }),
            key.version(),
        );

        self.map.get(&key_bytes).map(|e| e.value().clone())
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut data_size = 0;
        for (key, value) in data {
            data_size += key.raw_len() + value.len();
            self.map.insert(key.to_key_bytes(), Bytes::from(*value));
        }
        self.approximate_size
            .fetch_add(data_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref _wal) = self.wal {
            // TODO: add wal support.
            // wal.put_batch(data)?;
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::key::Key;

    use super::*;

    #[test]
    fn test_memtable_read_write() {
        let memtable = MemTable::new(0);
        let keys = vec![
            Key::from_slice(b"key1", 0),
            Key::from_slice(b"key2", 0),
            Key::from_slice(b"key3", 0),
        ];
        let values = [b"value1", b"value2", b"value3"];

        for (i, key) in keys.clone().into_iter().enumerate() {
            memtable.put(key, values[i]).unwrap();
        }

        for (i, key) in keys.into_iter().enumerate() {
            assert_eq!(&memtable.get(key).unwrap().as_ref(), values[i]);
        }
    }

    #[test]
    fn test_memtable_overwrite() {
        let memtable = MemTable::new(0);
        let keys = vec![
            Key::from_slice(b"key1", 0),
            Key::from_slice(b"key2", 0),
            Key::from_slice(b"key3", 0),
        ];
        let values = [b"value1", b"value2", b"value3"];

        for key in keys.clone().into_iter() {
            memtable.put(key, b"over write").unwrap();
        }

        for (i, key) in keys.clone().into_iter().enumerate() {
            memtable.put(key, values[i]).unwrap();
        }

        for (i, key) in keys.into_iter().enumerate() {
            assert_eq!(&memtable.get(key).unwrap().as_ref(), values[i]);
        }
    }
}

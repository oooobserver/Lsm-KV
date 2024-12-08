use std::sync::{atomic::AtomicUsize, Arc};

use crossbeam_skiplist::SkipMap;

use crate::{byte::Bytes, key::KeyBytes, wal::Wal};

pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

use std::{fs::File, os::unix::fs::FileExt, path::Path};

use crate::{block::BlockMeta, key::KeyBytes};

use anyhow::Result;

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    // block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    // pub(crate) bloom: Option<Bloom>,
    max_ts: u64,
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn new(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }

    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;

        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }
}

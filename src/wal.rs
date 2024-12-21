use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    // pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
    //     let path = path.as_ref();
    //     let mut file = OpenOptions::new()
    //         .read(true)
    //         .append(true)
    //         .open(path)
    //         .context("failed to recover from WAL")?;
    //     let mut buf = Vec::new();
    //     file.read_to_end(&mut buf)?;
    //     let mut rbuf: &[u8] = buf.as_slice();
    //     while rbuf.has_remaining() {
    //         let batch_size = rbuf.get_u32() as usize;
    //         if rbuf.remaining() < batch_size {
    //             bail!("incomplete WAL");
    //         }
    //         let mut batch_buf = &rbuf[..batch_size];
    //         let mut kv_pairs = Vec::new();
    //         let mut hasher = crc32fast::Hasher::new();
    //         // The checksum computed from the individual components should be the same as a direct checksum on the buffer.
    //         // Students' implementation only needs to do a single checksum on the buffer. We compute both for verification purpose.
    //         let single_checksum = crc32fast::hash(batch_buf);
    //         while batch_buf.has_remaining() {
    //             let key_len = batch_buf.get_u16() as usize;
    //             hasher.write(&(key_len as u16).to_be_bytes());
    //             let key = Bytes::copy_from_slice(&batch_buf[..key_len]);
    //             hasher.write(&key);
    //             batch_buf.advance(key_len);
    //             let ts = batch_buf.get_u64();
    //             hasher.write(&ts.to_be_bytes());
    //             let value_len = batch_buf.get_u16() as usize;
    //             hasher.write(&(value_len as u16).to_be_bytes());
    //             let value = Bytes::copy_from_slice(&batch_buf[..value_len]);
    //             hasher.write(&value);
    //             kv_pairs.push((key, ts, value));
    //             batch_buf.advance(value_len);
    //         }
    //         rbuf.advance(batch_size);
    //         let expected_checksum = rbuf.get_u32();
    //         let component_checksum = hasher.finalize();
    //         assert_eq!(component_checksum, single_checksum);
    //         if single_checksum != expected_checksum {
    //             bail!("checksum mismatch");
    //         }
    //         for (key, ts, value) in kv_pairs {
    //             skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
    //         }
    //     }
    //     Ok(Self {
    //         file: Arc::new(Mutex::new(BufWriter::new(file))),
    //     })
    // }

    // pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
    //     self.put_batch(&[(key, value)])
    // }

    // pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
    //     let mut file = self.file.lock();
    //     let mut buf = Vec::<u8>::new();
    //     for (key, value) in data {
    //         buf.put_u16(key.key_len() as u16);
    //         buf.put_slice(key.key_ref());
    //         buf.put_u64(key.ts());
    //         buf.put_u16(value.len() as u16);
    //         buf.put_slice(value);
    //     }
    //     // write batch_size header (u32)
    //     file.write_all(&(buf.len() as u32).to_be_bytes())?;
    //     // write key-value pairs body
    //     file.write_all(&buf)?;
    //     // write checksum (u32)
    //     file.write_all(&crc32fast::hash(&buf).to_be_bytes())?;
    //     Ok(())
    // }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.get_ref().flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}

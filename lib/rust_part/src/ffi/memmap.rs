use crate::prelude::*;

struct MemMapFileStoreFFI(MemMapStore);

fn blob_store_connect(path: &cxx::CxxString) -> crate::error::Result<Box<MemMapFileStoreFFI>> {
    MemMapStore::connect(path.to_str().unwrap())
        .map(MemMapFileStoreFFI)
        .map(Box::new)
}

impl MemMapFileStoreFFI {
    fn contains(&self, key: u64) -> crate::error::Result<bool> {
        self.0.contains(key.as_key())
    }

    fn blob_size(&self, key: u64) -> crate::error::Result<usize> {
        self.0.meta(key.as_key()).map(|meta| meta.size)
    }

    fn create(&self, key: u64, value: &[u8]) -> crate::error::Result<()> {
        self.0.put(key.as_key(), value, PutOpt::Create)
    }

    fn put(&self, key: u64, value: &[u8], offset: usize) -> crate::error::Result<()> {
        self.0.put(
            key.as_key(),
            value,
            PutOpt::Replace(offset..offset + value.len()),
        )
    }

    fn get_all(&self, key: u64, buf: &mut [u8]) -> crate::error::Result<()> {
        self.0.get(key.as_key(), buf, GetOpt::All)
    }

    fn get_offset(&self, key: u64, buf: &mut [u8], offset: usize) -> crate::error::Result<()> {
        self.0
            .get(key.as_key(), buf, GetOpt::Range(offset..offset + buf.len()))
    }

    fn delete(&self, key: u64) -> crate::error::Result<()> {
        self.0.delete(key.as_key(), DeleteOpt::Discard).map(|_| ())
    }
}

#[cxx::bridge(namespace = "blob_store::memmap")]
mod ffi {
    extern "Rust" {
        #[cxx_name = "blob_store_t"]
        type MemMapFileStoreFFI;
        fn blob_store_connect(path: &CxxString) -> Result<Box<MemMapFileStoreFFI>>;
        fn contains(&self, key: u64) -> Result<bool>;
        fn blob_size(&self, key: u64) -> Result<usize>;
        fn create(&self, key: u64, value: &[u8]) -> Result<()>;
        fn put(&self, key: u64, value: &[u8], offset: usize) -> Result<()>;
        fn get_all(&self, key: u64, buf: &mut [u8]) -> Result<()>;
        fn get_offset(&self, key: u64, buf: &mut [u8], offset: usize) -> Result<()>;
        #[cxx_name = "remove"]
        fn delete(&self, key: u64) -> Result<()>;
    }
}

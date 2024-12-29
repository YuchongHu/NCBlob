pub mod error;
mod ffi;
mod store_impl;

pub mod prelude {
    pub use super::error::Error as BlobStoreError;
    pub use super::error::Result as BlobStoreResult;
    pub use super::store_impl::prelude::*;
    pub use super::*;
}

#[derive(Debug, Clone)]
pub struct BlobMeta {
    pub size: usize,
}

pub type BlobRange = std::ops::Range<usize>;
pub type Offset = usize;

pub type Key = [u8; 8];

pub trait KeyLike {
    fn as_key(&self) -> Key;
}

impl KeyLike for u64 {
    fn as_key(&self) -> Key {
        self.to_le_bytes()
    }
}

#[derive(Debug, Clone)]
pub enum PutOpt {
    /// Create the blob if it doesn't exist, fail if it does.
    Create,
    /// Replace the blob content if it exists, create it if it doesn't.
    ReplaceOrCreate,
    /// Replace the blob content if it exists, fail if it doesn't.
    Replace(BlobRange),
}

#[derive(Debug, Clone)]
pub enum GetOpt {
    /// Get all the content of the blob.
    All,
    /// Get a range of the blob.
    Range(BlobRange),
}

#[derive(Debug, Clone)]
pub enum DeleteOpt {
    /// Delete the blob and return its content.
    Interest(BlobRange),
    /// Delete the blob and discard its content.
    Discard,
}

pub trait BlobStore {
    fn contains(&self, key: Key) -> error::Result<bool>;
    /// # Error
    /// - Blob(BlobError::NotFound): the blob doesn't exist.
    fn meta(&self, key: Key) -> error::Result<BlobMeta>;
    /// # Error
    /// - Blob(BlobError::AlreadyExists): the blob already exists and PutOpt::Create is used.
    /// - Blob(BlobError::NotFound): the blob doesn't exist and PutOpt::Replace is used.
    /// - Blob(BlobError::RangeError): the range is out of bounds when PutOpt::Replace is used.
    /// - Blob(BlobError::RangeError): the length of the buf doesn't match the PutOpt::Replace range.
    fn put(&self, key: Key, value: &[u8], opt: PutOpt) -> error::Result<()>;
    /// # Error
    /// - Blob(BlobError::NotFound): the blob doesn't exist.
    /// - Blob(BlobError::RangeError): the range is out of bounds
    /// - Blob(BlobError::RangeError): the length of the buf doesn't match the GetOpt::Range
    fn get(&self, key: Key, buf: &mut [u8], opt: GetOpt) -> error::Result<()>;
    fn get_owned(&self, key: Key, opt: GetOpt) -> error::Result<Vec<u8>> {
        let len = match &opt {
            GetOpt::All => self.meta(key)?.size,
            GetOpt::Range(range) => range.end - range.start,
        };
        let mut buf = vec![0_u8; len];
        self.get(key, &mut buf, opt).map(|_| buf)
    }
    /// # Error
    /// - Blob(BlobError::NotFound): the blob doesn't exist.
    fn delete(&self, key: Key, opt: DeleteOpt) -> error::Result<Option<Vec<u8>>>;
}

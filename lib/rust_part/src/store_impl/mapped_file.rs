use std::{cell::RefCell, num::NonZeroUsize};

use anyhow::anyhow;

use crate::{
    error::{Error, Result},
    BlobStore, Key,
};

type MappedFile = memmap2::MmapMut;

pub struct MemMapStore {
    root: std::path::PathBuf,
    cache: RefCell<lru::LruCache<Key, MappedFile>>,
}

impl MemMapStore {
    const DEFAULT_CACHE_SIZE: usize = 64;
    pub fn connect(root: impl Into<std::path::PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        Ok(Self {
            root,
            cache: RefCell::new(lru::LruCache::new(
                NonZeroUsize::new(Self::DEFAULT_CACHE_SIZE).unwrap(),
            )),
        })
    }

    pub fn connect_with_cache_size(
        root: impl Into<std::path::PathBuf>,
        cache_size: usize,
    ) -> Result<Self> {
        let root = root.into();
        if !root.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        let cache_size =
            NonZeroUsize::new(cache_size).ok_or(Error::Other(anyhow!("invalid cache size")))?;
        Ok(Self {
            root,
            cache: RefCell::new(lru::LruCache::new(cache_size)),
        })
    }

    fn key_to_path(&self, key: &Key) -> std::path::PathBuf {
        use itertools::Itertools;
        let mut path = self.root.clone();
        let key_hex = hex::encode(key);
        const DELIM: usize = 2;
        key_hex
            .chars()
            .chunks(key_hex.len() / DELIM)
            .into_iter()
            .for_each(|chunk| {
                let chunk: String = chunk.collect();
                path.push(&chunk);
            });
        path
    }
}

impl BlobStore for MemMapStore {
    fn contains(&self, key: Key) -> Result<bool> {
        self.key_to_path(&key).try_exists().map_err(Error::from)
    }

    fn meta(&self, key: Key) -> Result<crate::BlobMeta> {
        let path = self.key_to_path(&key);
        let size = path
            .metadata()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::from(crate::error::BlobError::NotFound)
                } else {
                    Error::from(e)
                }
            })?
            .len()
            .try_into()
            .unwrap();
        Ok(crate::BlobMeta { size })
    }

    fn put(&self, key: Key, value: &[u8], opt: crate::PutOpt) -> Result<()> {
        let mut cache = self.cache.borrow_mut();
        match opt {
            crate::PutOpt::Create => {
                // create a new file
                let path = self.key_to_path(&key);
                std::fs::create_dir_all(path.parent().unwrap())?;
                let file = std::fs::File::options()
                    .create_new(true)
                    .write(true)
                    .read(true)
                    .open(path)
                    .map_err(|e| {
                        if e.kind() == std::io::ErrorKind::AlreadyExists {
                            Error::from(crate::error::BlobError::AlreadyExists)
                        } else {
                            Error::from(e)
                        }
                    })?;
                file.set_len(value.len().try_into().unwrap())?;
                let mut page = unsafe { memmap2::MmapOptions::default().map_mut(&file) }?;
                page.copy_from_slice(value);
                cache.put(key, page);
                return Ok(());
            }
            crate::PutOpt::Replace(range) => {
                if let Some(page) = cache.get_mut(&key) {
                    if range.end > page.len() {
                        return Err(Error::from(crate::error::BlobError::RangeError));
                    }
                    if value.len() != range.len() {
                        return Err(Error::from(crate::error::BlobError::RangeError));
                    }
                    page[range].copy_from_slice(value);
                    return Ok(());
                }
                // fall through to cache miss
                let file = std::fs::File::options()
                    .write(true)
                    .read(true)
                    .open(self.key_to_path(&key))
                    .map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            Error::from(crate::error::BlobError::NotFound)
                        } else {
                            Error::from(e)
                        }
                    })?;
                let mut page = unsafe {
                    memmap2::MmapOptions::default()
                        .map_mut(&file)
                        .map_err(Error::from)?
                };
                if !crate::store_impl::helpers::range_contains(&(0..page.len()), &range) {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                if value.len() != range.len() {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                page[range].copy_from_slice(value);
                cache.put(key, page);
                return Ok(());
            }
            crate::PutOpt::ReplaceOrCreate => unimplemented!(),
        }
    }

    fn get(&self, key: Key, buf: &mut [u8], opt: crate::GetOpt) -> Result<()> {
        let mut cache = self.cache.borrow_mut();
        let page = if cache.contains(&key) {
            cache.get_mut(&key).unwrap()
        } else {
            let file = std::fs::File::options()
                .read(true)
                .write(true)
                .open(self.key_to_path(&key))
                .map_err(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        Error::from(crate::error::BlobError::NotFound)
                    } else {
                        Error::from(e)
                    }
                })?;
            let page = unsafe {
                memmap2::MmapOptions::default()
                    .map_mut(&file)
                    .map_err(Error::from)?
            };
            cache.put(key, page);
            cache.get_mut(&key).unwrap()
        };
        match opt {
            crate::GetOpt::All => {
                if buf.len() != page.len() {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                buf.copy_from_slice(page);
            }
            crate::GetOpt::Range(range) => {
                if !crate::store_impl::helpers::range_contains(&(0..page.len()), &range) {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                if buf.len() != range.len() {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                buf.copy_from_slice(&page[range]);
            }
        }
        return Ok(());
    }

    fn delete(&self, key: Key, opt: crate::DeleteOpt) -> Result<Option<Vec<u8>>> {
        match opt {
            crate::DeleteOpt::Interest(_) => unimplemented!(),
            crate::DeleteOpt::Discard => {
                self.cache.borrow_mut().pop(&key);
                let path = self.key_to_path(&key);
                std::fs::remove_file(path).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        Error::from(crate::error::BlobError::NotFound)
                    } else {
                        Error::from(e)
                    }
                })?;
                return Ok(None);
            }
        }
    }
}

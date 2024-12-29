use std::{
    io::prelude::{Read, Seek, Write},
    path::PathBuf,
};

use crate::{
    error::{Error, Result},
    BlobStore, DeleteOpt, GetOpt, Key, PutOpt,
};

pub struct LocalFileSystemBlobStore {
    root: PathBuf,
}

impl LocalFileSystemBlobStore {
    pub fn connect(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "dev path not found",
            )));
        }
        Ok(Self { root })
    }

    fn key_to_path(&self, key: &Key) -> PathBuf {
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

impl BlobStore for LocalFileSystemBlobStore {
    fn contains(&self, key: Key) -> Result<bool> {
        let path = self.key_to_path(&key);
        path.try_exists().map_err(Error::from)
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

    fn put(&self, key: Key, value: &[u8], opt: PutOpt) -> Result<()> {
        let path = self.key_to_path(&key);
        let mut open_opt = std::fs::OpenOptions::new();
        open_opt.read(true).write(true);
        match opt {
            PutOpt::Create => {
                std::fs::create_dir_all(path.parent().unwrap())?;
                open_opt.create_new(true)
            }
            PutOpt::Replace(_) => open_opt.create(false),
            PutOpt::ReplaceOrCreate => {
                std::fs::create_dir_all(path.parent().unwrap())?;
                open_opt.create(true)
            }
        };
        let mut file = open_opt.open(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => {
                Error::from(crate::error::BlobError::AlreadyExists)
            }
            std::io::ErrorKind::NotFound => Error::from(crate::error::BlobError::NotFound),
            _ => Error::from(e),
        })?;
        match opt {
            PutOpt::Create => file.set_len(value.len().try_into().unwrap())?,
            PutOpt::Replace(range) => {
                // check range validity
                let valid_range = 0..usize::try_from(file.metadata()?.len()).unwrap();
                if !crate::store_impl::helpers::range_contains(&valid_range, &range) {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                if range.len() != value.len() {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                file.seek(std::io::SeekFrom::Start(range.start.try_into().unwrap()))?;
            }
            PutOpt::ReplaceOrCreate => {
                file.set_len(value.len().try_into().unwrap())?;
                file.seek(std::io::SeekFrom::Start(0))?;
            }
        }
        file.write_all(value).map_err(Error::from)
    }

    fn get(&self, key: Key, buf: &mut [u8], opt: GetOpt) -> Result<()> {
        let path = self.key_to_path(&key);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::from(crate::error::BlobError::NotFound)
                } else {
                    Error::from(e)
                }
            })?;
        match opt {
            GetOpt::All => {
                let file_size: usize = file.metadata()?.len().try_into().unwrap();
                if file_size != buf.len() {
                    return Err(Error::from(crate::error::BlobError::RangeError));
                }
                file.seek(std::io::SeekFrom::Start(0))?;
            }
            GetOpt::Range(range) => {
                let file_size: usize = file.metadata()?.len().try_into().unwrap();
                let valid_range = 0..file_size;
                if !crate::store_impl::helpers::range_contains(&valid_range, &range) {
                    return Err(Error::Blob(crate::error::BlobError::RangeError));
                }
                let len = range.end - range.start;
                if len != buf.len() {
                    return Err(Error::Blob(crate::error::BlobError::RangeError));
                }
                file.seek(std::io::SeekFrom::Start(range.start.try_into().unwrap()))?;
            }
        }
        file.read_exact(buf).map_err(Error::from)
    }

    fn delete(&self, key: Key, opt: DeleteOpt) -> Result<Option<Vec<u8>>> {
        let path = self.key_to_path(&key);
        if let DeleteOpt::Interest(_) = &opt {
            unimplemented!("Interest delete not implemented, use \"get\" before delete instead")
        }
        std::fs::remove_file(path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Error::from(crate::error::BlobError::NotFound)
                } else {
                    Error::from(e)
                }
            })
            .map(|_| None)
    }
}

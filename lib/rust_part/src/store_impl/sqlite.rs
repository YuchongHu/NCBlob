use std::{
    cell::RefCell,
    collections::HashMap,
    io::prelude::{Seek, Write},
    path::{self, PathBuf},
};

use rusqlite::blob::{Blob, ZeroBlob};

use crate::{
    error::{Error, Result},
    BlobStore, Key,
};

type RowID = i64;
type Map<K, V> = HashMap<K, V>;
type KeyToRowIDMap = RefCell<Map<Key, RowID>>;

pub struct SqliteBlobStore {
    root: path::PathBuf,
    conn: rusqlite::Connection,
    key_to_row_map: KeyToRowIDMap,
}

impl SqliteBlobStore {
    const DATABASE_NAME: rusqlite::DatabaseName<'static> = rusqlite::MAIN_DB;
    const TABLE_NAME: &'static str = "blobs";
    const COLUMN_NAME: &'static str = "content";
    const SQL_INSERT: &'static str = concat!("INSERT INTO blobs (content) VALUES (?)",);
    const SQL_DELETE: &'static str = concat!("DELETE FROM blobs WHERE rowid = (?)",);
    const SQL_CREATE_TABLE: &'static str =
        concat!("CREATE TABLE IF NOT EXISTS blobs ( content BLOB NOT NULL )",);
    const DB_FILE: &'static str = "blobs.db";
    const MAP_FILE: &'static str = "blobs.map.dump";

    pub fn connect(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let db_path = {
            let mut path = path.clone();
            path.push(Self::DB_FILE);
            path
        };
        let map_path = {
            let mut path = path.clone();

            path.push(Self::MAP_FILE);
            path
        };
        let conn = rusqlite::Connection::open(db_path.as_path())?;
        conn.execute(Self::SQL_CREATE_TABLE, [])?;
        let map = if map_path.exists() {
            bincode::deserialize_from(std::fs::File::open(map_path)?)
                .map_err(|e| anyhow::Error::new(e))?
        } else {
            KeyToRowIDMap::default()
        };
        Ok(Self {
            conn,
            key_to_row_map: map,
            root: path.into(),
        })
    }

    fn open_blob(&self, key: &Key, read_only: bool) -> Result<Blob> {
        self.key_to_row_map
            .borrow()
            .get(key)
            .copied()
            .map(|row_id| {
                self.conn
                    .blob_open(
                        Self::DATABASE_NAME,
                        Self::TABLE_NAME,
                        Self::COLUMN_NAME,
                        row_id,
                        read_only,
                    )
                    .map_err(Error::from)
            })
            .ok_or_else(|| crate::error::BlobError::NotFound)
            .map_err(Error::from)
            .and_then(std::convert::identity)
    }
}

impl BlobStore for SqliteBlobStore {
    fn contains(&self, key: Key) -> crate::error::Result<bool> {
        Ok(self.key_to_row_map.borrow().contains_key(&key))
    }

    fn meta(&self, key: Key) -> crate::error::Result<crate::BlobMeta> {
        let size = self.open_blob(&key, true)?.len();
        Ok(crate::BlobMeta { size })
    }

    fn put(&self, key: Key, value: &[u8], opt: crate::PutOpt) -> crate::error::Result<()> {
        let mut blob = match &opt {
            crate::PutOpt::Create => {
                let mut key_to_row_map = self.key_to_row_map.borrow_mut();
                if key_to_row_map.contains_key(&key) {
                    return Err(crate::error::BlobError::AlreadyExists.into());
                }
                self.conn.execute(
                    Self::SQL_INSERT,
                    [ZeroBlob(value.len().try_into().unwrap())],
                )?;
                let row_id = self.conn.last_insert_rowid();
                key_to_row_map.insert(key, row_id);
                drop(key_to_row_map);
                self.open_blob(&key, false)?
            }
            crate::PutOpt::Replace(range) => {
                let mut blob = self.open_blob(&key, false)?;
                // check range
                let size = blob.len();
                let valid_range = 0..size;
                if !crate::store_impl::helpers::range_contains(&valid_range, &range) {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                if value.len() != range.len() {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                blob.seek(std::io::SeekFrom::Start((range.start).try_into().unwrap()))?;
                blob
            }
            crate::PutOpt::ReplaceOrCreate => unimplemented!(),
        };
        blob.write_all(value)?;
        Ok(())
    }

    fn get(&self, key: Key, buf: &mut [u8], opt: crate::GetOpt) -> crate::error::Result<()> {
        let key = key;
        let mut blob = self.open_blob(&key, true)?;
        match &opt {
            crate::GetOpt::All => {
                if blob.len() != buf.len() {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                blob.read_at_exact(buf, 0)?;
            }
            crate::GetOpt::Range(range) => {
                let len = range.end - range.start;
                if len != buf.len() {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                let valid_range = 0..blob.len();
                if !crate::store_impl::helpers::range_contains(&valid_range, &range) {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                blob.seek(std::io::SeekFrom::Start(range.start.try_into().unwrap()))?;
                blob.read_at_exact(buf, range.start)?;
            }
        }
        Ok(())
    }

    fn delete(&self, key: Key, opt: crate::DeleteOpt) -> crate::error::Result<Option<Vec<u8>>> {
        let key = key;
        let row_id = match self.key_to_row_map.borrow_mut().remove(&key) {
            Some(row_id) => row_id,
            None => return Err(crate::error::BlobError::NotFound.into()),
        };
        if let crate::DeleteOpt::Interest(_) = &opt {
            unimplemented!("Interest delete not implemented, use \"get\" before delete instead");
            // let mut blob = self.open_blob(&key, false)?;
            // let size = blob.len();
            // interest.reserve_exact(size - interest.len());
            // blob.raw_read_at_exact()?;
        }
        self.conn.execute(Self::SQL_DELETE, [row_id])?;
        Ok(None)
    }
}

impl Drop for SqliteBlobStore {
    fn drop(&mut self) {
        let map_path = {
            let mut path = self.root.clone();
            path.push(Self::MAP_FILE);
            path
        };

        bincode::serialize_into(
            std::fs::File::options()
                .truncate(true)
                .read(true)
                .write(true)
                .create(true)
                .open(map_path)
                .expect("failed to open map file"),
            &self.key_to_row_map,
        )
        .unwrap();
    }
}

mod common;

use tbr_rs::prelude::*;

#[test]
fn test_local_fs() {
    // write read
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystemBlobStore::connect(tmp_dir.path()).unwrap();
    common::test_write_read(&store);
    // dump
    let tmp_dir = tempfile::tempdir().unwrap();
    common::test_dump(|| {
        LocalFileSystemBlobStore::connect(tmp_dir.path())
            .map(|obj| -> Box<dyn BlobStore> { Box::new(obj) })
            .map_err(Into::into)
    });
    // concurrecy
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(LocalFileSystemBlobStore::connect(tmp_dir.path()).unwrap());
    common::test_concurrent(store);
}

#[test]
fn test_memory_cache() {
    const CAP: usize = 1 << 20;
    // write read
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystemBlobStore::connect(tmp_dir.path()).unwrap();
    let store = MemoryCache::with_capacity(store, std::num::NonZeroUsize::new(CAP).unwrap());
    common::test_write_read(&store);
    // dump
    let tmp_dir = tempfile::tempdir().unwrap();
    common::test_dump(|| {
        Ok(MemoryCache::with_capacity(
            LocalFileSystemBlobStore::connect(tmp_dir.path()).unwrap(),
            std::num::NonZeroUsize::new(CAP).unwrap(),
        ))
        .map(|obj| -> Box<dyn BlobStore> { Box::new(obj) })
    });
    // concurrency
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(MemoryCache::with_capacity(
        LocalFileSystemBlobStore::connect(tmp_dir.path()).unwrap(),
        std::num::NonZeroUsize::new(CAP).unwrap(),
    ));
    common::test_concurrent(store);
}

#[test]
#[cfg(feature = "sqlite")]
fn test_sqlite() {
    // write read
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = SqliteBlobStore::connect(tmp_dir.path()).unwrap();
    common::test_write_read(&store);
    // dump
    let tmp_dir = tempfile::tempdir().unwrap();
    common::test_dump(|| {
        SqliteBlobStore::connect(tmp_dir.path())
            .map(|obj| -> Box<dyn BlobStore> { Box::new(obj) })
            .map_err(Into::into)
    });
}

#[test]
#[cfg(feature = "memmap")]
fn test_mapped_file() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let store = MemMapStore::connect(tmp_dir.path()).unwrap();
    common::test_write_read(&store);
}

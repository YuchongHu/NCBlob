use std::sync::Arc;

use error::BlobError;
use rand::prelude::Rng;
use tbr_rs::error::Result as BlobResult;
use tbr_rs::prelude::*;

use crate::common::gen_random;

const LOAD: usize = 4096;
const BLOB_SIZE_RANGE: std::ops::Range<usize> = 1..4096;

fn put_blobs(blob_store: &dyn BlobStore) -> Vec<(Key, Vec<u8>)> {
    let mut rng = rand::thread_rng();
    (0..LOAD)
        .map(|_| gen_random(rng.gen_range(BLOB_SIZE_RANGE.clone())))
        .inspect(|(key, data)| blob_store.put(*key, &data, PutOpt::Create).unwrap())
        .collect::<Vec<_>>()
}

fn check_match(blob_store: &dyn BlobStore, expect: &[(Key, Vec<u8>)]) {
    expect.iter().for_each(|(key, expect)| {
        assert!(blob_store.contains(*key).unwrap());
        assert_eq!(blob_store.meta(*key).unwrap().size, expect.len());
        let received = blob_store.get_owned(*key, GetOpt::All).unwrap();
        assert_eq!(expect, &received);
    });
}

fn check_get(blob_store: &dyn BlobStore, expect: &[(Key, Vec<u8>)]) {
    let mut rng = rand::thread_rng();
    expect.iter().for_each(|(key, expect)| {
        // read all
        let mut received = vec![0; expect.len()];
        blob_store.get(*key, &mut received, GetOpt::All).unwrap();
        let received_owned = blob_store.get_owned(*key, GetOpt::All).unwrap();
        assert_eq!(expect, &received);
        assert_eq!(expect, &received_owned);
        // read in range
        let range_start = rng.gen_range(0..expect.len());
        let range_end = rng.gen_range(range_start..expect.len());
        let range = range_start..range_end;
        let mut range_received = vec![0; range.len()];
        blob_store
            .get(*key, &mut range_received, GetOpt::Range(range.clone()))
            .unwrap();
        let range_received_owned = blob_store
            .get_owned(*key, GetOpt::Range(range.clone()))
            .unwrap();
        assert_eq!(range_received, &expect[range.clone()]);
        assert_eq!(range_received_owned, expect[range.clone()]);
    });
}

fn put_exists(blob_store: &dyn BlobStore, expect: &[(Key, Vec<u8>)]) {
    expect.iter().for_each(|(key, _)| {
        assert!(matches!(
            blob_store.put(*key, &[0], PutOpt::Create),
            Err(BlobStoreError::Blob(BlobError::AlreadyExists))
        ));
    });
}

fn get_not_exist(blob_store: &dyn BlobStore) {
    let mut rng = rand::thread_rng();
    let range = 0..4096;
    let mut buf = vec![0; range.len()];
    (0..LOAD).for_each(|_| {
        let key: Key = rng.gen();
        assert!(blob_store.contains(key).unwrap() == false);
        assert!(matches!(
            blob_store.meta(key),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
        assert!(matches!(
            blob_store.get_owned(key, GetOpt::All),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
        assert!(matches!(
            blob_store.get(key, &mut buf, GetOpt::All),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
        assert!(matches!(
            blob_store.get_owned(key, GetOpt::Range(range.clone())),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
        assert!(matches!(
            blob_store.get(key, &mut buf, GetOpt::Range(range.clone())),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
    })
}

fn put_replace_not_exist(blob_store: &dyn BlobStore) {
    let mut rng = rand::thread_rng();
    let range = 0..4096;
    let data = vec![0; range.len()];
    (0..LOAD).for_each(|_| {
        let key: Key = rng.gen();
        assert!(matches!(
            blob_store.put(key, &data, PutOpt::Replace(range.clone())),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
    })
}

fn put_or_create(blob_store: &dyn BlobStore) {
    let mut rng = rand::thread_rng();
    let range = 0..4096;
    let data = vec![0; range.len()];
    (0..LOAD).for_each(|_| {
        let key: Key = rng.gen();
        let is_replace = rng.gen_bool(0.5);
        if is_replace {
            assert!(blob_store
                .put(key, &data[..4096 / 2], PutOpt::Create)
                .is_ok());
        }
        assert!(blob_store.put(key, &data, PutOpt::ReplaceOrCreate).is_ok());
        assert!(blob_store.get_owned(key, GetOpt::All).unwrap() == data);
    })
}

fn delete_not_exist(blob_store: &dyn BlobStore) {
    let mut rng = rand::thread_rng();
    (0..LOAD).for_each(|_| {
        let key: Key = rng.gen();
        // assert!(matches!(
        //     blob_store.delete(key, DeleteOpt::Interest(0..4096)),
        //     Err(BlobStoreError::BlobError(BlobError::NotFound))
        // ));
        assert!(matches!(
            blob_store.delete(key, DeleteOpt::Discard),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
    })
}

fn check_range(blob_store: &dyn BlobStore, expect: &[(Key, Vec<u8>)]) {
    expect.iter().for_each(|(key, expect)| {
        let valide_range = 0..expect.len();
        // buf not match range
        let mut buf_too_small = vec![0; expect.len() / 2];
        let mut buf_too_large = vec![0; expect.len() * 2];
        vec![&mut buf_too_small, &mut buf_too_large]
            .into_iter()
            .for_each(|buf| {
                assert!(matches!(
                    blob_store.put(*key, &buf, PutOpt::Replace(valide_range.clone())),
                    Err(BlobStoreError::Blob(BlobError::RangeError))
                ));
                assert!(matches!(
                    blob_store.get(*key, buf.as_mut(), GetOpt::Range(valide_range.clone())),
                    Err(BlobStoreError::Blob(BlobError::RangeError))
                ));
                assert!(matches!(
                    blob_store.get(*key, buf, GetOpt::All),
                    Err(BlobStoreError::Blob(BlobError::RangeError))
                ),);
            });

        // out of bound
        let out_of_bound = [
            valide_range.end + 1..valide_range.end + 1 + 128,
            valide_range.start..valide_range.end + 8,
        ];
        out_of_bound.into_iter().for_each(|range| {
            let mut buf = vec![0; range.len()];
            assert!(matches!(
                blob_store.put(*key, &buf, PutOpt::Replace(range.clone())),
                Err(BlobStoreError::Blob(BlobError::RangeError))
            ));
            assert!(matches!(
                blob_store.get(*key, &mut buf, GetOpt::Range(range.clone())),
                Err(BlobStoreError::Blob(BlobError::RangeError))
            ));
            assert!(matches!(
                blob_store.get_owned(*key, GetOpt::Range(range.clone())),
                Err(BlobStoreError::Blob(BlobError::RangeError))
            ));
        });
    });
}

fn check_delete(blob_store: &dyn BlobStore, expect: &[(Key, Vec<u8>)]) {
    expect.iter().for_each(|(key, expect)| {
        let valide_range = 0..expect.len();
        let mut buf = vec![0; expect.len()];
        // delete interest
        // let mut rng = rand::thread_rng();
        // let range_start = rng.gen_range(0..expect.len());
        // let range_end = rng.gen_range(range_start..expect.len());
        // let range = range_start..range_end;
        // let deleted = blob_store
        //     .delete(*key, DeleteOpt::Interest(range.clone()))
        //     .unwrap();
        // assert_eq!(deleted, Some(expect[range.clone()]));
        // assert!(matches!(
        //     blob_store.get(*key, &mut buf, GetOpt::Range(valide_range.clone())),
        //     Err(BlobStoreError::BlobError(BlobError::NotFound))
        // ));
        // delete discard
        let deleted = blob_store.delete(*key, DeleteOpt::Discard).unwrap();
        assert_eq!(deleted, None);
        assert!(matches!(
            blob_store.get(*key, &mut buf, GetOpt::Range(valide_range.clone())),
            Err(BlobStoreError::Blob(BlobError::NotFound))
        ));
    });
}

/// expected to receive a clean store
pub fn test_write_read(blob_store: &dyn BlobStore) {
    get_not_exist(blob_store);
    put_replace_not_exist(blob_store);
    delete_not_exist(blob_store);
    let expect = put_blobs(blob_store);
    check_match(blob_store, &expect);
    check_get(blob_store, &expect);
    check_range(blob_store, &expect);
    put_exists(blob_store, &expect);
    check_delete(blob_store, &expect);
    put_or_create(blob_store);
}

pub fn test_dump<F>(open: F)
where
    F: Fn() -> BlobResult<Box<dyn BlobStore>>,
{
    let store = open().unwrap();
    let expect = put_blobs(&*store);
    drop(store);
    // reopen
    let store = open().unwrap();
    check_match(&*store, &expect);
}

#[allow(dead_code)]
pub fn test_concurrent(blob: Arc<dyn BlobStore + Send + Sync>) {
    let (data_producer, data_consumer) = crossbeam_channel::bounded(128);
    let (delete_producer, delete_consumer) = crossbeam_channel::bounded(128);
    const CONCURRENCY: usize = 8;

    let send_handle = (0..CONCURRENCY)
        .map(|_| {
            let blob = Arc::clone(&blob);
            let producer = data_producer.clone();
            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let load = LOAD / CONCURRENCY;
                (0..load).for_each(|_| {
                    let (key, data) = gen_random(rng.gen_range(BLOB_SIZE_RANGE.clone()));
                    blob.put(key, &data, PutOpt::Create).unwrap();
                    producer.send((key, data)).unwrap();
                });
            })
        })
        .collect::<Vec<_>>();
    drop(data_producer);
    let recv_handle = (0..CONCURRENCY)
        .map(|_| {
            let blob = Arc::clone(&blob);
            let consumer = data_consumer.clone();
            let producer = delete_producer.clone();
            std::thread::spawn(move || {
                while let Ok((key, data)) = consumer.recv() {
                    assert!(blob.contains(key).unwrap());
                    assert_eq!(blob.meta(key).unwrap().size, data.len());
                    let received = blob.get_owned(key, GetOpt::All).unwrap();
                    assert_eq!(data, received);
                    producer.send((key, data)).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    drop(delete_producer);
    let delete_handle = (0..CONCURRENCY)
        .map(|_| {
            let blob = Arc::clone(&blob);
            let consumer = delete_consumer.clone();
            std::thread::spawn(move || {
                while let Ok((key, _)) = consumer.recv() {
                    assert!(blob.delete(key, DeleteOpt::Discard).is_ok());
                }
            })
        })
        .collect::<Vec<_>>();
    send_handle
        .into_iter()
        .chain(recv_handle)
        .chain(delete_handle)
        .for_each(|h| h.join().unwrap());
}

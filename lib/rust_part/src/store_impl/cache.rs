use std::num::NonZeroUsize;

use lru::LruCache;

use crate::{store_impl::helpers, BlobStore, Key};

type Mutex<T> = parking_lot::Mutex<T>;

struct SizedCache {
    cache: LruCache<Key, usize>,
    size: usize,
    cap: usize,
}

impl SizedCache {
    fn with_capacity(cap: NonZeroUsize) -> Self {
        Self {
            cache: LruCache::unbounded(),
            size: 0,
            cap: cap.get(),
        }
    }

    pub fn contains(&mut self, key: &Key) -> bool {
        self.cache.get(key).is_some()
    }

    pub fn push(&mut self, key: Key, size: usize) -> Vec<Key> {
        let mut evict = Vec::new();
        while self.size + size > self.cap {
            let (evict_key, s) = self.cache.pop_lru().expect("cache is too small");
            self.size -= s;
            evict.push(evict_key);
        }
        self.cache.push(key, size);
        self.size += size;
        evict
    }

    pub fn pop(&mut self) -> Option<(Key, usize)> {
        self.cache.pop_lru().inspect(|(_, size)| {
            self.size -= size;
        })
    }

    pub fn remove(&mut self, key: &Key) -> bool {
        if let Some(size) = self.cache.pop(key) {
            self.size -= size;
            return true;
        }
        false
    }
}

pub struct MemoryCache<S>
where
    S: BlobStore,
{
    store: S,
    lru: Mutex<SizedCache>,
    map: dashmap::DashMap<Key, Vec<u8>>,
}

impl<S> MemoryCache<S>
where
    S: BlobStore,
{
    /// create cache with capacity in bytes
    pub fn with_capacity(store: S, capacity: std::num::NonZeroUsize) -> Self {
        let lru = Mutex::new(SizedCache::with_capacity(capacity));
        let map = dashmap::DashMap::new();
        Self { store, lru, map }
    }

    fn flush_evict(&self, evict: Vec<Key>) -> crate::error::Result<()> {
        evict.iter().try_for_each(|key| {
            let (_, data) = self.map.remove(key).unwrap();
            self.store.put(*key, &data, crate::PutOpt::ReplaceOrCreate)
        })
    }

    pub fn bypass_put(
        &self,
        key: Key,
        value: &[u8],
        opt: crate::PutOpt,
    ) -> crate::error::Result<()> {
        self.store.put(key, value, opt)
    }
    pub fn bypass_get(
        &self,
        key: Key,
        buf: &mut [u8],
        opt: crate::GetOpt,
    ) -> crate::error::Result<()> {
        self.store.get(key, buf, opt)
    }
}

impl<S> BlobStore for MemoryCache<S>
where
    S: BlobStore,
{
    fn contains(&self, key: Key) -> crate::error::Result<bool> {
        if self.lru.lock().contains(&key) {
            Ok(true)
        } else {
            self.store.contains(key)
        }
    }

    fn meta(&self, key: Key) -> crate::error::Result<crate::BlobMeta> {
        if let Some(size) = self.lru.lock().cache.get(&key) {
            Ok(crate::BlobMeta { size: *size })
        } else {
            self.store.meta(key)
        }
    }

    fn put(&self, key: Key, value: &[u8], opt: crate::PutOpt) -> crate::error::Result<()> {
        let mut lru = self.lru.lock();
        match opt {
            // replace should guarantee the key exists
            crate::PutOpt::Replace(range) => {
                if !lru.contains(&key) {
                    // fetch from store
                    let data = self.store.get_owned(key, crate::GetOpt::All)?;
                    let evict = lru.push(key, data.len());
                    self.flush_evict(evict)?;
                    self.map.insert(key, data);
                }
                let mut data = self.map.get_mut(&key).unwrap();
                drop(lru);
                if data.len() != range.len()
                    || !helpers::range_contains(&(0..data.len()), &range)
                    || range.len() != value.len()
                {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                data[range.clone()].copy_from_slice(value);
            }
            crate::PutOpt::Create => {
                if lru.contains(&key) || self.store.contains(key)? {
                    return Err(crate::error::BlobError::AlreadyExists.into());
                }
                // create new
                let evict = lru.push(key, value.len());
                self.flush_evict(evict)?;
                self.map.insert(key, value.to_vec());
                return self.store.put(key, value, opt);
            }
            crate::PutOpt::ReplaceOrCreate => {
                let evict = lru.push(key, value.len());
                self.flush_evict(evict)?;
                self.map.insert(key, value.to_vec());
                return self.store.put(key, value, opt);
            }
        }
        Ok(())
    }

    fn get(&self, key: Key, buf: &mut [u8], opt: crate::GetOpt) -> crate::error::Result<()> {
        let mut lru = self.lru.lock();
        if !lru.contains(&key) {
            // cache miss, load from store
            let data = self.store.get_owned(key, opt.clone())?;
            let evict = lru.push(key, data.len());
            self.flush_evict(evict)?;
            self.map.insert(key, data);
        }
        let data = self.map.get(&key).unwrap();
        drop(lru);
        let data = match &opt {
            crate::GetOpt::All => &data,
            crate::GetOpt::Range(range) => {
                if !helpers::range_contains(&(0..data.len()), range) {
                    return Err(crate::error::BlobError::RangeError.into());
                }
                &data[range.clone()]
            }
        };
        (data.len() == buf.len())
            .then(|| buf.copy_from_slice(data))
            .ok_or_else(|| crate::error::BlobError::RangeError.into())
    }

    fn delete(&self, key: Key, opt: crate::DeleteOpt) -> crate::error::Result<Option<Vec<u8>>> {
        if let crate::DeleteOpt::Interest(_) = opt {
            unimplemented!("delete with interest is not supported yet")
        }
        let mut lru = self.lru.lock();
        lru.remove(&key);
        self.map.remove(&key);
        self.store.delete(key, opt)
    }
}

impl<S> Drop for MemoryCache<S>
where
    S: BlobStore,
{
    fn drop(&mut self) {
        let mut lru = self.lru.lock();
        while let Some((key, _)) = lru.pop() {
            let data = self.map.remove(&key).unwrap().1;
            self.store
                .put(key, &data, crate::PutOpt::ReplaceOrCreate)
                .unwrap();
        }
    }
}

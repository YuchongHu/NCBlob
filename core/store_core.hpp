#pragma once

#include "exception.hpp"
#include "local_file_system.rs.h"
#include "memory_cache.rs.h"
#include <cstddef>
#include <filesystem>
#include <limits>
#include <span>

namespace store {
using key_t = std::size_t;
struct StoreInterface {
  StoreInterface() = default;
  virtual ~StoreInterface() = default;
  StoreInterface(const StoreInterface &) = default;
  auto operator=(const StoreInterface &) -> StoreInterface & = default;
  StoreInterface(StoreInterface &&) = default;
  auto operator=(StoreInterface &&) -> StoreInterface & = default;
  virtual auto contains(key_t key) -> bool = 0;
  virtual auto blob_size(key_t key) -> std::size_t = 0;
  virtual auto create(key_t key, std::span<const std::byte> value) -> void = 0;
  virtual auto put(key_t key, std::span<const std::byte> value,
                   std::size_t offset) -> void = 0;
  virtual auto put_or_create(key_t key,
                             std::span<const std::byte> value) -> void = 0;
  virtual auto get_all(key_t key, std::span<std::byte> value) -> void = 0;
  virtual auto get_offset(key_t key, std::span<std::byte> value,
                          std::size_t offset) -> void = 0;
  virtual auto remove(key_t key) -> void = 0;

protected:
  auto to_rust_bytes(std::span<const std::byte> bytes)
      -> ::rust::Slice<const std::uint8_t> {
    return {reinterpret_cast<const std::uint8_t *>(bytes.data()), // NOLINT
            bytes.size()};
  }
  auto
  to_rust_bytes(std::span<std::byte> bytes) -> ::rust::Slice<std::uint8_t> {
    return {reinterpret_cast<std::uint8_t *>(bytes.data()), // NOLINT
            bytes.size()};
  }
};

class LocalStore : virtual public StoreInterface {
private:
  using store_t = ::rust::Box<::blob_store::local_fs::blob_store_t>;
  store_t store_;

public:
  LocalStore() = delete;
  LocalStore(const std::filesystem::path &path)
      : store_(blob_store::local_fs::blob_store_connect(path.string())) {};
  auto contains(key_t key) -> bool override { return store_->contains(key); };
  auto blob_size(key_t key) -> std::size_t override {
    return store_->blob_size(key);
  };
  auto create(key_t key, std::span<const std::byte> value) -> void override {
    store_->create(key, to_rust_bytes(value));
  };
  auto put(key_t key, std::span<const std::byte> value,
           std::size_t offset) -> void override {
    store_->put(key, to_rust_bytes(value), offset);
  };
  auto put_or_create(key_t key,
                     std::span<const std::byte> value) -> void override {
    store_->put_or_create(key, to_rust_bytes(value));
  };
  auto get_all(key_t key, std::span<std::byte> value) -> void override {
    store_->get_all(key, to_rust_bytes(value));
  };
  auto get_offset(key_t key, std::span<std::byte> value,
                  std::size_t offset) -> void override {
    store_->get_offset(key, to_rust_bytes(value), offset);
  };
  auto remove(key_t key) -> void override { store_->remove(key); };
};

class BypassCacheStore;
class CachedLocalStore : virtual public StoreInterface {
private:
  friend class BypassCacheStore;
  using store_t = ::rust::Box<::blob_store::cached_local_fs::blob_store_t>;
  store_t store_;
  std::size_t threshold_{std::numeric_limits<std::size_t>::max()};

public:
  CachedLocalStore() = delete;
  CachedLocalStore(const std::filesystem::path &path, std::size_t capacity)
      : store_(blob_store::cached_local_fs::blob_store_connect(path.string(),
                                                               capacity)) {};
  /// always bypass the cache and directly access the local store
  auto bypass_cache() -> BypassCacheStore;
  auto set_bypass_threshold(std::size_t threshold) -> void {
    threshold_ = threshold;
  };
  auto contains(key_t key) -> bool override { return store_->contains(key); };
  auto blob_size(key_t key) -> std::size_t override {
    return store_->blob_size(key);
  };
  auto create(key_t key, std::span<const std::byte> value) -> void override {
    if (value.size() > threshold_) {
      store_->bypass_create(key, to_rust_bytes(value));
    } else {
      store_->create(key, to_rust_bytes(value));
    }
  };
  auto put(key_t key, std::span<const std::byte> value,
           std::size_t offset) -> void override {
    if (value.size() > threshold_) {
      store_->bypass_put(key, to_rust_bytes(value), offset);
    } else {
      store_->put(key, to_rust_bytes(value), offset);
    }
  };
  auto put_or_create(key_t key,
                     std::span<const std::byte> value) -> void override {
    if (value.size() > threshold_) {
      store_->bypass_put_or_create(key, to_rust_bytes(value));
    } else {
      store_->put_or_create(key, to_rust_bytes(value));
    }
  };
  auto get_all(key_t key, std::span<std::byte> value) -> void override {
    if (value.size() > threshold_) {
      store_->bypass_get_all(key, to_rust_bytes(value));
    } else {
      store_->get_all(key, to_rust_bytes(value));
    }
  };
  auto get_offset(key_t key, std::span<std::byte> value,
                  std::size_t offset) -> void override {
    if (value.size() > threshold_) {
      store_->bypass_get_offset(key, to_rust_bytes(value), offset);
    } else {
      store_->get_offset(key, to_rust_bytes(value), offset);
    }
  };
  auto remove(key_t key) -> void override {
    err::Todo("bypass remove: clean the cache");
    store_->remove(key);
  };
};

class BypassCacheStore : virtual public StoreInterface {
private:
  friend class CachedLocalStore;
  CachedLocalStore::store_t &store_; // NOLINT

  BypassCacheStore(CachedLocalStore::store_t &store) : store_(store) {};

public:
  auto contains(key_t key) -> bool override { return store_->contains(key); };
  auto blob_size(key_t key) -> std::size_t override {
    return store_->blob_size(key);
  };
  auto create(key_t key, std::span<const std::byte> value) -> void override {
    store_->bypass_create(key, to_rust_bytes(value));
  };
  auto put(key_t key, std::span<const std::byte> value,
           std::size_t offset) -> void override {
    store_->bypass_put(key, to_rust_bytes(value), offset);
  };
  auto put_or_create(key_t key,
                     std::span<const std::byte> value) -> void override {
    store_->bypass_put_or_create(key, to_rust_bytes(value));
  };
  auto get_all(key_t key, std::span<std::byte> value) -> void override {
    store_->bypass_get_all(key, to_rust_bytes(value));
  };
  auto get_offset(key_t key, std::span<std::byte> value,
                  std::size_t offset) -> void override {
    store_->bypass_get_offset(key, to_rust_bytes(value), offset);
  };
  auto remove(key_t key) -> void override { store_->remove(key); };
};

inline auto CachedLocalStore::bypass_cache() -> BypassCacheStore {
  return BypassCacheStore{this->store_};
}

} // namespace store
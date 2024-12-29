#pragma once

#include "meta.hpp"
#include "meta_exception.hpp"
#include "serde.hpp"
#include "span.hpp"

#include <algorithm>
#include <boost/compute/detail/lru_cache.hpp>
#include <fmt/format.h>
#include <iostream>
#include <iterator>
#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>

#include <map>
#include <memory>
#include <msgpack/adaptor/define_decl.hpp>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <vector>

namespace meta {

class MetaStore;

class MetaWriteBatch {
private:
  friend class MetaStore;
  leveldb::WriteBatch batch_{};
  std::vector<std::pair<meta::pg_id_t, meta::stripe_id_t>> stripe_to_pg_map_;
  MetaStore &store_;

  MetaWriteBatch(MetaStore &store) : store_{store} {}

public:
  template <typename T> void putMeta(meta::key_t key, const T &value) {
    auto ser_key = leveldb::Slice{key.data(), key.size()};
    std::stringstream ser_buf{};
    serde::serialize(value, ser_buf);
    auto ser_str = std::move(ser_buf).str();
    auto ser_val = leveldb::Slice{ser_str};
    batch_.Put(ser_key, ser_val);
  };

  void putStripeToPG(meta::stripe_id_t stripe_id, meta::pg_id_t pg_id) {
    stripe_to_pg_map_.emplace_back(pg_id, stripe_id);
  };

  void flush();
};

/// record the stripes that belong to a PG
class PGToStripeMap {
  using pg_map_t = std::map<pg_id_t, std::set<stripe_id_t>>;
  using mutex_t = std::mutex;

public:
  auto getPGMap() const -> const pg_map_t & { return pg_to_stripe_map_; };
  auto getPGMap() -> pg_map_t & { return pg_to_stripe_map_; };
  auto getPGStripes(meta::pg_id_t pg_id) const
      -> std::optional<std::vector<stripe_id_t>> {
    auto lock = std::lock_guard<mutex_t>{store_mtx_};
    auto &pg_map = getPGMap();
    if (pg_map.find(pg_id) == pg_map.end()) {
      return std::nullopt;
    }
    auto &pg = getPGMap().at(pg_id);
    auto target = std::vector<stripe_id_t>{};
    target.reserve(target.size());
    std::copy(pg.cbegin(), pg.cend(), std::back_inserter(target));
    return target;
  }

  auto serialize(std::ostream &stream) const -> void {
    auto lock = std::lock_guard<mutex_t>{store_mtx_};
    serde::serialize(pg_to_stripe_map_, stream);
  };

  auto deserialize(util::bytes_span buf) -> void {
    auto lock = std::lock_guard<mutex_t>{store_mtx_};
    auto obj = pg_map_t{};
    serde::deserialize(buf, obj);
    this->pg_to_stripe_map_ = std::move(obj);
  };

private:
  pg_map_t pg_to_stripe_map_{};
  mutable mutex_t store_mtx_{};
  MSGPACK_DEFINE(pg_to_stripe_map_);
};
} // namespace meta

namespace serde {
template <>
inline void serialize(const meta::PGToStripeMap &obj, std::ostream &stream) {
  obj.serialize(stream);
};
template <>
inline void deserialize(util::bytes_span buf, meta::PGToStripeMap &obj) {
  obj.deserialize(buf);
};
} // namespace serde

namespace meta {
/// MetaStore is a class that stores the metadata to a database backend.
/// # Note
/// MetaStore can be shared by multiple threads.
/// But the concurrent write is not guaranteed to be serialized.
class MetaStore {
private:
  friend class MetaWriteBatch;
  using database_ptr = std::unique_ptr<leveldb::DB>;
  using mutex_t = std::mutex;
  database_ptr db_;
  PGToStripeMap pg_to_stripe_map_{};

  [[nodiscard]] auto getDB() const -> const leveldb::DB & {
    return *db_.get();
  };
  auto getDB() -> leveldb::DB & { return *db_.get(); };

public:
  MetaStore() = default;

  auto persist_pg_map_as(meta::key_t key) { putMeta(key, pg_to_stripe_map_); }
  auto load_pg_map_from(meta::key_t key) { getMeta(key, pg_to_stripe_map_); }

  /// Open the database at the given path.
  void open(const std::string &path) {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB *db{};
    auto status = leveldb::DB::Open(options, path, &db);
    if (!status.ok()) {
      throw Exception("fail to open database, " + status.ToString());
    }
    db_.reset(db);
  };

  /// Put the key-value pair to the database.
  /// # Note
  /// Use MetaWriteBatch for batch write.
  template <typename T> void putMeta(meta::key_t key, const T &value) {
    auto ser_key = leveldb::Slice{key.data(), key.size()};
    std::stringstream ser_buf{};
    serde::serialize(value, ser_buf);
    auto ser_str = std::move(ser_buf).str();
    auto ser_val = leveldb::Slice{ser_str};
    auto status = getDB().Put(leveldb::WriteOptions(), ser_key, ser_val);
    if (!status.ok()) {
      throw Exception("fail to put key-value pair, " + status.ToString());
    }
  };

  /// Get the value from the database.
  template <typename T> void getMeta(meta::key_t key, T &value) {
    auto ser_key = leveldb::Slice{key.data(), static_cast<size_t>(key.size())};
    std::string raw_value{};
    auto status = getDB().Get(leveldb::ReadOptions{}, ser_key, &raw_value);
    if (status.ok()) {
      serde::deserialize({raw_value.data(), raw_value.size()}, value);
      return;
    }
    if (status.IsNotFound()) {
      throw NotFound(fmt::format("key not found"));
    } else {
      throw Exception("fail to get key-value pair, " + status.ToString());
    }
  };

  auto getPGStripes(meta::pg_id_t pg_id) const
      -> std::optional<std::vector<stripe_id_t>> {
    return pg_to_stripe_map_.getPGStripes(pg_id);
  }

  /// Get a write batch.
  auto getWriteBatch() -> MetaWriteBatch { return MetaWriteBatch{*this}; };
};

inline void MetaWriteBatch::flush() {
  auto status = store_.getDB().Write(leveldb::WriteOptions{}, &batch_);
  if (!status.ok()) {
    throw Exception("fail to flush write batch, " + status.ToString());
  }
  for (auto &[pg_id, stripe_id] : stripe_to_pg_map_) {
    auto &pg_map = store_.pg_to_stripe_map_.getPGMap()[pg_id];
    pg_map.emplace(stripe_id);
  }
}

} // namespace meta
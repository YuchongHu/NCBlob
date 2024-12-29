#pragma once

#include "ceph_hash.hpp"
#include "meta.hpp"
#include "meta_store.hpp"

#include <boost/numeric/conversion/cast.hpp>
// #include <glog/logging.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <sys/types.h>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
template <typename T, typename = void> struct is_hashable : std::false_type {};

template <typename T>
struct is_hashable<
    T, std::void_t<decltype(std::declval<std::hash<T>>()(std::declval<T>()))>>
    : std::true_type {};
} // namespace

namespace meta {
class MetaCore;

class StripeMetaRecord {
  friend class MetaCore;

private:
  /// stripe id to register
  /// if stripe_id is not set, it will be assigned uniquely
  std::optional<meta::stripe_id_t> stripe_id;
  std::optional<EcType> ec_type;
  std::optional<BlobLayout> blob_layout;
  /// EC k and m parameters
  std::optional<std::pair<meta::ec_param_t, meta::ec_param_t>> ec_km;
  // std::optional<meta::pg_id_t> pg;
  /// size of each chunk
  std::optional<std::size_t> chunk_size;
  /// metadata of the chunks in this encoded stripe
  /// #Note
  /// - size of the list is excepted to be equal to k+m
  /// - all the fields except stripe_id are excepted to be set
  std::vector<ChunkMeta> chunks;
  /// metadata of the blobs merged in this stripe.
  /// It is a list of <blob trace id, blob size> pairs
  /// # Note
  /// - If blob.size() == 1, it is a single blob stripe
  /// - all the fields except stripe_id are excepted to be set
  std::vector<meta::BlobMeta> blobs;
  /// PG id to which this stripe belongs
  std::optional<pg_id_t> pg_id;

public:
  /// Setters for StripeMetaRecord
  /// Set the stripe id and return a reference to self
  /// # Note
  /// If stripe_id is not set or set to 0, it will be assigned uniquely
  auto setStripeId(meta::stripe_id_t stripeId) -> StripeMetaRecord & {
    stripe_id = stripeId;
    return *this;
  }
  /// Set the EC type and return a reference to self
  auto setEcType(EcType ecType) -> StripeMetaRecord & {
    ec_type = ecType;
    return *this;
  }
  /// Set the blob layout and return a reference to self
  auto setBlobLayout(BlobLayout blobLayout) -> StripeMetaRecord & {
    blob_layout = blobLayout;
    return *this;
  }
  /// Set the EC k and m parameters and return a reference to self
  auto setEcKM(meta::ec_param_t ec_k,
               meta::ec_param_t ec_m) -> StripeMetaRecord & {
    ec_km = std::make_pair(ec_k, ec_m);
    return *this;
  }
  /// Set the chunk size and return a reference to self
  auto setChunkSize(std::size_t chunkSize) -> StripeMetaRecord & {
    chunk_size = chunkSize;
    return *this;
  }
  /// Set the list of chunks and return a reference to self
  auto setChunks(std::vector<ChunkMeta> chunks) -> StripeMetaRecord & {
    this->chunks = std::move(chunks);
    return *this;
  }
  /// Set the list of blobs and return a reference to self
  auto setBlobs(std::vector<meta::BlobMeta> blobs) -> StripeMetaRecord & {
    this->blobs = std::move(blobs);
    return *this;
  }
  /// Set the PG id and return a reference to self
  auto setPG(meta::pg_id_t pgId) -> StripeMetaRecord & {
    this->pg_id = pgId;
    return *this;
  }
};

class BlobRecorder {
  std::filesystem::path path_{};
  std::fstream file_{};

public:
  auto open(const std::filesystem::path &path, bool create_new) -> void {
    path_ = path;
    auto stat = std::ios::out | std::ios::in;
    if (create_new) {
      stat |= std::ios::trunc;
    }
    file_.open(path.generic_string(), stat);
    if (!file_.is_open()) {
      throw std::runtime_error("failed to open blob record file");
    }
  }

  auto clear() -> void {
    // remove the content of the file
    file_.close();
    file_.open(path_.generic_string(),
               std::ios::out | std::ios::in | std::ios::trunc);
  }

  auto put_record(meta::blob_id_t blob_id) { file_ << blob_id << '\n'; }
  auto next_record() -> std::optional<meta::blob_id_t> {
    meta::blob_id_t blob_id{};
    if (file_ >> blob_id) {
      return blob_id;
    }
    return std::nullopt;
  }
};

/// MetaCore is a class that provides the metadata operations.
class MetaCore {
private:
  MetaStore metaStore_{};
  meta::stripe_id_t start_at{0};
  std::atomic<meta::stripe_id_t> stripe_id_counter_{start_at};
  BlobRecorder blobRecorder_{};

  std::string core_name_{};
  std::size_t _pg_num{};
  ec_param_t _k{};
  ec_param_t _m{};
  std::map<pg_id_t, PGMeta> pg_{};
  std::map<node_id_t, ip_t> worker_to_ip_{};
  std::map<node_id_t, std::vector<disk_id_t>> node_to_disk_{};
  std::map<disk_id_t, node_id_t> disk_to_node_{};

  template <typename I>
  static auto make_prefixed_key(MetaType type, I value) -> meta::key_t {
    static_assert(is_hashable<I>::value, "template type is not hashable");
    auto key = key_t{};
    std::memcpy(key.data(), &type, sizeof(type));
    auto value_hash = std::hash<I>{}(value);
    std::memcpy(key.data() + sizeof(type), &value_hash, sizeof(value_hash));
    return key;
  }

public:
  MetaCore() = delete;
  MetaCore(std::string core_name) : core_name_{std::move(core_name)} {}

  auto persist() -> void {
    auto name_hash = std::hash<std::string>{}(core_name_);
    auto key = make_prefixed_key(meta::MetaType::PG_MAP, name_hash);
    metaStore_.persist_pg_map_as(key);
    key = make_prefixed_key(MetaType::STRIPE_RANGE, name_hash);
    std::array<meta::stripe_id_t, 2> range = {start_at, stripe_id_counter_};
    metaStore_.putMeta(key, range);
  }

  auto load_meta() -> void {
    auto hash = std::hash<std::string>{}(core_name_);
    auto key = make_prefixed_key(meta::MetaType::PG_MAP, hash);
    metaStore_.load_pg_map_from(key);
    key = make_prefixed_key(meta::MetaType::STRIPE_RANGE, hash);
    std::array<meta::stripe_id_t, 2> range{};
    metaStore_.getMeta(key, range);
    this->start_at = range[0];
    this->stripe_id_counter_ = range[1];
  }

  auto launch(const std::filesystem::path &path, bool create_new) -> void {
    if (create_new) {
      try {
        if (std::filesystem::exists(path)) {
          // remove all the files under working dir
          for (auto &dir : std::filesystem::directory_iterator(path)) {
            std::filesystem::remove_all(dir);
          }
        } else {
          std::filesystem::create_directories(path);
        }
      } catch (std::exception &e) {
        throw meta::Exception(e.what());
      }
    }
    metaStore_.open(path.generic_string());
    blobRecorder_.open(path / "blob_record", create_new);
  }

  auto clear_blobs() -> void { blobRecorder_.clear(); }

  auto next_blobs_record() -> std::optional<meta::blob_id_t> {
    return blobRecorder_.next_record();
  }

  auto setStripeIdCounter(meta::stripe_id_t counter) {
    start_at = counter;
    stripe_id_counter_ = counter;
  }

  auto registerDisk(DiskMeta disk) {
    node_to_disk_[disk.node_id].push_back(disk.id);
    disk_to_node_[disk.id] = disk.node_id;
  }

  auto registerPG(std::size_t pg_num, ec_param_t k, ec_param_t m) {
    _pg_num = pg_num;
    _k = k;
    _m = m;
    std::vector<unsigned long> nodes;
    nodes.reserve(node_to_disk_.size());
    for (auto &[node, _] : node_to_disk_) {
      nodes.push_back(node);
    }

    constexpr std::int64_t RAND_SEED = 0x1234;
    std::mt19937 gen(RAND_SEED); // mersenne_twister_engine seeded with rd()
    std::vector<size_t> numbers(nodes.size());

    for (int i = 0; i < numbers.size(); i++) {
      numbers[i] = i;
    }

    srand((unsigned)time(0));
    for (std::size_t i = 0; i < _pg_num; i++) {
      auto pg_id = meta::pg_id_t(i);
      auto pg_meta = PGMeta{};
      pg_meta.pg_id = pg_id;
      pg_meta.k = _k;
      pg_meta.m = _m;

      std::shuffle(numbers.begin(), numbers.end(), gen);

      // random select k+m nodes
      for (std::size_t j = 0; j < _k + _m; j++) {
        auto node = nodes.at(numbers[j]);
        // random select a disk from each node
        auto &disks = node_to_disk_.at(node);
        auto disk_idx = rand() % disks.size();
        auto disk_id = disks[disk_idx];
        pg_meta.disk_list.push_back(disk_id);
      }

      pg_[pg_id] = pg_meta;
    }
  }

  auto registerWorker(node_id_t worker_id, ip_t ip) {
    worker_to_ip_[worker_id] = std::move(ip);
  }

  auto next_stripe_id() -> stripe_id_t { return stripe_id_counter_++; }
  auto current_stripe_id() const -> stripe_id_t { return stripe_id_counter_; }

  auto select_pg(meta::stripe_id_t stripe_id) -> meta::pg_id_t {
    auto s = std::to_string(stripe_id);
    auto pg_id = ceph_str_hash_rjenkins(s.c_str(), s.size()) % _pg_num;
    return pg_id;
  }

  auto pg_to_worker_nodes(meta::pg_id_t pg_id) -> std::vector<node_id_t> {
    if (pg_.find(pg_id) == pg_.end()) {
      throw meta::Exception("pg_id not found");
    }
    auto &disks = pg_[pg_id].disk_list;
    auto nodes = std::vector<node_id_t>{};
    nodes.reserve(disks.size());
    std::transform(disks.cbegin(),
                   disks.cend(),
                   std::back_inserter(nodes),
                   [&disk_to_node_ = this->disk_to_node_](auto &disk_id) {
                     return disk_to_node_.at(disk_id);
                   });
    return nodes;
  }

  auto pg_to_worker_ip(meta::pg_id_t pg_id) -> std::vector<ip_t> {
    if (pg_.find(pg_id) == pg_.end()) {
      throw meta::Exception("pg_id not found");
    }
    auto &disks = pg_[pg_id].disk_list;
    auto ips = std::vector<ip_t>{};
    ips.reserve(disks.size());
    std::transform(disks.cbegin(),
                   disks.cend(),
                   std::back_inserter(ips),
                   [this](auto &disk_id) {
                     return worker_to_ip_.at(disk_to_node_.at(disk_id));
                   });
    return ips;
  }

  auto
  pg_to_disks(meta::pg_id_t pg_id) const -> const std::vector<disk_id_t> & {
    if (pg_.find(pg_id) == pg_.end()) {
      throw meta::Exception("pg_id not found");
    }
    return pg_.at(pg_id).disk_list;
  }

  auto worker_ip(node_id_t worker_id) const -> const ip_t & {
    if (worker_to_ip_.find(worker_id) == worker_to_ip_.end()) {
      throw meta::Exception("worker_id not found");
    }
    return worker_to_ip_.at(worker_id);
  }

  /// Make meta data for a stripe and its blobs, and register them to the
  /// store
  /// # Return
  /// id for this stripe
  auto registerStripe(StripeMetaRecord record) -> stripe_id_t {
    // get a write batch
    auto batch = metaStore_.getWriteBatch();
    // make stripe meta data
    meta::StripeMeta stripe_meta{};
    // stripe id
    if (record.stripe_id.has_value()) {
      stripe_meta.stripe_id = record.stripe_id.value();
    } else {
      stripe_meta.stripe_id = stripe_id_counter_++;
    }
    // ec k and m
    if (!record.ec_km.has_value()) {
      throw meta::Exception("ec_km is required to register a stripe");
    }
    std::tie(stripe_meta.k, stripe_meta.m) = record.ec_km.value();
    // ec type
    if (!record.ec_type.has_value()) {
      throw meta::Exception("ec_type is required to register a stripe");
    }
    stripe_meta.ec_type = record.ec_type.value();
    // blob layout
    if (!record.blob_layout.has_value()) {
      throw meta::Exception("blob_layout is required to register a stripe");
    }
    stripe_meta.blob_layout = record.blob_layout.value();
    // chunk size
    if (!record.chunk_size.has_value()) {
      throw meta::Exception("chunk_size is required to register a stripe");
    }
    stripe_meta.chunk_size = record.chunk_size.value();
    // blobs
    if (record.blobs.empty()) {
      throw meta::Exception("blob list is required to register a stripe");
    }
    std::for_each_n(record.blobs.begin(),
                    record.blobs.size(),
                    [stripe_id = stripe_meta.stripe_id](auto &blob) {
                      blob.stripe_id = stripe_id;
                    });
    stripe_meta.blobs = std::move(record.blobs);
    // chunks
    if (record.chunks.empty()) {
      throw meta::Exception("chunk list is required to register a stripe");
    }
    std::for_each_n(record.chunks.begin(),
                    record.chunks.size(),
                    [stripe_id = stripe_meta.stripe_id](auto &chunk) {
                      chunk.stripe_id = stripe_id;
                    });
    stripe_meta.chunks = std::move(record.chunks);
    // pg
    if (!record.pg_id.has_value()) {
      throw meta::Exception("pg_id is required to register a stripe");
    }
    batch.putStripeToPG(stripe_meta.stripe_id, record.pg_id.value());

    // make key for the stripe
    auto stripe_id_key =
        make_prefixed_key(meta::MetaType::Stripe, stripe_meta.stripe_id);
    // write the stripe meta to the batch
    batch.putMeta(stripe_id_key, stripe_meta);

    // make blobs meta data and write
    for (const auto &blob : stripe_meta.blobs) {
      auto blob_id = blob.blob_id;
      auto blob_id_key = make_prefixed_key(meta::MetaType::Blob, blob_id);
      batch.putMeta(blob_id_key, blob);
      blobRecorder_.put_record(blob_id);
    }

    // make chunk meta data and write
    for (std::size_t i = 0; i < record.chunks.size(); i++) {
      auto &chunk = record.chunks[i];
      auto chunk_index = boost::numeric_cast<meta::chunk_index_t>(i);
      auto chunk_id = meta::chunk_id_t{.stripe_id = stripe_meta.stripe_id,
                                       .chunk_index = chunk_index};
      auto chunk_id_key = make_prefixed_key(meta::MetaType::Chunk, chunk_id);
      batch.putMeta(chunk_id_key, chunk);
    }

    // flush batch to the store
    batch.flush();
    return stripe_meta.stripe_id;
  };

  /// Get the repairing meta data from a failed chunk
  auto chunkRepair(meta::chunk_id_t chunk_id) -> StripeMeta {
    auto stripe_id = chunk_id.stripe_id;
    auto stripe_id_key = make_prefixed_key(meta::MetaType::Stripe, stripe_id);
    auto stripe_meta = meta::StripeMeta{};
    metaStore_.getMeta(stripe_id_key, stripe_meta);
    return stripe_meta;
  }

  struct DiskRepairMeta {
    PGMeta pg;
    chunk_index_t chunk_index;
    std::vector<stripe_id_t> stripe_list;
  };
  /// Get the repairing meta data from a failed disk
  auto diskRepair(disk_id_t disk_id) -> std::vector<DiskRepairMeta> {
    auto target = std::vector<DiskRepairMeta>{};
    for (auto &[pg_id, pg_meta] : pg_) {
      for (std::size_t i = 0; i < pg_meta.disk_list.size(); i++) {
        auto const &disk = pg_meta.disk_list[i];
        if (disk == disk_id) {
          // found a PG that contains the failed disk
          target.emplace_back(DiskRepairMeta{
              .pg = pg_meta,
              .chunk_index = boost::numeric_cast<chunk_index_t>(i),
              .stripe_list = {},
          });
          break;
        }
      }
    }
    // get the stripe list for each disk repair meta
    for (auto &disk_repair_meta : target) {
      disk_repair_meta.stripe_list =
          std::move(metaStore_.getPGStripes(disk_repair_meta.pg.pg_id))
              .value_or(std::vector<meta::stripe_id_t>{});
    }
    auto result = std::vector<DiskRepairMeta>{};
    std::copy_if(target.begin(),
                 target.end(),
                 std::back_inserter(result),
                 [](auto &disk_repair_meta) {
                   return !disk_repair_meta.stripe_list.empty();
                 });
    return result;
  }

  auto blob_meta(meta::blob_id_t blob_id) -> meta::BlobMeta {
    auto blob_id_key = make_prefixed_key(meta::MetaType::Blob, blob_id);
    auto blob_meta = meta::BlobMeta{};
    metaStore_.getMeta(blob_id_key, blob_meta);
    // auto stripe_meta = meta::StripeMeta{};
    // auto stripe_id_key =
    //     make_prefixed_key(meta::MetaType::Stripe, blob_meta.stripe_id);
    // metaStore_.getMeta(stripe_id_key, stripe_meta);
    // return {blob_meta, std::move(stripe_meta)};
    return blob_meta;
  }

  auto stripe_meta(meta::stripe_id_t stripe_id) -> meta::StripeMeta {
    auto stripe_id_key = make_prefixed_key(meta::MetaType::Stripe, stripe_id);
    auto stripe_meta = meta::StripeMeta{};
    metaStore_.getMeta(stripe_id_key, stripe_meta);
    return stripe_meta;
  }
};

} // namespace meta
#pragma once

#include "comm.hh"
#include "coord_prof.hh"
#include "meta.hpp"
#include "meta_core.hpp"

#include <cassert>
#include <cstddef>
#include <utility>
namespace coord {
struct StripeType {
  meta::EcType ec_type;
  meta::BlobLayout blob_layout;
  auto operator==(const StripeType &rhs) const -> bool {
    return ec_type == rhs.ec_type && blob_layout == rhs.blob_layout;
  }
};
} // namespace coord
namespace std {
template <> struct hash<coord::StripeType> {
  auto operator()(const coord::StripeType &stripe_type) const -> std::size_t {
    std::size_t hash_value = 0;
    std::hash<meta::EcType> ec_type_hash{};
    std::hash<meta::BlobLayout> blob_layout_hash{};
    hash_value ^= ec_type_hash(stripe_type.ec_type);
    hash_value ^= blob_layout_hash(stripe_type.blob_layout);
    return hash_value;
  }
};
} // namespace std

namespace coord {
struct StripeStat {
  std::size_t count;
  std::size_t size;
  auto operator==(const StripeStat &rhs) const -> bool {
    return count == rhs.count && size == rhs.size;
  }
};
struct BuildDataResult {
  using count_t = std::size_t;
  using size_t = std::size_t;
  std::unordered_map<StripeType, StripeStat> stripe_stat;
  std::pair<meta::stripe_id_t, meta::stripe_id_t> stripe_range;
  size_t total_size;
};
struct RepairResult {
  std::size_t total_size;
};
struct ReadResult {
  std::size_t total_size;
};

class Coordinator {
private:
  profile_ref_t profile_;
  meta::MetaCore meta_core_;
  comm::CommManager comm_;

public:
  Coordinator(profile_ref_t profile_ref)
      : profile_{profile_ref}, comm_{profile_ref->workspace_name},
        meta_core_(profile_ref->workspace_name) {
    auto create_new = profile_ref->action == ActionType::BuildData;
    meta_core_.launch(profile_->working_dir, create_new);
    meta_core_.setStripeIdCounter(profile_->start_at);
    for (std::size_t node_id = 0; node_id < profile_->worker_ip.size();
         node_id++) {
      for (auto disk_id : profile_->disk_list[node_id]) {
        meta_core_.registerDisk(
            meta::DiskMeta{.id = disk_id, .node_id = node_id});
      }
      meta_core_.registerWorker(node_id, profile_->worker_ip.at(node_id));
    }
    meta_core_.registerPG(profile_->pg_num, profile_->ec_k, profile_->ec_m);
    switch (profile_ref->action) {
    case ActionType::RepairChunk:
    case ActionType::RepairFailureDomain:
    case ActionType::Read:
      meta_core_.load_meta();
      break;
    case ActionType::BuildData:
    case ActionType::DegradeRead:
      // DO NOTHING
      break;
    }
  };
  auto build_data() -> BuildDataResult;
  auto repair_chunk() -> void;
  auto repair_failure_domain() -> RepairResult;
  auto read() -> ReadResult;
  auto degrade_read() -> ReadResult;
  auto persist() -> void;
  auto load_meta() -> void;
  auto clear_meta() -> void;
};

} // namespace coord
#pragma once

#include "meta.hpp"
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <variant>
namespace coord {
enum class RepairManner : std::uint8_t {
  Centralized = 0,
  Pipelined,
};

enum class ActionType : std::uint8_t {
  BuildData = 0,
  RepairChunk,
  RepairFailureDomain,
  Read,
  DegradeRead,
};

enum class MergeScheme : std::uint8_t {
  Fixed = 0,
  Partition,
  Baseline,
  IntraLocality,
  InterLocality,
  IntraForDegradeRead,
  InterForDegradeRead,
};

struct ChunkRepairProfile {
  RepairManner manner;
  meta::chunk_index_t chunk_index;
};

struct FailureDomainRepairProfile {
  meta::disk_id_t failed_disk;
};

struct BuildDataProfile {
  meta::ec_param_t ec_k;
  meta::ec_param_t ec_m;
  meta::EcType ec_type;
  MergeScheme merge_scheme;
  std::size_t merge_size;
  std::size_t partition_size;
};

enum class LoadType : std::uint8_t {
  ByStripe = 0,
  BySize,
};

template <typename T> auto from_str(std::string_view str) -> T {
  static_assert(sizeof(T) == 0, "from_str is not specialized for this type");
  throw std::invalid_argument("Unsupported type for from_str");
}

template <> auto from_str<RepairManner>(std::string_view str) -> RepairManner;
template <> auto from_str<ActionType>(std::string_view str) -> ActionType;
template <> auto from_str<MergeScheme>(std::string_view str) -> MergeScheme;
template <> auto from_str<LoadType>(std::string_view str) -> LoadType;
auto format_as(const RepairManner &manner) -> std::string_view;
auto format_as(const ActionType &action) -> std::string_view;
auto format_as(const MergeScheme &scheme) -> std::string_view;
auto format_as(const LoadType &load_type) -> std::string_view;

namespace profile_default {
inline static constexpr std::size_t START_AT{0};
}
// NOLINTBEGIN (cppcoreguidelines-non-private-member-variables-in-classes)
class Profile {
public:
  std::string workspace_name;
  meta::ip_t ip;
  std::filesystem::path working_dir;
  meta::ec_param_t ec_k;
  meta::ec_param_t ec_m;
  meta::EcType ec_type;
  std::vector<meta::ip_t> worker_ip;
  std::vector<std::vector<meta::disk_id_t>> disk_list;
  LoadType load_type;
  std::size_t test_load;
  std::size_t start_at;
  MergeScheme merge_scheme;
  std::size_t merge_size;
  std::size_t chunk_size;
  std::size_t blob_size;
  std::size_t partition_size;
  std::filesystem::path trace;
  std::size_t pg_num;
  ActionType action;
  std::filesystem::path log_file;
  // NOLINTEND (cppcoreguidelines-non-private-member-variables-in-classes)

private:
  using action_variant_t =
      std::variant<std::monostate, ChunkRepairProfile,
                   FailureDomainRepairProfile, BuildDataProfile>;
  action_variant_t action_variant_{};

public:
  [[nodiscard]] auto chunk_repair_profile() const -> const ChunkRepairProfile &;
  [[nodiscard]] auto
  failure_domain_repair_profile() const -> const FailureDomainRepairProfile &;
  [[nodiscard]] auto build_data_profile() const -> const BuildDataProfile &;

  /// parse the program arguments from the command line
  static auto ParseToml(const std::filesystem::path &cfg_file) -> Profile;

  // operator for ostream
  friend auto operator<<(std::ostream &os,
                         const Profile &profile) -> std::ostream &;
};

using profile_ref_t = std::shared_ptr<const Profile>;
} // namespace coord
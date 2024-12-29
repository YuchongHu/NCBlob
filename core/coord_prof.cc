#include "coord_prof.hh"

#include "exception.hpp"
#include "fmt/format.h"
#include "toml.hpp"
#include "toml11/exception.hpp"
#include "toml11/find.hpp"

#include <boost/numeric/conversion/cast.hpp>

template <>
auto coord::from_str<coord::RepairManner>(std::string_view str)
    -> RepairManner {
  if (str == "Centralized") {
    return RepairManner::Centralized;
  } else if (str == "Pipelined") {
    return RepairManner::Pipelined;
  } else {
    throw std::invalid_argument("Invalid RepairManner type");
  }
}

template <>
auto coord::from_str<coord::ActionType>(std::string_view str) -> ActionType {
  if (str == "BuildData") {
    return ActionType::BuildData;
  } else if (str == "RepairChunk") {
    return ActionType::RepairChunk;
  } else if (str == "RepairFailureDomain") {
    return ActionType::RepairFailureDomain;
  } else if (str == "Read") {
    return ActionType::Read;
  } else if (str == "DegradeRead") {
    return ActionType::DegradeRead;
  } else {
    throw std::invalid_argument("Invalid ActionType type");
  }
}

template <>
auto coord::from_str<coord::MergeScheme>(std::string_view str) -> MergeScheme {
  if (str == "Fixed") {
    return MergeScheme::Fixed;
  } else if (str == "Partition") {
    return MergeScheme::Partition;
  } else if (str == "Baseline") {
    return MergeScheme::Baseline;
  } else if (str == "IntraLocality") {
    return MergeScheme::IntraLocality;
  } else if (str == "InterLocality") {
    return MergeScheme::InterLocality;
  } else if (str == "IntraForDegradeRead") {
    return MergeScheme::IntraForDegradeRead;
  } else if (str == "InterForDegradeRead") {
    return MergeScheme::InterForDegradeRead;
  } else {
    throw std::invalid_argument("Invalid MergeScheme type");
  }
}

template <>
auto coord::from_str<coord::LoadType>(std::string_view str) -> LoadType {
  if (str == "ByStripe") {
    return LoadType::ByStripe;
  } else if (str == "BySize") {
    return LoadType::BySize;
  } else {
    throw std::invalid_argument("Invalid LoadType type");
  }
}

auto as_str(const coord::ActionType &action) -> std::string_view {
  using coord::ActionType;
  switch (action) {
  case ActionType::BuildData:
    return "BuildData";
  case ActionType::RepairChunk:
    return "RepairChunk";
  case ActionType::RepairFailureDomain:
    return "RepairFailureDomain";
  case ActionType::Read:
    return "Read";
  case ActionType::DegradeRead:
    return "DegradeRead";
  default:
    throw std::invalid_argument("Invalid action type");
  }
}

auto as_str(const coord::RepairManner &manner) -> std::string_view {
  using coord::RepairManner;
  switch (manner) {
  case RepairManner::Centralized:
    return "Centralized";
  case RepairManner::Pipelined:
    return "Pipelined";
  default:
    throw std::invalid_argument("Invalid repair manner type");
  }
}

auto as_str(const coord::MergeScheme &scheme) -> std::string_view {
  using coord::MergeScheme;
  switch (scheme) {
  case MergeScheme::Fixed:
    return "Fixed";
  case MergeScheme::Baseline:
    return "Baseline";
  case MergeScheme::Partition:
    return "Partition";
  case MergeScheme::IntraLocality:
    return "IntraLocality";
  case MergeScheme::InterLocality:
    return "InterLocality";
  case MergeScheme::InterForDegradeRead:
    return "InterForDegradeRead";
  case MergeScheme::IntraForDegradeRead:
    return "IntraForDegradeRead";
  default:
    throw std::invalid_argument("Invalid merge scheme type");
  }
}

auto as_str(const coord::LoadType &load_type) -> std::string_view {
  using coord::LoadType;
  switch (load_type) {
  case LoadType::ByStripe:
    return "ByStripe";
  case LoadType::BySize:
    return "BySize";
  default:
    throw std::invalid_argument("Invalid load type");
  }
}

auto coord::format_as(const RepairManner &manner) -> std::string_view {
  return as_str(manner);
}

auto coord::format_as(const ActionType &action) -> std::string_view {
  return as_str(action);
}

auto coord::format_as(const MergeScheme &scheme) -> std::string_view {
  return as_str(scheme);
}

auto coord::format_as(const LoadType &load_type) -> std::string_view {
  return as_str(load_type);
}

auto coord::Profile::chunk_repair_profile() const
    -> const ChunkRepairProfile & {
  return std::get<ChunkRepairProfile>(action_variant_);
}

auto coord::Profile::failure_domain_repair_profile() const
    -> const FailureDomainRepairProfile & {
  return std::get<FailureDomainRepairProfile>(action_variant_);
}

auto coord::Profile::build_data_profile() const -> const BuildDataProfile & {
  return std::get<BuildDataProfile>(action_variant_);
}

auto validate_profile(const coord::Profile &profile) {
  if (profile.worker_ip.empty()) {
    throw std::invalid_argument("worker_ip is empty");
  }
  if (profile.disk_list.empty()) {
    throw std::invalid_argument("disk_list is empty");
  }
  if (profile.test_load == 0) {
    throw std::invalid_argument("test_load is 0");
  }
  if (profile.merge_size == 0) {
    throw std::invalid_argument("merge_size is 0");
  }
  if (profile.pg_num == 0) {
    throw std::invalid_argument("pg_num is 0");
  }
  if (profile.disk_list.size() != profile.worker_ip.size()) {
    throw std::invalid_argument(
        "disk_list size is not equal to worker_ip size");
  }
  if (profile.ec_k + profile.ec_m > profile.worker_ip.size()) {
    throw std::invalid_argument("ec_k + ec_m > worker_ip.size()");
  }
}

auto coord::Profile::ParseToml(const std::filesystem::path &cfg_file)
    -> Profile {
  auto profile = Profile{};
  auto data = toml::parse(cfg_file, toml::spec::v(1, 1, 0));
  profile.workspace_name = toml::find<std::string>(data, "workspace_name");
  profile.ip = toml::find<std::string>(data, "ip");
  profile.working_dir = toml::find<std::string>(data, "working_dir");
  profile.working_dir /= profile.workspace_name;
  profile.worker_ip = toml::find<std::vector<std::string>>(data, "worker_ip");
  profile.disk_list =
      toml::find<std::vector<std::vector<meta::disk_id_t>>>(data, "disk_list");

  profile.action =
      from_str<ActionType>(toml::find<std::string>(data, "action"));
  profile.log_file = toml::find<std::string>(data, "log_file");

  profile.ec_k = toml::find<meta::ec_param_t>(data, "ec_k");
  profile.ec_m = toml::find<meta::ec_param_t>(data, "ec_m");
  profile.ec_type =
      meta::string_to_ectype(toml::find<std::string>(data, "ec_type"));
  profile.partition_size =
      toml::find_or<std::size_t>(data, "partition_size", 0);
  profile.load_type =
      from_str<LoadType>(toml::find<std::string>(data, "load_type"));
  auto load_f64 = std::double_t{0.0};
  try {
    load_f64 =
        static_cast<std::double_t>(toml::find<std::size_t>(data, "test_load"));
  } catch (toml::exception &e) {
    load_f64 = toml::find<std::double_t>(data, "test_load");
  }
  if (load_f64 < 0.0) {
    throw std::invalid_argument("test_load is negative");
  }
  switch (profile.load_type) {
  case LoadType::ByStripe: {
    profile.test_load = boost::numeric_cast<std::size_t>(load_f64);
  } break;
  case LoadType::BySize: {
    constexpr auto GB{1 << 30};
    profile.test_load = boost::numeric_cast<std::size_t>(load_f64 * GB);
    break;
  }
  }
  if (load_f64 == 0.0) {
    profile.test_load = std::numeric_limits<decltype(profile.test_load)>::max();
  }

  profile.start_at =
      toml::find_or<std::size_t>(data, "start_at", profile_default::START_AT);
  profile.trace = toml::find<std::string>(data, "trace");
  profile.merge_size = toml::find<std::size_t>(data, "merge_size");
  profile.merge_scheme =
      from_str<MergeScheme>(toml::find<std::string>(data, "merge_scheme"));
  if (profile.merge_scheme == MergeScheme::InterForDegradeRead ||
      profile.merge_scheme == MergeScheme::IntraForDegradeRead) {
    profile.blob_size = toml::find<std::size_t>(data, "blob_size");
    profile.chunk_size = toml::find<std::size_t>(data, "chunk_size");
  }
  profile.pg_num = toml::find<std::size_t>(data, "pg_num");
  switch (profile.action) {
  case coord::ActionType::RepairFailureDomain: {
    auto repair_profile = FailureDomainRepairProfile{};
    auto repair_domain_data = toml::find(data, "repair_failure_domain");
    auto failed_disk =
        toml::find<std::int64_t>(repair_domain_data, "failed_disk");
    if (failed_disk == -1) {
      // randomly select a disk
      repair_profile.failed_disk = boost::numeric_cast<meta::disk_id_t>(
          std::rand() % profile.disk_list.size());
    } else {
      repair_profile.failed_disk =
          boost::numeric_cast<meta::disk_id_t>(failed_disk);
    }
    profile.action_variant_ = repair_profile;
  } break;
  case ActionType::RepairChunk: {
    auto repair_profile = ChunkRepairProfile{};
    auto repair_chunk_data = toml::find(data, "repair_chunk");
    repair_profile.manner = from_str<RepairManner>(
        toml::find<std::string>(repair_chunk_data, "manner"));
    repair_profile.chunk_index =
        toml::find<meta::chunk_index_t>(repair_chunk_data, "chunk_index");
    profile.action_variant_ = repair_profile;
  } break;
  default: {
    profile.action_variant_ = std::monostate{};
  }
  };
  validate_profile(profile);
  return profile;
}
auto coord::operator<<(std::ostream &os,
                       const coord::Profile &profile) -> std::ostream & {
  os << fmt::format("[Info] workspace_name: {}\n", profile.workspace_name);
  os << fmt::format("[Info] ip: {}\n", profile.ip);
  os << fmt::format("[Info] pg_num: {}\n", profile.pg_num);
  os << fmt::format("[Info] working_dir: {}\n",
                    profile.working_dir.generic_string());
  os << fmt::format("[Info] trace: {}\n", profile.trace.generic_string());
  os << fmt::format("[Info] log_file: {}\n", profile.log_file.generic_string());
  os << fmt::format("[Info] ec_k: {}\n", profile.ec_k);
  os << fmt::format("[Info] ec_m: {}\n", profile.ec_m);
  if (profile.load_type == LoadType::ByStripe) {
    os << fmt::format("[Info] test load: {} stripes\n", profile.test_load);
  } else {
    os << fmt::format("[Info] test load: {} GB\n",
                      profile.test_load >> 30); // NOLINT
  }
  os << fmt::format("[Info] action: {}\n", profile.action);
  switch (profile.action) {
  case ActionType::BuildData: {
    os << fmt::format("[Info] start_at: {}\n", profile.start_at);
    os << fmt::format("[Info] merge_scheme: {}\n", profile.merge_scheme);
    switch (profile.merge_scheme) {
    case MergeScheme::Baseline:
      os << fmt::format("[Info] ec_type: {}\n", profile.ec_type);
      break;
    case MergeScheme::Partition:
      os << fmt::format("[Info] ec_type: large chunks: {}, small chunks: {}\n",
                        meta::EcType::CLAY,
                        meta::EcType::RS);
      os << fmt::format("[Info] partition_size: {}\n", profile.partition_size);
      break;
    case MergeScheme::IntraLocality:
    case MergeScheme::InterLocality:
      os << fmt::format("[Info] ec_type: large chunks: {}, small chunks: {}\n",
                        meta::EcType::CLAY,
                        meta::EcType::NSYS);
      os << fmt::format("[Info] merge_size: {}\n", profile.merge_size);
      break;
    case MergeScheme::Fixed:
      err::Unimplemented();
    case coord::MergeScheme::InterForDegradeRead:
      os << fmt::format("[Info] ec_type: {}\n", profile.ec_type);
      os << fmt::format("[Info] chunk_size: {}\n", profile.chunk_size);
      break;
    case MergeScheme::IntraForDegradeRead:
      os << fmt::format("[Info] ec_type: {}\n", profile.ec_type);
      os << fmt::format("[Info] chunk_size: {}\n", profile.chunk_size);
      os << fmt::format("[Info] blob_size: {}\n", profile.blob_size);
      break;
    default:
      err::Unreachable();
    };
  } break;
  case ActionType::RepairChunk: {
    os << "[Info] repair_profile: \n";
    os << fmt::format("\tmanner: {}\n", profile.chunk_repair_profile().manner);
    os << fmt::format("\tchunk_index: {}\n",
                      profile.chunk_repair_profile().chunk_index);
  } break;
  case ActionType::RepairFailureDomain:
    break;
  case coord::ActionType::Read: {
  } break;
  case coord::ActionType::DegradeRead: {
  } break;
  default:
    err::Unreachable();
  }
  os << std::flush;
  return os;
}

#pragma once

#include <boost/core/span.hpp>
#include <functional>
#include <msgpack.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace meta {

/// type alias for EC
using ec_param_t = std::int32_t;
enum class EcType : std::uint8_t { RS = 0, NSYS = 1, CLAY = 2 };
inline static auto ec_type_to_string(EcType ec_type) -> std::string_view {
  switch (ec_type) {
  case EcType::RS:
    return "RS";
  case EcType::NSYS:
    return "NSYS";
  case EcType::CLAY:
    return "CLAY";
  default:
    throw std::invalid_argument("Invalid ec type");
  }
}
inline auto format_as(const EcType &ec_type) -> std::string_view {
  return ec_type_to_string(ec_type);
}
inline auto operator<<(std::ostream &os,
                       const EcType &ec_type) -> std::ostream & {
  os << ec_type_to_string(ec_type);
  return os;
}
inline static auto string_to_ectype(const std::string_view &str) -> EcType {
  if (str == "RS") {
    return EcType::RS;
  } else if (str == "NSYS") {
    return EcType::NSYS;
  } else if (str == "CLAY") {
    return EcType::CLAY;
  } else {
    throw std::invalid_argument("Invalid ec type");
  }
}

/// type alias for Nodes
using ip_t = std::string;
using node_id_t = std::uint64_t;
using disk_id_t = std::uint32_t;
struct DiskMeta {
  disk_id_t id;
  node_id_t node_id;
};
struct NodeMeta {
  ip_t ip;
  std::vector<disk_id_t> disks;
};

/// type alias for Stripe

using stripe_id_t = std::uint64_t;
struct BlobMeta;
struct ChunkMeta;
enum class BlobLayout : std::uint8_t {
  /// merge before split
  Horizontal = 0,
  /// split before merge
  Vertical = 1
};
inline auto format_as(const BlobLayout &layout) -> std::string_view {
  switch (layout) {
  case BlobLayout::Horizontal:
    return {"Horizontal"};
  case BlobLayout::Vertical:
    return {"Vertical"};
  }
};
struct StripeMeta {
  stripe_id_t stripe_id;
  ec_param_t k;
  ec_param_t m;
  EcType ec_type;
  BlobLayout blob_layout;
  std::size_t chunk_size;
  std::vector<BlobMeta> blobs;
  std::vector<ChunkMeta> chunks;

  MSGPACK_DEFINE(stripe_id, k, m, ec_type, blob_layout, chunk_size, blobs,
                 chunks);
};

/// type alias for chunk
using chunk_index_t = std::uint8_t;
using chunk_id_t = struct {
  stripe_id_t stripe_id;
  chunk_index_t chunk_index;
};
struct ChunkMeta {
  stripe_id_t stripe_id;
  /// chunk index in the stripe
  chunk_index_t chunk_index;
  std::size_t size;

  MSGPACK_DEFINE(stripe_id, chunk_index, size);
};

/// type alias for Blob
/// Blob id is a pair of stripe id and blob index in the stripe
struct BlobMeta;
struct BlobAccessMeta;
using blob_index_t = std::uint32_t;
using blob_id_t = std::size_t;
namespace blob_types {
using anon_id_t = std::size_t;
using time_stamp_t = std::uint64_t;
using blob_type_t = std::string;
using e_tag_t = std::size_t;
} // namespace blob_types
struct BlobMeta {
  /// unique id of this blob
  /// # Note: 0 is reserved
  blob_id_t blob_id{};
  /// stripe id of this blob
  /// # Note: 0 is reserved
  stripe_id_t stripe_id{0};
  /// index of this blob in the stripe
  blob_index_t blob_index{};
  std::size_t size{};
  /// offset in the merged block
  std::size_t offset{};

  MSGPACK_DEFINE(blob_id, stripe_id, blob_index, size, offset);
};

/// type alias for PG
struct PGMeta;
using pg_id_t = std::uint32_t;
struct PGMeta {
  pg_id_t pg_id;
  ec_param_t k;
  ec_param_t m;
  std::vector<disk_id_t> disk_list;
};

enum class MetaType : std::uint8_t {
  Blob = 0,
  Stripe = 1,
  Disk = 2,
  Node = 3,
  PG = 4,
  Chunk = 5,
  PG_MAP = 6,
  STRIPE_RANGE = 7,
};
/// key for the meta data entry
using key_t = std::array<char, sizeof(MetaType) + sizeof(std::size_t)>;

} // namespace meta

namespace std {
template <> struct hash<meta::chunk_id_t> {
  auto operator()(const meta::chunk_id_t &chunk_id) const -> std::size_t {
    constexpr std::size_t HASH_SALT{0x9e3779b9};
    std::size_t hash_value = 0;
    std::hash<meta::stripe_id_t> stripe_id_hash{};
    std::hash<meta::chunk_index_t> chunk_index_hash{};
    hash_value ^= stripe_id_hash(chunk_id.stripe_id);
    hash_value ^= chunk_index_hash(chunk_id.chunk_index) + HASH_SALT +
                  (hash_value << 6) + (hash_value >> 2); // NOLINT
    return hash_value;
  }
};
} // namespace std

MSGPACK_ADD_ENUM(meta::EcType);
MSGPACK_ADD_ENUM(meta::BlobLayout);

namespace std {
template <> struct hash<meta::EcType> {
  auto operator()(const meta::EcType &ec_type) const noexcept -> std::size_t {
    return std::hash<std::underlying_type_t<meta::EcType>>{}(
        static_cast<std::underlying_type_t<meta::EcType>>(ec_type));
  }
};

template <> struct hash<meta::BlobLayout> {
  auto operator()(const meta::BlobLayout &blob_layout) const noexcept
      -> std::size_t {
    return std::hash<std::underlying_type_t<meta::BlobLayout>>{}(
        static_cast<std::underlying_type_t<meta::BlobLayout>>(blob_layout));
  }
};

template <> struct equal_to<meta::EcType> {
  auto operator()(const meta::EcType &lhs,
                  const meta::EcType &rhs) const -> bool {
    return lhs == rhs;
  }
};

template <> struct equal_to<meta::BlobLayout> {
  auto operator()(const meta::BlobLayout &lhs,
                  const meta::BlobLayout &rhs) const -> bool {
    return lhs == rhs;
  }
};
} // namespace std

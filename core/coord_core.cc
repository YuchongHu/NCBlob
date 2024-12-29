#include "coord_core.hh"

#include "BS_thread_pool.hpp"
#include "BlockCommand.hh"
#include "Command.hh"
#include "Tasks.hh"
#include "azure_trace.hh"
#include "channel.hpp"
#include "comm.hh"
#include "coord_prof.hh"
#include "ec_intf.hh"
#include "exception.hpp"
#include "merge_scheme.hpp"
#include "meta.hpp"
#include "meta_core.hpp"

#include "meta_exception.hpp"

#include <boost/numeric/conversion/cast.hpp>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {
using namespace coord;
/// repair single chunk in a stripe
struct RepairChunk {
private:
  auto centralize_horizontal(std::size_t offset, std::size_t size)
      -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
    namespace trc = task::repair::centralize;
    auto &meta_core = meta_core_ref.get();
    auto stripe_id = stripe_meta.stripe_id;
    // size of the chunk
    auto const chunk_size = stripe_meta.chunk_size;
    auto pg_id = meta_core.select_pg(stripe_id);
    const auto &diskList = meta_core.pg_to_disks(pg_id);
    const auto &ipList = meta_core.pg_to_worker_ip(pg_id);
    auto ec_type = stripe_meta.ec_type;
    auto ec_k = stripe_meta.k;
    auto ec_m = stripe_meta.m;
    switch (ec_type) {
    case meta::EcType::RS: {
      return trc::rs::TaskBuilder{.stripeId = stripe_id,
                                  .chunk_index = failed_chunk.chunk_index,
                                  .k = ec_k,
                                  .m = ec_m,
                                  .offset = offset,
                                  .size = size,
                                  .diskList = diskList,
                                  .ipList = ipList}
          .build();
    } break;
    case meta::EcType::NSYS: {
      return trc::nsys::TaskBuilder{.stripeId = stripe_id,
                                    .chunk_index = failed_chunk.chunk_index,
                                    .k = ec_k,
                                    .m = ec_m,
                                    .offset = offset,
                                    .size = size / ec_m,
                                    .diskList = diskList,
                                    .ipList = ipList}
          .build();
    } break;
    case meta::EcType::CLAY: {
      auto w = ec::encoder::clay::Encoder{ec_k, ec_m}.get_sub_chunk_num();
      if (stripe_meta.chunk_size % w != 0) {
        throw std::invalid_argument("chunk size is not divisible by clay::w");
      }
      return trc::clay::TaskBuilder{.stripeId = stripe_id,
                                    .chunk_index = failed_chunk.chunk_index,
                                    .k = ec_k,
                                    .m = ec_m,
                                    .size = stripe_meta.chunk_size / w,
                                    .diskList = diskList,
                                    .ipList = ipList}
          .build();
    } break;
    }
  };
  auto pipelined_horizontal()
      -> std::pair<std::vector<Command>, std::vector<meta::ip_t>> {
    namespace trp = task::repair::pipeline;
    err::Unreachable();
    auto &meta_core = meta_core_ref.get();
    auto stripe_id = stripe_meta.stripe_id;
    // size of the chunk
    auto const chunk_size = stripe_meta.chunk_size;
    auto pg_id = meta_core.select_pg(stripe_id);
    const auto &diskList = meta_core.pg_to_disks(pg_id);
    const auto &ipList = meta_core.pg_to_worker_ip(pg_id);
    auto ec_type = stripe_meta.ec_type;
    auto ec_k = stripe_meta.k;
    auto ec_m = stripe_meta.m;
    switch (ec_type) {
    case meta::EcType::RS: {
      return trp::rs::TaskBuilder{.stripeId = stripe_id,
                                  .chunk_index = failed_chunk.chunk_index,
                                  .k = ec_k,
                                  .m = ec_m,
                                  .diskList = diskList,
                                  .ipList = ipList}
          .build();
    } break;
    case meta::EcType::NSYS: {
      return trp::nsys::TaskBuilder{.stripeId = stripe_id,
                                    .chunk_index = failed_chunk.chunk_index,
                                    .k = ec_k,
                                    .m = ec_m,
                                    .diskList = diskList,
                                    .ipList = ipList}
          .build();
    } break;
    case meta::EcType::CLAY: {
      return trp::clay::TaskBuilder{.stripeId = stripe_id,
                                    .chunk_index = failed_chunk.chunk_index,
                                    .k = ec_k,
                                    .m = ec_m,
                                    .diskList = diskList,
                                    .ipList = ipList}
          .build();
    } break;
    }
  };
  auto centralize_vertical(std::size_t offset, std::size_t size)
      -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
    namespace trc = task::repair::centralize;
    // err::Todo("check range");
    auto &meta_core = meta_core_ref.get();
    auto stripe_id = stripe_meta.stripe_id;
    // size of the chunk
    auto const chunk_size = stripe_meta.chunk_size;
    auto pg_id = meta_core.select_pg(stripe_id);
    const auto &diskList = meta_core.pg_to_disks(pg_id);
    const auto &ipList = meta_core.pg_to_worker_ip(pg_id);
    auto ec_type = stripe_meta.ec_type;
    switch (ec_type) {
    case meta::EcType::RS: {
      err::Unreachable();
      return trc::rs::TaskBuilder{.stripeId = stripe_id,
                                  .chunk_index = failed_chunk.chunk_index,
                                  .k = stripe_meta.k,
                                  .m = stripe_meta.m,
                                  .offset = offset,
                                  .size = size,
                                  .diskList = diskList,
                                  .ipList = ipList}
          .build();
    } break;
    case meta::EcType::NSYS: {
      return trc::nsys::TaskBuilder{.stripeId = stripe_id,
                                    .chunk_index = failed_chunk.chunk_index,
                                    .k = stripe_meta.k,
                                    .m = stripe_meta.m,
                                    .offset = offset,
                                    .size = size,
                                    .diskList = diskList,
                                    .ipList = ipList}
          .build();
    } break;
    case meta::EcType::CLAY: {
      err::Unreachable();
      auto w = ec::encoder::clay::Encoder{stripe_meta.k, stripe_meta.m}
                   .get_sub_chunk_num();
      if (stripe_meta.chunk_size % w != 0) {
        throw std::invalid_argument("chunk size is not divisible by clay::w");
      }
      return trc::clay::TaskBuilder{.stripeId = stripe_id,
                                    .chunk_index = failed_chunk.chunk_index,
                                    .k = stripe_meta.k,
                                    .m = stripe_meta.m,
                                    .offset = offset,
                                    .size = size / w,
                                    .diskList = diskList,
                                    .ipList = ipList}
          .build();
    } break;
    }
  };
  auto pipelined_vertical()
      -> std::pair<std::vector<Command>, std::vector<meta::ip_t>> {
    err::Unimplemented("unreachable");
    namespace trp = task::repair::pipeline;
    auto &meta_core = meta_core_ref.get();
    auto stripe_id = stripe_meta.stripe_id;
    // size of the chunk
    auto const chunk_size = stripe_meta.chunk_size;
    auto pg_id = meta_core.select_pg(stripe_id);
    const auto &diskList = meta_core.pg_to_disks(pg_id);
    const auto &ipList = meta_core.pg_to_worker_ip(pg_id);
    auto ec_type = stripe_meta.ec_type;
    auto ec_k = stripe_meta.k;
    auto ec_m = stripe_meta.m;
    switch (ec_type) {
    case meta::EcType::RS: {
      return trp::rs::TaskBuilder{
          .stripeId = stripe_id,
          .chunk_index = failed_chunk.chunk_index,
          .k = ec_k,
          .m = ec_m,
          // .offset = offset,
          // .size = stripe_meta.chunk_size / profile.ec_m,
          .diskList = diskList,
          .ipList = ipList}
          .build();
    } break;
    case meta::EcType::NSYS: {
      return trp::nsys::TaskBuilder{
          .stripeId = stripe_id,
          .chunk_index = failed_chunk.chunk_index,
          .k = ec_k,
          .m = ec_m,
          // .offset = offset,
          // .size =
          //     stripe_meta.chunk_size / profile.ec_m,
          .diskList = diskList,
          .ipList = ipList}
          .build();
    } break;
    case meta::EcType::CLAY: {
      return trp::clay::TaskBuilder{
          .stripeId = stripe_id,
          .chunk_index = failed_chunk.chunk_index,
          .k = ec_k,
          .m = ec_m,
          // .offset = offset,
          // .size =
          //     stripe_meta.chunk_size / profile.ec_m,
          .diskList = diskList,
          .ipList = ipList}
          .build();
    } break;
    }
  };

public:
  /// repair a chunk in a stripe
  /// # Return: the ip of the ack node
  [[nodiscard("wait for the ack")]] auto
  operator()(::coord::RepairManner repair_manner) -> meta::ip_t {
    auto ec_type = stripe_meta.ec_type;
    // auto repair_manner = profile_cref.get().chunk_repair_profile().manner;
    auto ack_ip = meta::ip_t{};
    if (stripe_meta.blob_layout == meta::BlobLayout::Horizontal) {
      if (repair_manner == RepairManner::Centralized) {
        auto [commandList, distIpList] =
            centralize_horizontal(0, stripe_meta.chunk_size);
        for (std::size_t i = 0; i < commandList.size(); i++) {
          const auto &cmd = commandList.at(i);
          // if (i == commandList.size() - 1) {
          //   // the last command is the write command
          //   LOG(INFO) << fmt::format("sending repair write command to ip: {},
          //   "
          //                            "stripe id: {}, chunk index: {}, ",
          //                            distIpList.at(i),
          //                            stripe_meta.stripe_id,
          //                            failed_chunk.chunk_index)
          //             << std::endl;
          // }
          comm_ref.get().push_to(distIpList.at(i), cmd);
        }
        ack_ip = distIpList.back();
      } else if (repair_manner == RepairManner::Pipelined) {
        auto [commandList, distIpList] = pipelined_horizontal();
        for (std::size_t i = 0; i < commandList.size(); i++) {
          const auto &cmd = commandList.at(i);
          comm_ref.get().push_to(distIpList.at(i), cmd);
        }
        err::Todo("set ack ip");
      } else {
        throw std::invalid_argument("Unsupported repair manner");
      }
    } else if (stripe_meta.blob_layout == meta::BlobLayout::Vertical) {
      // read by vertical blob layout
      if (repair_manner == RepairManner::Centralized) {
        std::size_t sub_chunk_size = stripe_meta.chunk_size / stripe_meta.m;
        std::size_t offset = 0;
        auto [commandList, distIpList] =
            centralize_vertical(offset, sub_chunk_size);
        for (std::size_t i = 0; i < commandList.size(); i++) {
          const auto &cmd = commandList.at(i);
          comm_ref.get().push_to(distIpList.at(i), cmd);
        }
        ack_ip = distIpList.back();
      } else if (repair_manner == RepairManner::Pipelined) {
        auto [commandList, distIpList] = pipelined_vertical();
        for (std::size_t i = 0; i < commandList.size(); i++) {
          const auto &cmd = commandList.at(i);
          comm_ref.get().push_to(distIpList.at(i), cmd);
        }
        err::Todo("set ack ip");
      } else {
        throw std::invalid_argument("Unsupported repair manner");
      }
    } else {
      throw std::invalid_argument("Unsupported blob layout");
    }
    return ack_ip;
  }
  meta::StripeMeta stripe_meta;
  meta::chunk_id_t failed_chunk;
  std::reference_wrapper<meta::MetaCore> meta_core_ref;
  std::reference_wrapper<comm::CommManager> comm_ref;
};

struct ReadBlob {

  [[nodiscard("wait for ack")]] auto operator()() -> std::vector<meta::ip_t> {
    auto &meta_core = meta_core_ref.get();
    const auto &stripe_meta = *stripe_meta_ref.get();
    auto pg_id = meta_core.select_pg(blob_meta.stripe_id);
    const auto &node_id = meta_core.pg_to_worker_nodes(pg_id);
    const auto &diskList = meta_core.pg_to_disks(pg_id);
    const auto &ipList = meta_core.pg_to_worker_ip(pg_id);
    const auto chunk_size = stripe_meta.chunk_size;
    auto command_list = std::vector<std::vector<BlockCommand>>{};
    auto ip_list = std::vector<std::vector<meta::ip_t>>{};
    auto ack_ip_list = std::vector<meta::ip_t>{};
    auto blob_range_start = blob_meta.offset;
    auto blob_range_end = blob_meta.offset + blob_meta.size;
    switch (stripe_meta.blob_layout) {
    case meta::BlobLayout::Horizontal: {
      switch (stripe_meta.ec_type) {
      case meta::EcType::RS:
        err::Unreachable();
      case meta::EcType::NSYS: {
        using offset_t = std::size_t;
        using size_t = std::size_t;
        auto ranges = std::vector<std::pair<offset_t, size_t>>{};
        auto chunk_indicies = std::vector<meta::chunk_index_t>{};
        auto remaining = boost::numeric_cast<std::int64_t>(blob_meta.size);
        auto cur_off = blob_meta.offset;
        while (remaining > 0) {
          chunk_indicies.emplace_back(cur_off / chunk_size);
          auto in_chunk_off = cur_off % chunk_size;
          ranges.emplace_back(in_chunk_off, chunk_size - in_chunk_off);
          cur_off += ranges.back().second;
          remaining -= boost::numeric_cast<std::int64_t>(ranges.back().second);
          if (remaining < 0) {
            ranges.back().second += remaining;
          }
        }
        for (std::size_t i = 0; i < ranges.size(); i++) {
          auto [commands, ips] =
              task::read::nsys::TaskBuilder{
                  .stripeId = stripe_meta.stripe_id,
                  .chunk_index = chunk_indicies.at(i),
                  .k = stripe_meta.k,
                  .m = stripe_meta.m,
                  .offset = ranges.at(i).first,
                  .size = ranges.at(i).second,
                  .diskList = diskList,
                  .ipList = ipList,
              }
                  .build();
          ack_ip_list.emplace_back(ips.back());
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips));
        }
      } break;
      case meta::EcType::CLAY: {
        // for clay, we need to repair the whole chunk
        auto chunk_index_start = blob_range_start / chunk_size;
        auto chunk_index_end = (blob_range_end + chunk_size - 1) / chunk_size;
        auto w = ec::encoder::clay::Encoder{stripe_meta.k, stripe_meta.m}
                     .get_sub_chunk_num();
        for (auto chunk_index = chunk_index_start;
             chunk_index < chunk_index_end;
             chunk_index++) {
          auto offset = chunk_index * chunk_size;
          auto size = chunk_size;
          auto [commands, ips] =
              task::read::clay::TaskBuilder{.stripeId = blob_meta.stripe_id,
                                            .chunk_index = chunk_index,
                                            .k = stripe_meta.k,
                                            .m = stripe_meta.m,
                                            // .offset = offset,
                                            .size = size / w,
                                            .diskList = diskList,
                                            .ipList = ipList}
                  .build();
          ack_ip_list.emplace_back(ips.back());
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips));
        }
      } break;
      default:
        err::Unreachable();
      }
    } break;
    case meta::BlobLayout::Vertical: {
      switch (stripe_meta.ec_type) {
      case meta::EcType::RS:
      case meta::EcType::CLAY:
        err::Unimplemented("use nsys for vertical layout");
      case meta::EcType::NSYS: {
        auto sub_chunk_off_start = blob_range_start / stripe_meta.k;
        auto sub_chunk_off_end = blob_range_end / stripe_meta.k;
        auto size = sub_chunk_off_end - sub_chunk_off_start;
        for (std::size_t chunk_index = 0;
             chunk_index < stripe_meta.k + stripe_meta.m;
             chunk_index++) {
          auto [commands, ips] =
              task::read::nsys::TaskBuilder{.stripeId = blob_meta.stripe_id,
                                            .chunk_index = chunk_index,
                                            .k = stripe_meta.k,
                                            .m = stripe_meta.m,
                                            .offset = sub_chunk_off_start,
                                            .size = size,
                                            .diskList = diskList,
                                            .ipList = ipList}
                  .build();
          ack_ip_list.emplace_back(ips.back());
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips));
        }
      } break;
      default:
        err::Unreachable();
      }
    } break;
    default:
      err::Unreachable();
    }
    for (std::size_t i = 0; i < command_list.size(); i++) {
      const auto &commands = command_list.at(i);
      const auto &ips = ip_list.at(i);
      for (std::size_t j = 0; j < commands.size(); j++) {
        const auto &cmd = commands.at(j);
        comm_ref.get().push_to(ips.at(j), cmd);
        {
          DLOG(INFO) << fmt::format("{} stripe {} chunk {}, size {}, ip {}",
                                    cmd.getComputeType(),
                                    cmd.getStripeId(),
                                    cmd.getBlockId(),
                                    cmd.getSize(),
                                    ips.at(j));
        }
      }
    }
    return ack_ip_list;
  }

  meta::BlobMeta blob_meta;
  std::shared_ptr<const meta::StripeMeta> stripe_meta_ref;
  std::reference_wrapper<meta::MetaCore> meta_core_ref;
  std::reference_wrapper<comm::CommManager> comm_ref;
};

struct DegradeReadBlob {
  [[nodiscard("wait for ack")]] auto
  operator()() -> std::vector<std::pair<meta::ip_t, std::string_view>> {
    auto &meta_core = meta_core_ref.get();
    const auto &stripe_meta = *stripe_meta_ref.get();
    auto pg_id = meta_core.select_pg(blob_meta.stripe_id);
    const auto &node_id = meta_core.pg_to_worker_nodes(pg_id);
    const auto &diskList = meta_core.pg_to_disks(pg_id);
    const auto &ipList = meta_core.pg_to_worker_ip(pg_id);
    const auto chunk_size = stripe_meta.chunk_size;
    auto command_list = std::vector<std::vector<BlockCommand>>{};
    auto ip_list =
        std::vector<std::pair<std::vector<meta::ip_t>, std::string_view>>{};
    auto ack_ip_list = std::vector<std::pair<meta::ip_t, std::string_view>>{};
    auto blob_range_start = blob_meta.offset;
    auto blob_range_end = blob_meta.offset + blob_meta.size;
    switch (stripe_meta.blob_layout) {
    case meta::BlobLayout::Horizontal: {
      using offset_t = std::size_t;
      using size_t = std::size_t;
      auto ranges = std::vector<std::pair<offset_t, size_t>>{};
      auto chunk_indicies = std::vector<meta::chunk_index_t>{};
      auto remaining = boost::numeric_cast<std::int64_t>(blob_meta.size);
      auto cur_off = blob_meta.offset;
      while (remaining > 0) {
        chunk_indicies.emplace_back(cur_off / chunk_size);
        auto in_chunk_off = cur_off % chunk_size;
        ranges.emplace_back(in_chunk_off, chunk_size - in_chunk_off);
        cur_off += ranges.back().second;
        remaining -= boost::numeric_cast<std::int64_t>(ranges.back().second);
        if (remaining < 0) {
          ranges.back().second += remaining;
        }
      }
      switch (stripe_meta.ec_type) {
      case meta::EcType::RS:
        for (std::size_t i = 0; i < ranges.size(); i++) {
          // for rs, degrade read is the same as repair
          auto [commands, ips] =
              task::repair::centralize::rs::TaskBuilder{
                  .stripeId = stripe_meta.stripe_id,
                  .chunk_index = chunk_indicies.at(i),
                  .k = stripe_meta.k,
                  .m = stripe_meta.m,
                  .offset = ranges.at(i).first,
                  .size = ranges.at(i).second,
                  .diskList = diskList,
                  .ipList = ipList,
              }
                  .build();
          ack_ip_list.emplace_back(ips.back(), comm::REPAIR_ACK_LIST_KEY);
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips), comm::REPAIR_ACK_LIST_KEY);
        }
      case meta::EcType::NSYS: {
        for (std::size_t i = 0; i < ranges.size(); i++) {
          auto [commands, ips] =
              task::read::nsys::TaskBuilder{
                  .stripeId = stripe_meta.stripe_id,
                  .chunk_index = chunk_indicies.at(i),
                  .k = stripe_meta.k,
                  .m = stripe_meta.m,
                  .offset = ranges.at(i).first,
                  .size = ranges.at(i).second,
                  .diskList = diskList,
                  .ipList = ipList,
              }
                  .build();
          ack_ip_list.emplace_back(ips.back(), comm::READ_ACK_LIST_KEY);
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips), comm::READ_ACK_LIST_KEY);
        }
      } break;
      case meta::EcType::CLAY: {
        // for clay, we need to repair the whole chunk
        auto chunk_index_start = blob_range_start / chunk_size;
        auto chunk_index_end = (blob_range_end + chunk_size - 1) / chunk_size;
        auto w = ec::encoder::clay::Encoder{stripe_meta.k, stripe_meta.m}
                     .get_sub_chunk_num();
        for (auto chunk_index = chunk_index_start;
             chunk_index < chunk_index_end;
             chunk_index++) {
          auto offset = chunk_index * chunk_size;
          auto size = chunk_size;
          auto [commands, ips] =
              task::repair::centralize::clay::TaskBuilder{
                  .stripeId = blob_meta.stripe_id,
                  .chunk_index = chunk_index,
                  .k = stripe_meta.k,
                  .m = stripe_meta.m,
                  // .offset = offset,
                  .size = size / w,
                  .diskList = diskList,
                  .ipList = ipList}
                  .build();
          ack_ip_list.emplace_back(ips.back(), comm::REPAIR_ACK_LIST_KEY);
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips), comm::REPAIR_ACK_LIST_KEY);
        }
      } break;
      default:
        err::Unreachable();
      }
    } break;
    case meta::BlobLayout::Vertical: {
      switch (stripe_meta.ec_type) {
      case meta::EcType::RS:
      case meta::EcType::CLAY:
        err::Unimplemented("use nsys for vertical layout");
      case meta::EcType::NSYS: {
        auto sub_chunk_off_start = blob_range_start / stripe_meta.k;
        auto sub_chunk_off_end = blob_range_end / stripe_meta.k;
        auto size = sub_chunk_off_end - sub_chunk_off_start;
        for (std::size_t chunk_index = 0;
             chunk_index < stripe_meta.k + stripe_meta.m;
             chunk_index++) {
          auto [commands, ips] =
              task::read::nsys::TaskBuilder{.stripeId = blob_meta.stripe_id,
                                            .chunk_index = chunk_index,
                                            .k = stripe_meta.k,
                                            .m = stripe_meta.m,
                                            .offset = sub_chunk_off_start,
                                            .size = size,
                                            .diskList = diskList,
                                            .ipList = ipList}
                  .build();
          ack_ip_list.emplace_back(ips.back(), comm::READ_ACK_LIST_KEY);
          command_list.emplace_back(std::move(commands));
          ip_list.emplace_back(std::move(ips), comm::READ_ACK_LIST_KEY);
        }
      } break;
      default:
        err::Unreachable();
      }
    } break;
    default:
      err::Unreachable();
    }
    for (std::size_t i = 0; i < command_list.size(); i++) {
      const auto &commands = command_list.at(i);
      const auto &ips = ip_list.at(i);
      for (std::size_t j = 0; j < commands.size(); j++) {
        const auto &cmd = commands.at(j);
        auto &[ip, key] = ips;
        comm_ref.get().push_to(ip.at(j), cmd);
        {
          DLOG(INFO) << fmt::format("{} stripe {} chunk {}, size {}, ip {}",
                                    cmd.getComputeType(),
                                    cmd.getStripeId(),
                                    cmd.getBlockId(),
                                    cmd.getSize(),
                                    ip.at(j));
        }
      }
    }
    return ack_ip_list;
  }

  meta::BlobMeta blob_meta;
  std::shared_ptr<const meta::StripeMeta> stripe_meta_ref;
  std::reference_wrapper<meta::MetaCore> meta_core_ref;
  std::reference_wrapper<comm::CommManager> comm_ref;
};
} // namespace

auto coord::Coordinator::build_data() -> BuildDataResult {
  auto &profile = *profile_.get();
  // open trace and pre-paremerge scheme
  std::unique_ptr<trace::stripe_stream::StripeStreamInterface> stripe_stream{};
  constexpr std::size_t TRACE_STEP_BY{256};
  auto trace_reader = trace::make_azure_trace(profile.trace, TRACE_STEP_BY);
  if (profile.merge_scheme == MergeScheme::Fixed) {
    auto stream =
        std::make_unique<trace::stripe_stream::baseline::StripeStream>();
    stream->set_encoder(
        ec::make_encoder(profile.ec_type, profile.ec_k, profile.ec_m));
    auto blobs = trace::blob_stream::MergeStreamInterfacePtr{
        std::make_unique<trace::blob_stream::FixedSizeMergeStream>(
            std::move(trace_reader), profile.merge_size)};
    stream->set_merge_stream(std::move(blobs));
    stripe_stream = std::move(stream);
  } else if (profile.merge_scheme == MergeScheme::Baseline) {
    auto stream =
        std::make_unique<trace::stripe_stream::baseline::StripeStream>();
    stream->set_encoder(
        ec::make_encoder(profile.ec_type, profile.ec_k, profile.ec_m));
    stream->set_merge_stream(
        std::make_unique<trace::blob_stream::BasicMergeStream>(
            std::move(trace_reader), profile.merge_size));
    stripe_stream = std::move(stream);
  } else if (profile.merge_scheme == MergeScheme::Partition) {
    auto stream =
        std::make_unique<trace::stripe_stream::partition::StripeStream>(
            profile.partition_size);
    stream->set_large_blob_encoder(
        ec::make_encoder(meta::EcType::CLAY, profile.ec_k, profile.ec_m));
    stream->set_small_blob_encoder(
        ec::make_encoder(meta::EcType::RS, profile.ec_k, profile.ec_m));
    stream->set_merge_stream(
        std::make_unique<trace::blob_stream::BasicMergeStream>(
            std::move(trace_reader), profile.merge_size));
    stripe_stream = std::move(stream);
  } else if (profile.merge_scheme == MergeScheme::IntraLocality) {
    auto stream =
        std::make_unique<trace::stripe_stream::hybrid::SplitBeforeMerge>(
            std::move(trace_reader),
            profile.merge_size,
            ec::make_encoder(meta::EcType::CLAY, profile.ec_k, profile.ec_m),
            ec::make_encoder(meta::EcType::NSYS, profile.ec_k, profile.ec_m));
    stripe_stream = std::move(stream);
  } else if (profile.merge_scheme == MergeScheme::InterLocality) {
    auto stream = std::make_unique<trace::stripe_stream::hybrid::InterLocality>(
        std::move(trace_reader),
        profile.merge_size,
        ec::make_encoder(meta::EcType::CLAY, profile.ec_k, profile.ec_m),
        ec::make_encoder(meta::EcType::NSYS, profile.ec_k, profile.ec_m),
        profile.merge_size);
    stripe_stream = std::move(stream);
  } else if (profile.merge_scheme == MergeScheme::InterForDegradeRead) {
    stripe_stream =
        std::make_unique<trace::stripe_stream::degrade_read::InterLocality>(
            ec::make_encoder(profile.ec_type, profile.ec_k, profile.ec_m),
            profile.chunk_size * profile.ec_k,
            profile.blob_size);
  } else if (profile.merge_scheme == MergeScheme::IntraForDegradeRead) {
    stripe_stream =
        std::make_unique<trace::stripe_stream::degrade_read::IntraLocality>(
            ec::make_encoder(profile.ec_type, profile.ec_k, profile.ec_m),
            profile.chunk_size * profile.ec_k);
  } else {
    throw std::invalid_argument("Unsupported merge scheme");
  }

  // build data and store
  std::size_t load_cnt{0};
  std::size_t stripe_cnt{profile.start_at};
  std::atomic_size_t total_size{0};
  std::unordered_map<coord::StripeType, StripeStat> stripe_stat{};
  BS::thread_pool task_pool{};
  std::queue<std::future<void>> future_queue{};
  constexpr std::size_t QUEUE_THRESHOLD = 64;
  auto wait_ack = [&future_queue](std::size_t threshold = QUEUE_THRESHOLD) {
    if (future_queue.size() >= threshold) {
      while (!future_queue.empty()) {
        try {
          future_queue.front().wait();
        } catch (std::exception &e) {
          LOG(ERROR) << fmt::format("Exception caught: {}", e.what())
                     << std::endl;
        }
        future_queue.pop();
      }
    }
  };
  while (load_cnt < profile.test_load) {
    auto item = trace::stripe_stream::StripeStreamItem{};
    try {
      // make stripe
      item = stripe_stream->next_stripe();
    } catch (trace::TraceException &e) {
      if (e.error_enum() == trace::trace_error_e::Exhaust) {
        std::cerr << "[Info] Trace exhausted at load: " << load_cnt
                  << std::endl;
        break;
      } else {
        throw;
      }
    }
    auto [blobs, stripe, ec_type, blob_layout] = std::move(item);
    auto stripe_id = meta_core_.next_stripe_id();
    stripe_cnt++;
    auto stripe_size = std::accumulate(
        stripe.cbegin(),
        stripe.cend(),
        std::size_t{0},
        [](auto acc, auto &chunk) { return acc + chunk.size(); });
    auto &stat = stripe_stat[coord::StripeType{.ec_type = ec_type,
                                               .blob_layout = blob_layout}];
    stat.count++;
    stat.size += stripe_size;

    auto task = [this,
                 stripe_id,
                 blobs = std::move(blobs),
                 stripe = std::move(stripe),
                 ec_type,
                 blob_layout,
                 &total_size]() {
      auto &profile = *profile_.get();
      auto chunk_size = stripe.front().size();
      // map this stripe to a pg
      auto pg_id = meta_core_.select_pg(stripe_id);
      // register the stripe meta data
      std::vector<meta::ChunkMeta> chunk_meta{};
      chunk_meta.reserve(profile.ec_k + profile.ec_m);
      for (meta::ec_param_t i = 0; i < profile.ec_k + profile.ec_m; i++) {
        chunk_meta.emplace_back(meta::ChunkMeta{
            .stripe_id = stripe_id,
            .chunk_index = boost::numeric_cast<meta::chunk_index_t>(i),
            .size = chunk_size,
        });
      }
      auto stripe_meta_record = meta::StripeMetaRecord{};
      stripe_meta_record.setStripeId(stripe_id)
          .setBlobs(blobs)
          .setChunks(std::move(chunk_meta))
          .setChunkSize(chunk_size)
          .setEcKM(profile.ec_k, profile.ec_m)
          .setPG(pg_id)
          .setBlobLayout(blob_layout)
          .setEcType(ec_type);
      meta_core_.registerStripe(std::move(stripe_meta_record));
      // distribute the stripe data
      auto workers = meta_core_.pg_to_worker_nodes(pg_id);
      auto distIpList = std::vector<meta::ip_t>{};
      std::transform(workers.cbegin(),
                     workers.cend(),
                     std::back_inserter(distIpList),
                     [&meta_core_ = this->meta_core_](auto worker_id) {
                       return meta_core_.worker_ip(worker_id);
                     });
      std::vector<meta::disk_id_t> diskList = meta_core_.pg_to_disks(pg_id);
      for (std::size_t i = 0; i < stripe.size(); i++) {
        std::string listName =
            comm::make_list_name(stripe_id, i, stripe.at(i).size());
        const auto &chunk = stripe.at(i);
        comm_.push_to(distIpList.at(i), listName, chunk);
        total_size += chunk.size();
        auto cmd = BlockCommand();
        cmd.buildType2(boost::numeric_cast<BlockCommand::block_id_t>(i),
                       stripe_id,
                       diskList.at(i),
                       {distIpList.at(i)},
                       {boost::numeric_cast<BlockCommand::block_id_t>(i)},
                       0,
                       stripe.at(i).size(),
                       profile.ec_k,
                       profile.ec_m);
        comm_.push_to(distIpList.at(i), cmd);
        // LOG(INFO) << fmt::format(
        //                  "build stripe {} chunk {}, size {},to {}, ec_type:
        //                  {}", stripe_id, i, stripe.at(i).size(),
        //                  distIpList.at(i),
        //                  ec_type)
        //           << std::endl;
      }
      for (std::size_t i = 0; i < (profile.ec_k + profile.ec_m); i++) {
        auto ack = comm_.pop_from(distIpList.at(i), comm::BUILD_ACK_LIST_KEY);
        if (ack.as_cstr() != comm::ACK_PAYLOAD) {
          LOG(ERROR) << fmt::format("ack error: {}", ack.as_cstr())
                     << std::endl;
        }
        // LOG(INFO) << fmt::format("ack received from: {}", distIpList.at(i))
        //           << std::endl;
      }
    };
    future_queue.emplace(task_pool.submit_task(task));
    if (profile.load_type == LoadType::ByStripe) {
      load_cnt++;
    } else {
      load_cnt += stripe_size;
    }
    wait_ack();
    constexpr std::size_t LOG_INTERVAL = 100;
    if (load_cnt % LOG_INTERVAL == 0) {
      LOG(INFO) << fmt::format("stripe num: {}; cur size: {}GB;",
                               stripe_cnt,
                               total_size >> 30) // NOLINT
                << std::endl;
    }
  }
  LOG(INFO) << fmt::format("stripe num: {}; cur size: {}GB;",
                           stripe_cnt,
                           total_size >> 30) // NOLINT
            << std::endl;
  wait_ack(0);
  LOG(INFO) << "All ack received" << std::endl;
  google::FlushLogFiles(google::GLOG_INFO);
  task_pool.wait();
  return {.stripe_stat = std::move(stripe_stat),
          .stripe_range = {profile.start_at, profile.start_at + stripe_cnt},
          .total_size = total_size};
}

auto coord::Coordinator::repair_chunk() -> void {
  err::Unreachable();
  auto &profile = *this->profile_.get();
  auto [sink, stream] = util::make_channel<meta::node_id_t, false>();
  auto ack_receiver =
      std::thread([&comm = comm_, stream, &meta_core = this->meta_core_]() {
        while (true) {
          auto node_id = meta::node_id_t{};
          stream >> node_id;
          auto ip = meta_core.worker_ip(node_id);
          auto ack = comm.pop_from(ip, comm::REPAIR_ACK_LIST_KEY);
          if (ack.as_cstr() != comm::ACK_PAYLOAD) {
            LOG(ERROR) << fmt::format("ack error: {}", ack.as_cstr())
                       << std::endl;
          }
        }
      });
  for (meta::stripe_id_t stripe_id = profile.start_at;
       stripe_id < profile.start_at + profile.test_load;
       stripe_id++) {
    // the necessary meta data for repair
    auto failed_chunk = meta::chunk_id_t{
        .stripe_id = stripe_id,
        .chunk_index = profile.chunk_repair_profile().chunk_index};
    auto stripe_meta = meta_core_.chunkRepair(failed_chunk);
    auto node_id =
        meta_core_.pg_to_worker_nodes(meta_core_.select_pg(stripe_id))
            .at(failed_chunk.chunk_index);
    auto ack_ip = RepairChunk{
        .stripe_meta = std::move(stripe_meta),
        .failed_chunk = failed_chunk,
        .meta_core_ref = std::ref(meta_core_),
        .comm_ref = std::ref(comm_),
    }(profile.chunk_repair_profile().manner);
    auto payload = comm_.pop_from(ack_ip, comm::REPAIR_ACK_LIST_KEY);
    if (payload.as_cstr() != comm::ACK_PAYLOAD) {
      LOG(ERROR) << fmt::format("ack error: {}", payload.as_cstr())
                 << std::endl;
    }
    sink << node_id;
  }
  ack_receiver.join();
}
auto coord::Coordinator::repair_failure_domain() -> RepairResult {
  meta::disk_id_t disk_id =
      profile_->failure_domain_repair_profile().failed_disk;
  auto repair_meta = meta_core_.diskRepair(disk_id);
  BS::thread_pool task_pool{};
  std::queue<std::future<void>> future_queue{};
  /// wait for the future queue to be the threshold
  constexpr std::size_t QUEUE_THRESHOLD = 32;
  auto wait_future = [&future_queue](std::size_t threshold = QUEUE_THRESHOLD) {
    while (future_queue.size() > threshold) {
      try {
        future_queue.front().wait();
      } catch (std::exception &e) {
        LOG(ERROR) << fmt::format("Exception caught: {}", e.what())
                   << std::endl;
      }
      future_queue.pop();
    }
  };
  std::atomic<std::size_t> total_size{0};
  for (auto const &repair : repair_meta) {
    const auto pg = repair.pg;
    const auto pg_id = pg.pg_id;
    const auto chunk_index = repair.chunk_index;
    const auto &diskList = meta_core_.pg_to_disks(pg.pg_id);
    const auto &stripes = repair.stripe_list;
    for (auto stripe_id : stripes) {
      auto task = [this, stripe_id, chunk_index, pg_id, &total_size]() {
        auto failed_chunk = meta::chunk_id_t{.stripe_id = stripe_id,
                                             .chunk_index = chunk_index};
        auto stripe_repair = meta_core_.chunkRepair(failed_chunk);
        auto node_id = meta_core_.pg_to_worker_nodes(pg_id).at(chunk_index);
        auto ack_ip = RepairChunk{
            .stripe_meta = std::move(stripe_repair),
            .failed_chunk = failed_chunk,
            .meta_core_ref = std::ref(meta_core_),
            .comm_ref = std::ref(comm_),
        }(RepairManner::Centralized);
        auto ack = comm_.pop_from(ack_ip, comm::REPAIR_ACK_LIST_KEY);
        if (ack.as_cstr() != comm::ACK_PAYLOAD) {
          LOG(ERROR) << fmt::format("ack error: {}", ack.as_cstr())
                     << std::endl;
        }
        total_size += stripe_repair.chunk_size;
        // comm_.pop_from(const std::string_view host, const std::string_view
        // key);
      };
      future_queue.emplace(task_pool.submit_task(task));
      wait_future();
    }
  };
  wait_future(0);
  task_pool.wait();
  if (!future_queue.empty()) {
    err::Unreachable();
  }
  return {.total_size = total_size};
}
auto coord::Coordinator::read() -> ReadResult {
  std::atomic<std::size_t> total_size{0};
  BS::thread_pool task_pool{};
  std::queue<std::future<void>> future_queue{};
  /// wait for the future queue to be the threshold
  constexpr std::size_t QUEUE_THRESHOLD = 32;
  auto wait_future = [&future_queue](std::size_t threshold = QUEUE_THRESHOLD) {
    while (future_queue.size() > threshold) {
      try {
        future_queue.front().wait();
      } catch (std::exception &e) {
        LOG(ERROR) << fmt::format("Exception caught: {}", e.what())
                   << std::endl;
      }
      future_queue.pop();
    }
  };
  // cache the last stripe to avoid reading the same stripe repeatedly
  auto locality_stripe = std::shared_ptr<const meta::StripeMeta>{};
  auto blob_opt = meta_core_.next_blobs_record();
  while (blob_opt.has_value()) {
    auto blob_meta = meta::BlobMeta{};
    LOG(INFO) << fmt::format("reading blob id: {}", blob_opt.value())
              << std::endl;
    auto stripe_meta_ref = std::shared_ptr<const meta::StripeMeta>{};
    try {
      blob_meta = meta_core_.blob_meta(blob_opt.value());
    } catch (meta::NotFound &e) {
      LOG(WARNING) << fmt::format("blob {} not found", blob_opt.value())
                   << std::endl;
      return {.total_size = total_size.load()};
    } catch (std::exception &e) {
      LOG(ERROR) << fmt::format("Exception caught: {}", e.what()) << std::endl;
      return {.total_size = total_size.load()};
    }
    blob_opt = meta_core_.next_blobs_record();
    if (locality_stripe != nullptr &&
        locality_stripe->stripe_id == blob_meta.stripe_id) {
      // this blob is in the same stripe as the previous one
      stripe_meta_ref = locality_stripe;
    } else {
      stripe_meta_ref = std::make_shared<meta::StripeMeta>(
          meta_core_.stripe_meta(blob_meta.stripe_id));
      locality_stripe = stripe_meta_ref;
    }
    auto task = [blob_meta,
                 stripe_meta_ref,
                 &total_size,
                 &meta_core_ = this->meta_core_,
                 &comm_ = this->comm_] {
      auto ack_list = ReadBlob{
          .blob_meta = blob_meta,
          .stripe_meta_ref = stripe_meta_ref,
          .meta_core_ref = std::ref(meta_core_),
          .comm_ref = std::ref(comm_),
      }();
      for (const auto &ip : ack_list) {
        auto ack = comm_.pop_from(ip, comm::READ_ACK_LIST_KEY);
        if (ack.as_cstr() != comm::ACK_PAYLOAD) {
          LOG(ERROR) << fmt::format("ack error: {}", ack.as_cstr())
                     << std::endl;
        }
      }
      total_size += blob_meta.size;
    };
    future_queue.emplace(task_pool.submit_task(task));
    wait_future();
  }
  wait_future(0);
  return {.total_size = total_size.load()};
}
auto coord::Coordinator::degrade_read() -> ReadResult {
  std::atomic<std::size_t> total_size{0};
  BS::thread_pool task_pool{};
  std::queue<std::future<void>> future_queue{};
  /// wait for the future queue to be the threshold
  constexpr std::size_t QUEUE_THRESHOLD = 32;
  auto wait_future = [&future_queue](std::size_t threshold = QUEUE_THRESHOLD) {
    while (future_queue.size() > threshold) {
      try {
        future_queue.front().wait();
      } catch (std::exception &e) {
        LOG(ERROR) << fmt::format("Exception caught: {}", e.what())
                   << std::endl;
      }
      future_queue.pop();
    }
  };
  // cache the last stripe to avoid reading the same stripe repeatedly
  auto locality_stripe = std::shared_ptr<const meta::StripeMeta>{};
  auto blob_opt = meta_core_.next_blobs_record();
  while (blob_opt.has_value()) {
    auto blob_meta = meta::BlobMeta{};
    LOG(INFO) << fmt::format("reading blob id: {}", blob_opt.value())
              << std::endl;
    auto stripe_meta_ref = std::shared_ptr<const meta::StripeMeta>{};
    try {
      blob_meta = meta_core_.blob_meta(blob_opt.value());
    } catch (meta::NotFound &e) {
      LOG(WARNING) << fmt::format("blob {} not found", blob_opt.value())
                   << std::endl;
      return {.total_size = total_size.load()};
    } catch (std::exception &e) {
      LOG(ERROR) << fmt::format("Exception caught: {}", e.what()) << std::endl;
      return {.total_size = total_size.load()};
    }
    blob_opt = meta_core_.next_blobs_record();
    if (locality_stripe != nullptr &&
        locality_stripe->stripe_id == blob_meta.stripe_id) {
      // this blob is in the same stripe as the previous one
      stripe_meta_ref = locality_stripe;
    } else {
      stripe_meta_ref = std::make_shared<meta::StripeMeta>(
          meta_core_.stripe_meta(blob_meta.stripe_id));
      locality_stripe = stripe_meta_ref;
    }
    auto task = [blob_meta,
                 stripe_meta_ref,
                 &total_size,
                 &meta_core_ = this->meta_core_,
                 &comm_ = this->comm_] {
      auto ack_list = DegradeReadBlob{
          .blob_meta = blob_meta,
          .stripe_meta_ref = stripe_meta_ref,
          .meta_core_ref = std::ref(meta_core_),
          .comm_ref = std::ref(comm_),
      }();
      for (const auto &[ip, ack_list_name] : ack_list) {
        auto ack = comm_.pop_from(ip, ack_list_name);
        if (ack.as_cstr() != comm::ACK_PAYLOAD) {
          LOG(ERROR) << fmt::format("ack error: {}", ack.as_cstr())
                     << std::endl;
        }
      }
      total_size += blob_meta.size;
    };
    future_queue.emplace(task_pool.submit_task(task));
    wait_future();
  }
  wait_future(0);
  return {.total_size = total_size.load()};
}
auto coord::Coordinator::persist() -> void { this->meta_core_.persist(); }
auto coord::Coordinator::load_meta() -> void { this->meta_core_.load_meta(); }
auto coord::Coordinator::clear_meta() -> void {
  this->meta_core_.clear_blobs();
}
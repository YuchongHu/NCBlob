#include "worker_core.hh"

#include "BlockCommand.hh"
#include "Command.hh"
#include "comm.hh"
#include "erasure_code.hh"
#include "erasure_code_factory.hpp"
#include "erasure_code_intf.hpp"
#include "exception.hpp"
#include "meta.hpp"
#include "shared_vec.hpp"
#include "store_core.hpp"
#include "toml11/find.hpp"
#include "toml11/parser.hpp"
#include <chrono>
#include <cstddef>
#include <exception>
#include <fmt/format.h>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
auto make_block_key(meta::stripe_id_t stripe_id) -> std::size_t {
  // auto name = "block_" + std::to_string(stripe_id);
  auto name = fmt::format("block_{}", stripe_id);
  return std::hash<std::string>{}(name);
}

auto make_chunk_name(meta::stripe_id_t stripe_id,
                     Command::shard_id_t shard_id) -> std::string {
  return fmt::format("{}_{}", stripe_id, shard_id);
}

using comm::make_list_name;
using comm::make_subchunk_list_name;

#pragma optimize("", off)
using namespace std::chrono_literals;
auto sim_hdd(std::chrono::milliseconds ms = 0ms) -> void {
  auto epoch = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - epoch < ms) {
    // do nothing
  }
}
#pragma optimize("", on)
} // namespace

auto worker::Profile::ParseToml(const std::string &path) -> Profile {
  auto profile = Profile{};
  auto data = toml::parse(path, toml::spec::v(1, 1, 0));
  profile.workspace_name = toml::find<std::string>(data, "workspace_name");
  profile.working_dir =
      std::filesystem::path{toml::find<std::string>(data, "working_dir")};
  // postfix with workspace name
  profile.working_dir /= profile.workspace_name;
  profile.create_new = toml::find<bool>(data, "create_new");
  profile.ip = toml::find_or<std::string>(data, "ip", "127.0.0.1");
  profile.num_threads = toml::find_or<BS::concurrency_t>(
      data, "num_threads", std::thread::hardware_concurrency());
  if (profile.num_threads == 0) {
    profile.num_threads = std::thread::hardware_concurrency();
  }
  profile.block = toml::find<bool>(data, "do_block");
  profile.cache_size = toml::find_or<std::size_t>(data, "cache_size", 0);
  profile.cache_size <<= 20; // bytes to megabytes // NOLINT
  profile.large_chunk_size =
      toml::find_or<std::size_t>(data, "large_chunk_size", 0);
  return profile;
}
auto worker::operator<<(std::ostream &os,
                        const Profile &profile) -> std::ostream & {
  using fmt::format;
  // clang-format off
  os << "[Info] Worker Profile:\n";
  os << format("\tworkspace: {}\n", profile.workspace_name);
  os << format("\tip: {}\n", profile.ip);
  os << format("\tworking dir: {}\n", profile.working_dir.generic_string());
  os << format("\tcreate new dir: {}\n", profile.create_new);
  os << format("\tblock worker: {}\n", profile.block);
  os << format("\tcache size (in MB): {}\n", profile.cache_size >> 20); // NOLINT
  os << format("\tlarge chunk size (in MB): {}\n", profile.large_chunk_size >> 20); // NOLINT
  os << format("\tthread number: {}\n", profile.num_threads);
  os << std::flush;
  // clang-format on
  return os;
}

worker::WorkerCtx::WorkerCtx(worker::ProfileRef profile)
    : profile_(profile), comm_(profile->workspace_name),
      thread_pool_(profile_->num_threads), WorkInterface() {};
auto worker::WorkerCtx::getProfile() const -> const worker::Profile & {
  return *profile_;
}
auto worker::WorkerCtx::detachTask(std::function<void()> &&task) -> void {
  this->thread_pool_.detach_task(std::move(task));
}
auto worker::WorkerCtx::getComm() -> comm::CommManager & { return comm_; }

auto worker::BlockWorkerCtx::run() -> void {
  while (true) {
    // std::string listName = "blockCommand";
    // auto localCtx = RedisUtil::CreateContext("127.0.0.1");
    // auto content = RedisUtil::blpopContent(localCtx.get(), listName.c_str());
    auto conn = getComm().get_connection(comm::LOCAL_HOST);
    auto content = conn->pop(comm::BLK_CMD_LIST_KEY.data());
    command_t bCmd{std::span{content.data(), content.size()}};
    command_ref cmd = to_const_shared(bCmd);
    switch (bCmd.getCommandType()) {
    case READANDCACHEBLOCK:
      // readAndCacheBlock(bCmd, std::move(ctx));
      {
        const auto &stripeId = cmd->getStripeId();
        const auto &blockId = cmd->getBlockId();
        const auto size = cmd->getSize();
        // std::cout << fmt::format("read and cache: stripeId: {}, blockId: {},
        // "
        //                          "size: {}",
        //                          stripeId,
        //                          blockId,
        //                          size)
        //           << std::endl;
      }
      pipe_read_cache(std::move(cmd));
      break;
    case READANDCACHEBLOCKCLAY: {
      const auto &stripeId = cmd->getStripeId();
      const auto &blockId = cmd->getBlockId();
      const auto &clayOffsetList = cmd->getClayOffsetList();
      const auto size = cmd->getSize();
      // std::cout << fmt::format("read and cache clay: stripeId: {}, blockId: "
      //                          "{}, size: {}",
      //                          stripeId,
      //                          blockId,
      //                          size)
      //           << std::endl;
    }
      pipe_read_cache_clay(std::move(cmd));
      break;
    case FETCHANDCOMPUTEANDWRITEBLOCK: {
      const auto &stripeId = cmd->getStripeId();
      const auto &blockId = cmd->getBlockId();
      const auto size = cmd->getSize();
      // std::cout << fmt::format("repaired write to disk: stripeId: {},
      // blockId: "
      //                          "{}, size: {}",
      //                          stripeId,
      //                          blockId,
      //                          size)
      //           << std::endl;
    }
      pipe_fetch_compute_write(std::move(cmd));
      break;
    case FETCH_WRITE_BLOCK: {
      const auto &stripeId = cmd->getStripeId();
      const auto &blockId = cmd->getBlockId();
      const auto size = cmd->getSize();
      // std::cout << fmt::format(
      //                  " write to disk: stripeId: {}, blockId: {}, size: {}",
      //                  stripeId,
      //                  blockId,
      //                  size)
      //           << std::endl;
    }
      pipe_fetch_write(std::move(cmd));
      break;
    default:
      throw std::runtime_error("Unknown command type");
      break;
    }
  }
}

auto worker::BlockWorkerCtx::doRead(const command_t &cmd,
                                    bytes_sink sink) -> void {
  auto stripeId = cmd.getStripeId();
  auto offset = cmd.getOffset();
  auto size = cmd.getSize();
  auto block_key = make_block_key(stripeId);
  auto content = util::SharedVec::with_size(size);
  try {
    using namespace std::chrono_literals;
    auto chunk_size = getStore().blob_size(block_key);
    sim_hdd();
    getStore().get_offset(block_key, content.as_bytes(), offset);
  } catch (std::exception &e) {
    std::cerr << fmt::format("read stripe:{}-{} failed, what: {}",
                             stripeId,
                             cmd.getBlockId(),
                             e.what())
              << std::endl;
    throw;
  }
  std::cout << fmt::format("read stripe:{}-{} success, size {}",
                           stripeId,
                           cmd.getBlockId(),
                           content.size())
            << std::endl;
  sink << content;
};
auto worker::BlockWorkerCtx::doCache(const command_t &cmd,
                                     bytes_stream stream) -> void {
  // auto localCtx = RedisUtil::CreateContext("127.0.0.1");
  auto blockId = cmd.getBlockId();
  // auto offset = cmd.getOffset();
  // auto size = cmd.getSize();
  auto stripeId = cmd.getStripeId();
  // auto listName = "stripeid_" + std::to_string(stripeId) + "blockid_" +
  //                 std::to_string(blockId);
  auto listName = make_list_name(stripeId, blockId, cmd.getSize());
  auto content = bytes_t{};
  stream >> content;
  getComm().push_to(comm::LOCAL_HOST, listName.c_str(), content.as_cbytes());
};
auto worker::BlockWorkerCtx::doReadClay(const command_t &cmd,
                                        bytes_sink sink) -> void {
  auto stripeId = cmd.getStripeId();
  auto blockId = cmd.getBlockId();
  auto &clayOffsetList = cmd.getClayOffsetList();
  auto size = cmd.getSize();
  auto listName = make_list_name(stripeId, blockId, cmd.getSize());
  auto content = bytes_t::with_size(clayOffsetList.size() * size);
  auto blockKey = make_block_key(stripeId);
  auto content_off = content.data();
  auto blob_size = getStore().blob_size(blockKey);
  for (auto offset : clayOffsetList) {
    sim_hdd();
    getStore().get_offset(blockKey, {content_off, size}, offset);
    std::advance(content_off, size);
  }
  sink << content;
};
auto worker::BlockWorkerCtx::doPush(const command_t &cmd,
                                    bytes_stream stream) -> void {
  err::Unreachable();
  auto size = cmd.getSize();
  auto content = bytes_t{};
  auto stripeId = cmd.getStripeId();
  auto blockId = cmd.getBlockId();
  auto listName = make_list_name(stripeId, blockId, 0);
  stream >> content;
  getComm().push_to(comm::LOCAL_HOST, listName.c_str(), content.as_cbytes());
};
auto worker::BlockWorkerCtx::doCompute(const command_t &cmd,
                                       bytes_stream stream,
                                       bytes_sink sink) -> void {
  using namespace ec;

  auto blockId = cmd.getBlockId();
  auto blockNum = cmd.getBlockNum();
  // auto dataList = std::vector<std::vector<char>>();
  std::map<int, bytes_t> dataList{};
  // prepare dataList
  for (int i = 0; i < blockNum; i++) {
    // auto content = fetchQueue->pop();
    auto content = bytes_t{};
    stream >> content;
    // @Edgar FIX: narrowing conversion from 'long' to 'int' [-Wnarrowing]
    // consider using auto or a cast
    int srcBlockId = (int)(cmd._srcBlockIdList[i]);
    dataList[srcBlockId] = content;
  }

  std::ostringstream errors;
  ec::ErasureCodeProfile profile;
  int k = cmd._k;
  int m = cmd._m;
  profile["k"] = std::to_string(k);
  profile["m"] = std::to_string(m);

  std::map<int, ec::bufferlist> decoded;

  int repairIdx = cmd.getDestBlockId();
  int computeType = cmd.getComputeType();

  if (computeType == cmd.CLAY_REPAIR) {
    ec::ErasureCodeInterfaceRef intf =
        ec::ErasureCodeClayFactory{}.make(profile, errors);
    auto &clay = dynamic_cast<ec::ErasureCode &>(*intf.get());

    std::set<int> want_to_read;
    want_to_read.insert(repairIdx);

    // for Clay, don't need to merge the matrix_row before chunk data
    std::map<int, ec::bufferlist> chunks;
    for (const auto &p : dataList) {
      auto srcBlockId = p.first;
      auto content = p.second;
      ec::bufferlist in;
      ec::bufferptr in_ptr(ceph::buffer::create_page_aligned(content.size()));
      in_ptr.zero();
      in_ptr.set_length(0);
      auto as_str = content.as_cstr();
      in_ptr.append(as_str.data(), as_str.size());
      in.push_back(in_ptr);
      chunks[srcBlockId] = in;
    }

    std::set<int> available;
    for (int i = 0; i < k + m; i++) {
      if (i != repairIdx)
        available.insert(i);
    }

    std::map<int, std::vector<std::pair<int, int>>> minimum;
    clay.minimum_to_decode(want_to_read, available, &minimum);

    int repair_sub_chunk_count = 0;
    for (const auto &p : minimum.begin()->second) {
      repair_sub_chunk_count += p.second;
    }
    std::cout << "repair_sub_chunk_count:" << repair_sub_chunk_count
              << std::endl;

    int times = clay.get_sub_chunk_count() / repair_sub_chunk_count;
    int chunk_size = chunks.begin()->second.length() * times;

    // chunks.size() == k+m-1
    assert(chunks.size() == k + m - 1);
    if (clay.decode(want_to_read, chunks, &decoded, chunk_size) != 0) {
      std::cerr << "Clay repair error" << std::endl;
    }
    // decoded.size() == 1

    assert(decoded.begin()->second.length() == chunk_size);

  } else if (computeType == cmd.RS_REPAIR) {
    ec::ErasureCodeInterfaceRef intf =
        ec::ErasureCodeJerasureFactory{}.make(profile, errors);
    auto &jerasure = dynamic_cast<ec::ErasureCode &>(*intf.get());

    std::set<int> want_to_read;
    want_to_read.insert(repairIdx);

    // for RS, don't need to merge the matrix_row before chunk data
    std::map<int, bufferlist> chunks;
    for (const auto &p : dataList) {
      auto srcBlockId = p.first;
      auto content = p.second;
      bufferlist in;
      bufferptr in_ptr(ceph::buffer::create_page_aligned(content.size()));
      in_ptr.zero();
      in_ptr.set_length(0);
      auto as_str = content.as_cstr();
      in_ptr.append(as_str.data(), as_str.size());
      in.push_back(in_ptr);
      chunks[srcBlockId] = in;
    }

    assert(chunks.size() == k);

    if (jerasure._decode(want_to_read, chunks, &decoded) != 0) {
      std::cerr << "RS repair error" << std::endl;
    }

  } else if (computeType == cmd.NSYS_REPAIR) {
    // NSYS repair
    ErasureCodeInterfaceRef intf =
        ErasureCodeLonseFactory{}.make(profile, errors);
    auto &Lonse = dynamic_cast<ErasureCode &>(*intf.get());

    std::set<int> want_to_read;
    want_to_read.insert(repairIdx);

    // TODO:for NSYS, the matrix_row is before the chunk data
    std::map<int, bufferlist> chunks;
    for (const auto &p : dataList) {
      auto srcBlockId = p.first;
      auto content = p.second;
      bufferlist in;
      bufferptr in_ptr(ceph::buffer::create_page_aligned(content.size()));
      in_ptr.zero();
      in_ptr.set_length(0);
      auto as_str = content.as_cstr();
      in_ptr.append(as_str.data(), as_str.size());
      in.push_back(in_ptr);
      chunks[srcBlockId] = in;
    }
    int chunk_size = chunks.begin()->second.length() * m;
    assert(chunks.size() == k + m - 1);
    if (Lonse.decode(want_to_read, chunks, &decoded, chunk_size) != 0) {
      std::cerr << "NSYS repair error" << std::endl;
    }
    assert(decoded.begin()->second.length() == chunk_size);
    for (int i = 0; i < decoded.size(); i++) {
      std::cout << "decoded[" << i << "].size: " << decoded[i].length()
                << std::endl;
    }
  } else if (computeType == cmd.NSYS_READ) {
    // std::cerr << "SKIP NSYS_READ" << std::endl;
    // return;
    // NSYS degraded read, the same as normal read
    ErasureCodeInterfaceRef intf =
        ErasureCodeLonseFactory{}.make(profile, errors);
    auto &Lonse = dynamic_cast<ErasureCode &>(*intf.get());

    std::set<int> want_to_read;

    // TODO:for NSYS, the matrix_row is before the chunk data
    std::map<int, bufferlist> chunks;
    for (const auto &p : dataList) {
      auto srcBlockId = p.first;
      want_to_read.insert(p.first);
      auto content = p.second;
      bufferlist in;
      bufferptr in_ptr(ceph::buffer::create_page_aligned(content.size()));
      in_ptr.zero();
      in_ptr.set_length(0);
      auto as_str = content.as_cstr();
      in_ptr.append(as_str.data(), as_str.size());
      in.push_back(in_ptr);
      chunks[srcBlockId] = in;
    }

    // chunk size = * m ???
    int chunk_size = chunks.begin()->second.length();

    // dataList.size() == k
    assert(dataList.size() == k);

    if (Lonse.decode(want_to_read, chunks, &decoded, chunk_size) != 0) {
      std::cerr << "NSYS degraded read error" << std::endl;
      throw std::runtime_error("NSYS degraded read error");
    }
    // decoded.size() == k

    // assert(decoded.begin()->second.length() == chunk_size);

  } else {
    // throw std::runtime_error("Unknown compute type");
    return;
  }
  // callback

  if (computeType == cmd.CLAY_REPAIR || computeType == cmd.RS_REPAIR ||
      computeType == cmd.NSYS_REPAIR) {
    // callback the one lost chunk
    bufferlist &contentBl = decoded[repairIdx];
    auto content = bytes_t::from_str({contentBl.c_str(), contentBl.length()});
    sink << content;
  }
}
auto worker::BlockWorkerCtx::doFetch(const command_t &cmd,
                                     bytes_sink sink) -> void {
  auto stripeId = cmd.getStripeId();
  auto srcIpList = cmd.getSrcIpList();
  auto srcBlockIdList = cmd.getSrcBlockIdList();
  for (int i = 0; i < srcIpList.size(); i++) {
    // auto listName = "stripeid_" + std::to_string(stripeId) + "blockid_" +
    //                 std::to_string(srcBlockIdList.at(i));
    auto listName =
        make_list_name(stripeId, srcBlockIdList.at(i), cmd.getSize());
    auto content = getComm().pop_from(srcIpList[i], listName);
    std::cout << fmt::format("fetching from {}: {}, size: {}",
                             srcIpList[i],
                             listName,
                             content.size())
              << std::endl;
    sink << content;
  }
};
auto worker::BlockWorkerCtx::doWrite(const command_t &cmd,
                                     bytes_stream stream) -> void {
  auto stripeId = cmd.getStripeId();
  auto blockKey = make_block_key(stripeId);
  auto content = bytes_t{};
  stream >> content;
  // std::cout << fmt::format(
  //                  "writing to block: {}, size: {}", stripeId,
  //                  content.size())
  //           << std::endl;
  // sim_hdd();
  this->getStore().put_or_create(blockKey, content.as_cbytes());
};
auto worker::BlockWorkerCtx::pipe_read_cache(command_ref cmd) -> void {
  auto [sink, stream] = make_bytes_channel();
  this->detachTask([this, cmd, sink = std::move(sink)]() {
    this->doRead(*cmd.get(), sink);
  });
  this->detachTask([this, cmd, stream = std::move(stream)]() {
    this->doCache(*cmd.get(), stream);
  });
};
auto worker::BlockWorkerCtx::pipe_fetch_write(command_ref cmd) -> void {
  auto [sink, stream] = make_bytes_channel();
  this->detachTask([this, cmd, sink = std::move(sink)]() {
    this->doFetch(*cmd.get(), sink);
  });
  this->detachTask([this, cmd, stream = std::move(stream)]() {
    this->doWrite(*cmd.get(), stream);
    this->getComm().push_to(
        comm::LOCAL_HOST, comm::BUILD_ACK_LIST_KEY, comm::ACK_PAYLOAD);
  });
};
auto worker::BlockWorkerCtx::pipe_read_cache_clay(command_ref cmd) -> void {
  auto [sink, stream] = make_bytes_channel();
  detachTask(
      [this, cmd, sink = std::move(sink)]() { doReadClay(*cmd.get(), sink); });
  detachTask([this, cmd, stream = std::move(stream)]() {
    doCache(*cmd.get(), stream);
  });
};
auto worker::BlockWorkerCtx::pipe_fetch_compute_write(command_ref cmd) -> void {
  auto [sink_to_compute, stream_from_fetch] = make_bytes_channel();
  auto [sink_to_cache, stream_from_compute] = make_bytes_channel();

  bool perform_read = cmd->getComputeType() == command_t::NSYS_READ ||
                      cmd->getComputeType() == command_t::CLAY_READ;

  detachTask([this, cmd, sink = std::move(sink_to_compute)]() {
    doFetch(*cmd.get(), sink);
  });
  detachTask([this,
              cmd,
              stream = std::move(stream_from_fetch),
              sink = std::move(sink_to_cache),
              perform_read]() {
    doCompute(*cmd.get(), stream, sink);
    if (perform_read) {
      // read and degrade read
      this->getComm().push_to(
          comm::LOCAL_HOST, comm::READ_ACK_LIST_KEY, comm::ACK_PAYLOAD);
    }
  });

  if (!perform_read) {
    // repair
    detachTask([this, cmd, stream = std::move(stream_from_compute)]() {
      doWrite(*cmd.get(), stream);
      // std::cout
      //     << fmt::format(
      //            "[{}] sending ack to {}",
      //            std::chrono::system_clock::now().time_since_epoch().count(),
      //            comm::LOCAL_HOST)
      //     << std::endl;
      this->getComm().push_to(
          comm::LOCAL_HOST, comm::REPAIR_ACK_LIST_KEY, comm::ACK_PAYLOAD);
    });
  }
};
// worker::BlockWorkerCtx::BlockWorkerCtx(worker::ProfileRef profile)
//     : store_(profile->working_dir, profile->cache_size), WorkerCtx(profile) {
//   store_.set_bypass_threshold(profile->large_chunk_size);
// }
worker::BlockWorkerCtx::BlockWorkerCtx(worker::ProfileRef profile)
    : store_(profile->working_dir), WorkerCtx(profile) {
  // store_.set_bypass_threshold(profile->large_chunk_size);
}
worker::SlicedWorkerCtx::SlicedWorkerCtx(worker::ProfileRef profile)
    : store_(profile->working_dir, profile->cache_size), WorkerCtx(profile) {
  store_.set_bypass_threshold(profile->large_chunk_size);
};
auto worker::SlicedWorkerCtx::run() -> void {
  while (true) {
    auto content =
        getComm().pop_from(comm::LOCAL_HOST, comm::CMD_LIST_KEY.data());
    // command_t cmd{std::span{content.data(), content.size()}};
    auto cmd =
        to_const_shared(Command{std::span{content.data(), content.size()}});
    switch (cmd->getCommandType()) {
    case READANDCACHE:
      pipe_read_cache(std::move(cmd));
      break;
    case FETCHANDCOMPUTE:
      pipe_fetch_compute_cache(std::move(cmd));
      break;
    case CONCATENATE:
      pipe_cat_write(std::move(cmd));
      break;
    default:
      throw std::runtime_error("Unknown command type");
      break;
    }
  }
};
auto worker::SlicedWorkerCtx::pipe_read_cache(command_ref cmd) -> void {
  auto [sink, stream] = make_bytes_channel();
  detachTask(
      [this, cmd, sink = std::move(sink)]() { doRead(*cmd.get(), sink); });
  detachTask([this, cmd, stream = std::move(stream)]() {
    doCache(*cmd.get(), stream);
  });
};
auto worker::SlicedWorkerCtx::pipe_fetch_compute_cache(command_ref cmd)
    -> void {
  auto [sink_to_compute, stream_from_fetch] = make_bytes_channel();
  auto [sink_to_cache, stream_from_compute] = make_bytes_channel();
  detachTask([this, cmd, sink = std::move(sink_to_compute)]() {
    doFetch(*cmd.get(), sink);
  });
  detachTask([this,
              cmd,
              stream = std::move(stream_from_fetch),
              sink = std::move(sink_to_cache)]() {
    doCompute(*cmd.get(), stream, sink);
  });
  detachTask([this, cmd, stream = std::move(stream_from_compute)]() {
    doCache(*cmd.get(), stream);
  });
};
auto worker::SlicedWorkerCtx::pipe_cat_write(command_ref cmd) -> void {
  auto [sink, stream] = make_bytes_channel();
  detachTask(
      [this, cmd, sink = std::move(sink)]() { doFetch(*cmd.get(), sink); });
  detachTask([this, cmd, stream = std::move(stream)]() {
    doWrite(*cmd.get(), stream);
    this->getComm().push_to(
        comm::LOCAL_HOST, comm::REPAIR_ACK_LIST_KEY, comm::ACK_PAYLOAD);
  });
};
auto worker::SlicedWorkerCtx::doRead(const command_t &cmd,
                                     bytes_sink sink) -> void {
  auto stripeId = cmd.getStripeId();
  auto shardId = cmd.getShardId();
  auto chunkName = make_chunk_name(stripeId, shardId);
  auto chunkKey = std::hash<std::string>{}(chunkName);
  auto srcSubShardIdList = cmd.getSrcSubShardIdList();
  auto subChunkNum = cmd.getW();
  auto chunkSize = getStore().blob_size(chunkKey);
  auto subChunkSize = chunkSize / subChunkNum;

  const auto large_chunk_size = getProfile().large_chunk_size;
  auto bypass_store = store_.bypass_cache();
  store::StoreInterface &store =
      chunkSize < large_chunk_size ? getStore() : bypass_store;
  for (auto subShardId : srcSubShardIdList) {
    auto content = bytes_t::with_size(chunkSize);
    store.get_offset(chunkKey, content.as_bytes(), subShardId * subChunkSize);
    sink << content;
  }
  return;
};
auto worker::SlicedWorkerCtx::doCache(const command_t &cmd,
                                      bytes_stream stream) -> void {
  auto stripeId = cmd.getStripeId();
  auto shardId = cmd.getShardId();
  const auto &distSubShardIdList = cmd.getDistSubShardIdList();
  for (auto distSubShardId : distSubShardIdList) {
    auto listName = make_subchunk_list_name(stripeId, shardId, distSubShardId);
    auto content = bytes_t{};
    stream >> content;
    // cache to local redis
    getComm().push_to(comm::LOCAL_HOST, listName, content.as_cbytes());
  }
};
auto worker::SlicedWorkerCtx::doFetch(const command_t &cmd,
                                      bytes_sink sink) -> void {
  auto stripeId = cmd.getStripeId();
  const auto &shardIdList = cmd.getShardIdList();
  const auto &srcSubShardIdList = cmd.getSrcSubShardIdList();
  const auto &srcIpList = cmd.getSrcIpList();
  for (int i = 0; i < shardIdList.size(); i++) {
    auto listName =
        make_subchunk_list_name(stripeId, shardIdList[i], srcSubShardIdList[i]);
    auto content = getComm().pop_from(srcIpList.at(i), listName);
    sink << content;
  }
};
auto worker::SlicedWorkerCtx::doWrite(const command_t &cmd,
                                      bytes_stream stream) -> void {
  auto stripeId = cmd.getStripeId();
  auto shardId = cmd.getShardId();
  const auto &distSubShardIdList = cmd.getDistSubShardIdList();
  const auto &chunkName = make_chunk_name(stripeId, shardId);
  auto chunkKey = std::hash<std::string>{}(chunkName);
  auto subChunkNum = cmd.getW();
  auto chunkSize = getStore().blob_size(chunkKey);
  auto subChunkSize = chunkSize / subChunkNum;
  auto bypass = store_.bypass_cache();

  const auto large_chunk_size = getProfile().large_chunk_size;
  auto bypass_store = store_.bypass_cache();
  store::StoreInterface &store =
      chunkSize < large_chunk_size ? getStore() : bypass_store;

  for (auto distSubShardId : distSubShardIdList) {
    auto content = bytes_t{};
    stream >> content;
    store.put(chunkKey, content.as_cbytes(), subChunkSize * distSubShardId);
  }
};
auto worker::SlicedWorkerCtx::doCompute(const command_t &cmd,
                                        bytes_stream stream,
                                        bytes_sink sink) -> void {
  using namespace ec;
  using namespace ceph::buffer;

  err::Unimplemented();
}
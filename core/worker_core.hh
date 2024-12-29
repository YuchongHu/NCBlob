#pragma once

#include "BlockCommand.hh"
#include "Command.hh"
#include "channel.hpp"
#include "comm.hh"

#include "BS_thread_pool.hpp"
#include "shared_vec.hpp"
#include "store_core.hpp"

#include <boost/compute/detail/lru_cache.hpp>
#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <rados/buffer.h>

#include <cstddef>
#include <filesystem>
#include <fmt/format.h>
#include <memory>
#include <string>
#include <utility>

namespace worker {

using store_ref = store::StoreInterface &;
struct Profile {
  std::string workspace_name;
  std::filesystem::path working_dir;
  bool create_new;
  std::string ip;
  bool block;
  // in bytes
  std::size_t cache_size;
  BS::concurrency_t num_threads;
  std::size_t large_chunk_size;

  static auto ParseToml(const std::string &path) -> Profile;
};

auto operator<<(std::ostream &os, const Profile &profile) -> std::ostream &;

using ProfileRef = std::shared_ptr<const Profile>;

struct WorkInterface {
  WorkInterface() = default;
  virtual ~WorkInterface() = default;
  WorkInterface(const WorkInterface &) = default;
  auto operator=(const WorkInterface &) -> WorkInterface & = default;
  WorkInterface(WorkInterface &&) = default;
  auto operator=(WorkInterface &&) -> WorkInterface & = default;
  virtual auto run() -> void = 0;
};

using bytes_t = util::SharedVec;
using bytes_sink = util::ChannelSink<bytes_t>;
using bytes_stream = util::ChannelStream<bytes_t>;

inline auto make_bytes_channel() -> std::pair<bytes_sink, bytes_stream> {
  return util::make_channel<bytes_t, true>();
}

class WorkerCtx : virtual public WorkInterface {
private:
  worker::ProfileRef profile_;
  BS::thread_pool thread_pool_;
  comm::CommManager comm_;

protected:
  WorkerCtx(worker::ProfileRef profile);
  auto getProfile() const -> const worker::Profile &;
  auto detachTask(std::function<void()> &&task) -> void;
  auto getComm() -> comm::CommManager &;
  virtual auto getStore() -> store_ref = 0;
};

class BlockWorkerCtx : public WorkerCtx {
protected:
  auto getStore() -> store_ref override { return store_; };
  using command_t = BlockCommand;
  using command_ref = BlockCommandRef;

  auto doRead(const command_t &, bytes_sink sink) -> void;
  auto doCache(const command_t &, bytes_stream stream) -> void;
  auto doReadClay(const command_t &cmd, bytes_sink sink) -> void;
  auto doPush(const command_t &cmd, bytes_stream stream) -> void;
  auto doCompute(const command_t &cmd, bytes_stream stream,
                 bytes_sink sink) -> void;
  auto doFetch(const command_t &, bytes_sink sink) -> void;
  auto doWrite(const command_t &, bytes_stream stream) -> void;

  /// read data from local store and cache it to local redis
  auto pipe_read_cache(command_ref cmd) -> void;
  auto pipe_read_cache_clay(command_ref cmd) -> void;
  auto pipe_fetch_compute_write(command_ref cmd) -> void;
  auto pipe_fetch_write(command_ref cmd) -> void;

public:
  BlockWorkerCtx() = delete;
  BlockWorkerCtx(worker::ProfileRef profile);
  auto run() -> void override;

private:
  using store_t = store::LocalStore;
  store_t store_;
};

class [[deprecated]] SlicedWorkerCtx : public WorkerCtx {
protected:
  auto getStore() -> store_ref override { return store_; };
  using command_t = Command;
  using command_ref = CommandRef;

  auto doRead(const command_t &, bytes_sink sink) -> void;
  auto doCache(const command_t &, bytes_stream stream) -> void;
  // auto doReadClay(command_t cmd, bytes_channel_ref sink) -> void;
  // auto doPush(command_t cmd, bytes_channel_ref stream) -> void;
  // auto doCompute(command_t cmd, bytes_channel_ref stream,
  //                bytes_channel_ref sink) -> void;
  auto doCompute(const command_t &cmd, bytes_stream stream,
                 bytes_sink sink) -> void;
  auto doFetch(const command_t &, bytes_sink sink) -> void;
  auto doWrite(const command_t &, bytes_stream stream) -> void;

  auto pipe_read_cache(command_ref cmd) -> void;
  auto pipe_fetch_compute_cache(command_ref cmd) -> void;
  auto pipe_cat_write(command_ref cmd) -> void;

public:
  SlicedWorkerCtx() = delete;
  SlicedWorkerCtx(worker::ProfileRef profile);
  auto run() -> void override;

private:
  using store_t = store::CachedLocalStore;
  store_t store_;
};

} // namespace worker
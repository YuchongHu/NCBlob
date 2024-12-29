#pragma once

#include "BlockCommand.hh"
#include "Command.hh"
#include "config.hpp"
#include "shared_vec.hpp"
#include <cstddef>
#include <fmt/format.h>
#include <hiredis/hiredis.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
namespace comm {
static inline constexpr int DEFAULT_PORT{6379};
static inline constexpr std::string_view REPAIR_ACK_LIST_KEY{"_RP_L_ACK"};
static inline constexpr std::string_view BUILD_ACK_LIST_KEY{"_BD_L_ACK"};
static inline constexpr std::string_view READ_ACK_LIST_KEY{"_RD_L_ACK"};
static inline constexpr std::string_view CMD_LIST_KEY{"_LIST_CMD"};
static inline constexpr std::string_view BLK_CMD_LIST_KEY{"_LIST_BLK_CMD"};
static inline constexpr std::string_view ACK_PAYLOAD{"ACK"};
static inline constexpr std::string_view LOCAL_HOST{"127.0.0.1"};
// static inline constexpr std::string_view COORD_HOST{"192.168.0.186"};
inline auto make_list_name(meta::stripe_id_t stripe_id,
                           meta::chunk_index_t chunk_idx,
                           std::size_t size) -> std::string {

  // auto listName = "stripeid_" + std::to_string(stripeId) + "blockid_" +
  //                 std::to_string(srcBlockIdList.at(i));
  return fmt::format("stripeid_{}blockid_{}sz_{}", stripe_id, chunk_idx, size);
}
inline auto
make_subchunk_list_name(meta::stripe_id_t stripe_id,
                        Command::shard_id_t shard_id,
                        Command::sub_shard_id_t sub_shard_id) -> std::string {
  return fmt::format("{}_{}_{}", stripe_id, shard_id, sub_shard_id);
}
class CommException : public std::runtime_error {
public:
  CommException() : std::runtime_error("Communication Exception") {}
  explicit CommException(const std::string &msg) : std::runtime_error(msg) {}
  CommException(const CommException &) = default;
  CommException(CommException &&) = default;
  auto operator=(const CommException &) -> CommException & = default;
  auto operator=(CommException &&) -> CommException & = default;
  ~CommException() override = default;
};

class CommContext {
private:
  friend class CommManager;
  using redisContextPtr = std::unique_ptr<redisContext, decltype(&redisFree)>;
  using redisReplyPtr = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;
  using mtx_t = std::mutex;
  inline static std::string password_{"gc123456."}; // TODO: set by config
  redisContextPtr context_;
  mtx_t mtx_{};
  std::shared_ptr<std::string> workspace_name_{};

public:
  CommContext(std::shared_ptr<std::string> workspace_name,
              const std::string_view host, int port = DEFAULT_PORT)
      : context_(redisConnect(host.data(), port), redisFree),
        workspace_name_(std::move(workspace_name)) {
    if (context_ == nullptr || context_->err) {
      throw CommException("Failed to connect to redis server");
    }
    auto reply = redisReplyPtr{static_cast<redisReply *>(redisCommand( // NOLINT
                                   context_.get(),
                                   "AUTH %s",
                                   password_.c_str())),
                               freeReplyObject};
    if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
      throw CommException{"auth fail"};
    }
  }
  auto workspace_key(const std::string_view key) -> std::string {
    return fmt::format("{}_{}", *workspace_name_, key);
  }

public:
  CommContext() = delete;
  auto list_len(const std::string_view key) -> std::size_t {
    auto key_in_workspace = workspace_key(key);
    // auto lock = std::lock_guard<mtx_t>{mtx_};
    auto rReply = redisReplyPtr{
        static_cast<redisReply *>(redisCommand(this->context_.get(), // NOLINT
                                               "LLEN %s",
                                               key.data())),
        freeReplyObject};
    if (rReply == nullptr || rReply->type == REDIS_REPLY_ERROR) {
      throw CommException{rReply->str};
    }
    return rReply->integer;
  }
  auto pop(const std::string_view key) -> util::SharedVec {
    auto key_in_workspace = workspace_key(key);
    // std::cout << fmt::format("pop from {}", key_in_workspace) << std::endl;
    // auto lock = std::unique_lock<mtx_t>{mtx_};
    auto rReply =
        redisReplyPtr{static_cast<redisReply *>(redisCommand( // NOLINT
                          this->context_.get(),
                          "blpop %s 0",
                          key.data())),
                      freeReplyObject};
    // lock.unlock();
    // std::cout << fmt::format("popped from {}", key_in_workspace) <<
    // std::endl;
    if (rReply == nullptr || rReply->type == REDIS_REPLY_ERROR) {
      // failure
      throw CommException{rReply->str};
    }
    auto element = rReply->element[1]; // NOLINT
    auto retVal = util::SharedVec::from_str({element->str, element->len});
    return retVal;
  };

  auto push(const std::string_view key,
            std::span<const std::byte> data) -> void {
    if constexpr (config::ENABLE_TRAFFIC_CONTROL) {
      constexpr std::size_t MAX_LEN{512};
      while (list_len(key) > MAX_LEN) {
        std::this_thread::yield();
      }
    }
    auto key_in_workspace = workspace_key(key);
    // std::cout << fmt::format("push to {}", key_in_workspace) << std::endl;
    // auto lock = std::lock_guard<mtx_t>{mtx_};
    auto rReply = redisReplyPtr{
        static_cast<redisReply *>(redisCommand(this->context_.get(), // NOLINT
                                               "rpush %s %b",
                                               key.data(),
                                               data.data(),
                                               data.size())),
        freeReplyObject};
    if (rReply == nullptr || rReply->type == REDIS_REPLY_ERROR) {
      throw CommException{rReply->str};
    }
  }
};

class CommManager {
  std::shared_mutex mtx_{};
  std::unordered_map<std::string, std::shared_ptr<CommContext>> context_map_{};
  std::shared_ptr<std::string> workspace_name_{};

public:
  CommManager() = delete;
  CommManager(std::string_view workspace_name)
      : workspace_name_(std::make_shared<std::string>(workspace_name)) {}

  auto
  get_connection(const std::string_view host) -> std::shared_ptr<CommContext> {
    // auto lock = std::shared_lock<std::shared_mutex>{mtx_};
    // auto it = context_map_.find(std::string{host});
    // if (it != context_map_.end()) {
    //   return it->second;
    // } else {
    //   lock.unlock();
    //   auto exclusive_lock = std::unique_lock<std::shared_mutex>{mtx_};
    //   // Double-check if the context was created by another thread
    //   it = context_map_.find(std::string{host});
    //   if (it == context_map_.end()) {
    //     auto context = std::make_shared<CommContext>(workspace_name_, host);
    //     context_map_[std::string{host}] = context;
    //     return context;
    //   }
    //   // If context was created by another thread, return the existing one
    //   return it->second;
    // }
    auto context = std::make_shared<CommContext>(workspace_name_, host);

    return context;
  }

  [[nodiscard]] auto pop_from(const std::string_view host,
                              const std::string_view key) -> util::SharedVec {
    auto context = this->get_connection(host);
    return context->pop(key);
  }

  auto push_to(const std::string_view host, const std::string_view key,
               std::span<const std::byte> data) -> void {
    auto context = this->get_connection(host);
    context->push(key, data);
  }
  auto push_to(const std::string_view host, const std::string_view key,
               const std::vector<char> &data) -> void {
    push_to(host,
            key,
            {reinterpret_cast<const std::byte *>(data.data()), // NOLINT
             data.size()});
  }
  auto push_to(const std::string_view host, const std::string_view key,
               const std::string_view data) -> void {
    push_to(host,
            key,
            {reinterpret_cast<const std::byte *>(data.data()), // NOLINT
             data.size()});
  }
  auto push_to(const std::string_view host, const Command &cmd) -> void {
    std::string str = cmd.serialize();
    push_to(host, CMD_LIST_KEY, str);
  }
  auto push_to(const std::string_view host, const BlockCommand &cmd) -> void {
    std::string str = cmd.serialize();
    push_to(host, BLK_CMD_LIST_KEY, str);
  }
};
} // namespace comm
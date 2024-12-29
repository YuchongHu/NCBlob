/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-22 11:33:12
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-29 09:15:59
 * @FilePath: /tbr/protocol/Command.hh
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置
 * 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once

#include "meta.hpp"

#include <cstddef>
#include <cstdint>
#include <hiredis/hiredis.h>
#include <iostream>
#include <msgpack.hpp>
#include <string>
#include <string_view>

#define READANDCACHE 0
#define READANDCOMPUTE 1
#define FETCHANDCOMPUTE 2
#define CONCATENATE 3

class ClayComputeTask {
public:
  using sub_shard_id_t = std::int64_t;
  std::vector<sub_shard_id_t> _srclist;
  std::vector<sub_shard_id_t> _dstlist;
  std::vector<std::vector<int>> _matrix;

  ClayComputeTask(std::vector<sub_shard_id_t> srclist,
                  std::vector<sub_shard_id_t> dstlist,
                  std::vector<std::vector<int>> matrix)
      : _srclist(std::move(srclist)), _dstlist(std::move(dstlist)),
        _matrix(std::move(matrix)) {}

  ClayComputeTask() : _srclist({}), _dstlist({}), _matrix({}) {}

  void display() {
    std::cout << "srclist: ";
    for (auto i : _srclist) {
      std::cout << i << " ";
    }
    std::cout << std::endl;

    std::cout << "dstlist: ";
    for (auto i : _dstlist) {
      std::cout << i << " ";
    }
    std::cout << std::endl;

    std::cout << "matrix: " << std::endl;
    for (const auto &i : _matrix) {
      for (auto j : i) {
        std::cout << j << " ";
      }
      std::cout << std::endl;
    }
  };

  MSGPACK_DEFINE(_srclist, _dstlist, _matrix);
};

class Command {
public:
  using stripe_id_t = meta::stripe_id_t;
  using block_id_t = meta::chunk_index_t;
  using offset_t = std::size_t;
  using shard_id_t = std::int64_t;
  using sub_shard_id_t = std::int64_t;
  using command_type_t = std::int64_t;
  using compute_type_t = std::int64_t;
  using ip_t = meta::ip_t;
  using ec_param_t = meta::ec_param_t;
  using disk_id_t = meta::disk_id_t;

  command_type_t _commandType;

  // read and cache type0
  std::string _stripeName;
  stripe_id_t _stripeId;
  shard_id_t _shardId;
  ec_param_t _k;
  ec_param_t _m;
  std::vector<sub_shard_id_t> _srcSubShardIdList;
  disk_id_t _diskId;

  // read and compute type1
  static constexpr compute_type_t Clay{0}, RS{1}, NSYS{2};
  compute_type_t _computeType;

  // fetch and compute type2
  std::vector<shard_id_t> _shardIdList;
  std::vector<ip_t> _srcIpList;
  std::vector<sub_shard_id_t> _distSubShardIdList;
  std::vector<ClayComputeTask> _clayComputeTaskList;

  // concatenate type3

  MSGPACK_DEFINE(_commandType, _stripeName, _stripeId, _shardId,
                 _srcSubShardIdList, _computeType, _srcIpList,
                 _distSubShardIdList, _shardIdList, _clayComputeTaskList, _k,
                 _m, _diskId);

  Command();
  // ~Command();
  explicit Command(std::string_view reqStr);
  explicit Command(const std::span<std::byte> reqBytes);

  [[nodiscard]] auto getCommandType() const -> command_type_t;
  [[nodiscard]] auto getStripeName() const -> const std::string &;
  [[nodiscard]] auto getStripeId() const -> stripe_id_t;
  [[nodiscard]] auto getShardId() const -> shard_id_t;
  [[nodiscard]] auto
  getSrcSubShardIdList() const -> const std::vector<sub_shard_id_t> &;
  [[nodiscard]] auto getComputeType() const -> compute_type_t;
  [[nodiscard]] auto getSrcIp() const -> const std::string &;
  [[nodiscard]] auto getSrcIpList() const -> const std::vector<ip_t> &;
  [[nodiscard]] auto
  getDistSubShardIdList() const -> const std::vector<sub_shard_id_t> &;
  [[nodiscard]] auto getShardIdList() const -> const std::vector<shard_id_t> &;
  // get the number of sub-chunks according to the EC parameters
  [[nodiscard]] auto getW() const -> std::size_t;

  // read and cache
  void buildType0(std::string stripeName, stripe_id_t stripeId,
                  shard_id_t shardId,
                  std::vector<sub_shard_id_t> srcSubShardIdList,
                  std::vector<sub_shard_id_t> distSubShardIdList,
                  disk_id_t diskId, ec_param_t k, ec_param_t m);
  // read and compute
  // void buildType1(string stripeName, int stripeId, int shardId, vector<int>
  // srcSubShardIdList,int computeType); fetch and compute (fetch a sub-shard
  // from a remote machine and compute it with a local sub-shard)
  void buildType2(std::string stripeName, stripe_id_t stripeId,
                  shard_id_t shardId, std::vector<shard_id_t> shardIdList,
                  std::vector<sub_shard_id_t> srcSubShardIdList,
                  compute_type_t computeType,
                  std::vector<std::string> srcIpList,
                  std::vector<sub_shard_id_t> distSubShardIdList);
  // clay fitch and comupte
  void buildType2(stripe_id_t stripeId, shard_id_t shardId,
                  std::vector<shard_id_t> shardIdList,
                  std::vector<sub_shard_id_t> srcSubShardIdList,
                  compute_type_t computeType,
                  std::vector<ClayComputeTask> clayComputeTaskList,
                  std::vector<std::string> srcIpList,
                  std::vector<sub_shard_id_t> distSubShardIdList);
  // concatenate (concatenate a list of sub-shards to a new shard)
  void buildType3(std::string stripeName, stripe_id_t stripeId,
                  shard_id_t shardId, std::vector<shard_id_t> shardIdList,
                  std::vector<sub_shard_id_t> srcSubShardIdList,
                  std::vector<std::string> srcIpList,
                  std::vector<sub_shard_id_t> distSubShardIdList,
                  disk_id_t diskId);

  [[nodiscard]] std::string serialize() const;

  void sendToRedis(redisContext *c) const;

  void display() const;
};

using CommandRef = std::shared_ptr<const Command>;
auto to_const_shared(Command cmd) -> CommandRef;
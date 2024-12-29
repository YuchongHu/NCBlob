/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-22 11:33:12
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-24 06:46:35
 * @FilePath: /tbr/protocol/BlockCommand.hh
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置
 * 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once

#include "meta.hpp"

#include <cstddef>
#include <cstdint>
#include <hiredis/hiredis.h>
#include <memory>
#include <msgpack.hpp>
#include <span>
#include <string_view>

#define READANDCACHEBLOCK 0
#define FETCHANDCOMPUTEANDWRITEBLOCK 1
#define READANDCACHEBLOCKCLAY 2
#define FETCH_WRITE_BLOCK 3

class BlockCommand {
public:
  using block_id_t = meta::chunk_index_t;
  using stripe_id_t = meta::stripe_id_t;
  using disk_id_t = meta::disk_id_t;
  using offset_t = std::size_t;
  using size_t = std::size_t;
  using ec_param_t = meta::ec_param_t;
  using compute_type_t = std::int32_t;
  using command_type_t = std::int32_t;
  using ip_t = meta::ip_t;

  command_type_t _commandType;

  // read and cache type0
  /// index of the block in its stripe
  /// #Note
  /// different from the block id in the meta
  /// #See
  /// meta::block_id_t
  block_id_t _blockId;
  offset_t _offset;
  size_t _size;
  stripe_id_t _stripeId;
  disk_id_t _diskId;

  ec_param_t _k;
  ec_param_t _m;

  // fetch and compute type1
  static constexpr compute_type_t CLAY_REPAIR{0}, RS_REPAIR{1}, NSYS_REPAIR{2},
      NSYS_READ = {3}, CLAY_READ = {4}, RS_READ = {5};
  compute_type_t _computeType;

  std::vector<ip_t> _srcIpList;
  std::vector<block_id_t> _srcBlockIdList;
  block_id_t _destBlockId;
  size_t _blockNum;

  // Clay
  std::vector<offset_t> _clayOffsetList;

  // concatenate type2

  MSGPACK_DEFINE(_commandType, _blockId, _offset, _size, _computeType,
                 _srcIpList, _srcBlockIdList, _destBlockId, _blockNum, _k, _m,
                 _clayOffsetList, _stripeId, _diskId);

  BlockCommand();
  explicit BlockCommand(std::string_view reqStr);
  explicit BlockCommand(const std::span<std::byte> reqBytes);

  [[nodiscard]] auto getCommandType() const -> command_type_t;
  // string getBlockName();
  [[nodiscard]] auto getBlockId() const -> block_id_t;
  [[nodiscard]] auto getStripeId() const -> stripe_id_t;
  [[nodiscard]] auto getDiskId() const -> disk_id_t;
  [[nodiscard]] auto getOffset() const -> offset_t;
  [[nodiscard]] auto getSize() const -> size_t;
  [[nodiscard]] auto getComputeType() const -> compute_type_t;
  [[nodiscard]] auto getSrcIpList() const -> const std::vector<std::string> &;
  [[nodiscard]] auto
  getSrcBlockIdList() const -> const std::vector<block_id_t> &;
  [[nodiscard]] auto getDestBlockId() const -> block_id_t;
  [[nodiscard]] auto getBlockNum() const -> std::size_t;
  [[nodiscard]] auto getClayOffsetList() const -> const std::vector<offset_t> &;

  // read and cache
  void buildType0(block_id_t blockId, offset_t offset, size_t size,
                  stripe_id_t stripeId, disk_id_t diskId, ec_param_t k,
                  ec_param_t m);
  // fetch and compute (fetch a sub-shard from a remote machine and compute it
  // read and cache with clay
  void buildType0(block_id_t blockId, std::vector<offset_t> clayOffsetList,
                  size_t size, stripe_id_t stripeId, disk_id_t diskId,
                  ec_param_t k, ec_param_t m);
  // with a local sub-shard)
  void buildType1(block_id_t blockId, compute_type_t computeType,
                  std::vector<ip_t> srcIpList,
                  std::vector<block_id_t> srcBlockIdList,
                  block_id_t destBlockId, size_t blockNum, stripe_id_t stripeId,
                  disk_id_t diskId, ec_param_t k, ec_param_t m);

  void buildType2(block_id_t blockId, stripe_id_t stripeId, disk_id_t diskId,
                  std::vector<ip_t> srcIpList,
                  std::vector<block_id_t> _srcBlockIdList, offset_t offset,
                  size_t size, ec_param_t k, ec_param_t m);

  // // concatenate (concatenate a list of sub-shards to a new shard)
  // void buildType2(int blockId);

  [[nodiscard]] std::string serialize() const;
  void display() const;
  void sendToRedis(redisContext *ctxbool, bool traffic_control = false) const;
};

using BlockCommandRef = std::shared_ptr<const BlockCommand>;
auto to_const_shared(BlockCommand bCmd) -> BlockCommandRef;
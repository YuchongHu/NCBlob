#include "BlockCommand.hh"

#include <cstddef>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

BlockCommand::BlockCommand() {
  _commandType = -1;
  _blockId = -1;
  _offset = -1;
  _size = -1;
  _computeType = -1;
  _destBlockId = -1;
}

BlockCommand::BlockCommand(std::string_view reqStr) {
  msgpack::object_handle oh = msgpack::unpack(reqStr.data(), reqStr.size());
  msgpack::object obj = oh.get();
  obj.convert(*this);
}

BlockCommand::BlockCommand(std::span<std::byte> reqBytes)
    : BlockCommand(std::string_view{
          reinterpret_cast<const char *>(reqBytes.data()), // NOLINT
          reqBytes.size()}) {}

void BlockCommand::buildType0(BlockCommand::block_id_t blockId,
                              BlockCommand::offset_t offset,
                              BlockCommand::size_t size,
                              BlockCommand::stripe_id_t stripeId,
                              BlockCommand::disk_id_t diskId,
                              BlockCommand::ec_param_t k,
                              BlockCommand::ec_param_t m) {
  _commandType = READANDCACHEBLOCK;
  _blockId = blockId;
  _offset = offset;
  _size = size;
  _stripeId = stripeId;
  _diskId = diskId;
  _k = k;
  _m = m;
}

void BlockCommand::buildType0(
    BlockCommand::block_id_t blockId,
    std::vector<BlockCommand::offset_t> clayOffsetList,
    BlockCommand::size_t size, BlockCommand::stripe_id_t stripeId,
    BlockCommand::disk_id_t diskId, BlockCommand::ec_param_t k,
    BlockCommand::ec_param_t m) {
  _commandType = READANDCACHEBLOCKCLAY;
  _blockId = blockId;
  _clayOffsetList = std::move(clayOffsetList);
  _size = size;
  _stripeId = stripeId;
  _diskId = diskId;
  _k = k;
  _m = m;
}

void BlockCommand::buildType1(block_id_t blockId, compute_type_t computeType,
                              std::vector<ip_t> srcIpList,
                              std::vector<block_id_t> srcBlockIdList,
                              block_id_t destBlockId, size_t blockNum,
                              stripe_id_t stripeId, disk_id_t diskId,
                              ec_param_t k, ec_param_t m) {
  _blockId = blockId;
  _commandType = FETCHANDCOMPUTEANDWRITEBLOCK;
  _computeType = computeType;
  _srcIpList = std::move(srcIpList);
  _srcBlockIdList = std::move(srcBlockIdList);
  _destBlockId = destBlockId;
  _blockNum = blockNum;
  _stripeId = stripeId;
  _diskId = diskId;
  _k = k;
  _m = m;
  ;
}

void BlockCommand::buildType2(block_id_t blockId, stripe_id_t stripeId,
                              disk_id_t diskId, std::vector<ip_t> srcIpList,
                              std::vector<block_id_t> srcBlockIdList,
                              offset_t offset, size_t size, ec_param_t k,
                              ec_param_t m) {
  _blockId = blockId;
  _commandType = FETCH_WRITE_BLOCK;
  _stripeId = stripeId;
  _diskId = diskId;
  _srcIpList = std::move(srcIpList);
  _srcBlockIdList = std::move(srcBlockIdList);
  _offset = offset;
  _size = size;
  _k = k;
  _m = m;
}

int BlockCommand::getCommandType() const { return _commandType; }

BlockCommand::block_id_t BlockCommand::getBlockId() const { return _blockId; }

BlockCommand::stripe_id_t BlockCommand::getStripeId() const {
  return _stripeId;
}

BlockCommand::disk_id_t BlockCommand::getDiskId() const { return _diskId; }

BlockCommand::offset_t BlockCommand::getOffset() const { return _offset; }

BlockCommand::size_t BlockCommand::getSize() const { return _size; }

const std::vector<std::string> &BlockCommand::getSrcIpList() const {
  return _srcIpList;
}

const std::vector<BlockCommand::block_id_t> &
BlockCommand::getSrcBlockIdList() const {
  return _srcBlockIdList;
}

BlockCommand::compute_type_t BlockCommand::getComputeType() const {
  return _computeType;
}

BlockCommand::block_id_t BlockCommand::getDestBlockId() const {
  return _destBlockId;
}

std::size_t BlockCommand::getBlockNum() const { return _blockNum; }

const std::vector<BlockCommand::offset_t> &
BlockCommand::getClayOffsetList() const {
  return _clayOffsetList;
}

std::string BlockCommand::serialize() const {
  msgpack::sbuffer sbuf;
  msgpack::packer<msgpack::sbuffer> pk(&sbuf);
  pk.pack(*this);
  return std::string(sbuf.data(), sbuf.size());
}

void BlockCommand::sendToRedis(redisContext *ctx, bool traffic_control) const {
  throw std::runtime_error("Not implemented");
  // std::string str = serialize();
  // auto data = std::vector<char>(str.begin(), str.end());
  // if (traffic_control) {
  //   constexpr std::size_t MAX_QUEUE_SIZE = 256;
  //   while (RedisUtil::llen(ctx, "blockCommand") > MAX_QUEUE_SIZE) {
  //     std::this_thread::sleep_for(std::chrono::milliseconds(20));
  //   }
  // }
  // RedisUtil::rpushContent(ctx, "blockCommand", data);
}

void BlockCommand::display() const {
  std::cout << "BlockCommand: " << std::endl;
  std::cout << "commandType: " << _commandType << std::endl;
  std::cout << "blockId: " << _blockId << std::endl;
  std::cout << "offset: " << _offset << std::endl;
  std::cout << "size: " << _size << std::endl;
  std::cout << "computeType: " << _computeType << std::endl;
  std::cout << "destBlockId: " << _destBlockId << std::endl;
  std::cout << "blockNum: " << _blockNum << std::endl;
  std::cout << "srcIpList: ";
  for (auto ip : _srcIpList) {
    std::cout << ip << " ";
  }
  std::cout << std::endl;
  std::cout << "srcBlockIdList: ";
  for (auto id : _srcBlockIdList) {
    std::cout << id << " ";
  }
  std::cout << std::endl;
}
auto to_const_shared(BlockCommand bCmd) -> BlockCommandRef {
  return std::make_shared<BlockCommand>(std::move(bCmd));
}
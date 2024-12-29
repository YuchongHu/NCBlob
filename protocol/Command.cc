#include "Command.hh"
#include "ec_intf.hh"
#include "exception.hpp"

#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

using std::string;
using std::vector;

Command::Command(std::string_view reqStr) {
  msgpack::object_handle oh = msgpack::unpack(reqStr.data(), reqStr.size());
  msgpack::object obj = oh.get();
  obj.convert(*this);
}
Command::Command(const std::span<std::byte> reqBytes)
    : Command(std::string_view{
          reinterpret_cast<const char *>(reqBytes.data()), // NOLINT
          reqBytes.size()}) {}

Command::Command() {
  _commandType = -1;
  _stripeName = "";
  _stripeId = -1;
  _shardId = -1;
  _srcSubShardIdList = {};
  _computeType = -1;
  _srcIpList = {};
  _distSubShardIdList = {};
}

void Command::buildType0(std::string stripeName, stripe_id_t stripeId,
                         shard_id_t shardId,
                         std::vector<sub_shard_id_t> srcSubShardIdList,
                         std::vector<sub_shard_id_t> distSubShardIdList,
                         disk_id_t diskId, ec_param_t k, ec_param_t m) {
  _commandType = READANDCACHE;
  _stripeName = std::move(stripeName);
  _stripeId = stripeId;
  _shardId = shardId;
  _srcSubShardIdList = std::move(srcSubShardIdList);
  _distSubShardIdList = std::move(distSubShardIdList);
  _diskId = diskId;
  _k = k;
  _m = m;
}

// void Command::buildType1(string stripeName, int stripeId, int shardId,
// vector<int> srcSubShardIdList, int computeType) {
//     _commandType = 1;
//     _stripeName = stripeName;
//     _stripeId = stripeId;
//     _shardId = shardId;
//     _subShardIdList = srcSubShardIdList;
//     _computeType = computeType;
// }

void Command::buildType2(std::string stripeName, stripe_id_t stripeId,
                         shard_id_t shardId,
                         std::vector<shard_id_t> shardIdList,
                         std::vector<sub_shard_id_t> srcSubShardIdList,
                         compute_type_t computeType,
                         std::vector<std::string> srcIpList,
                         std::vector<sub_shard_id_t> distSubShardIdList) {
  _commandType = FETCHANDCOMPUTE;
  _stripeName = std::move(stripeName);
  _stripeId = stripeId;
  _shardId = shardId;
  _srcSubShardIdList = std::move(srcSubShardIdList);
  _computeType = computeType;
  _shardIdList = std::move(shardIdList);
  _srcIpList = std::move(srcIpList);
  _distSubShardIdList = std::move(distSubShardIdList);
}

// clay
void Command::buildType2(stripe_id_t stripeId, shard_id_t shardId,
                         std::vector<shard_id_t> shardIdList,
                         std::vector<sub_shard_id_t> srcSubShardIdList,
                         compute_type_t computeType,
                         std::vector<ClayComputeTask> clayComputeTaskList,
                         std::vector<std::string> srcIpList,
                         std::vector<sub_shard_id_t> distSubShardIdList) {
  _commandType = FETCHANDCOMPUTE;
  _stripeId = stripeId;
  _shardId = shardId;
  _srcSubShardIdList = std::move(srcSubShardIdList);
  _computeType = computeType;
  _shardIdList = std::move(shardIdList);
  _srcIpList = std::move(srcIpList);
  _clayComputeTaskList = std::move(clayComputeTaskList);
  _distSubShardIdList = std::move(distSubShardIdList);
}

void Command::buildType3(std::string stripeName, stripe_id_t stripeId,
                         shard_id_t shardId,
                         std::vector<shard_id_t> shardIdList,
                         std::vector<sub_shard_id_t> srcSubShardIdList,
                         std::vector<ip_t> srcIpList,
                         std::vector<sub_shard_id_t> distSubShardIdList,
                         disk_id_t diskId) {
  _commandType = CONCATENATE;
  _stripeName = std::move(stripeName);
  _stripeId = stripeId;
  _shardId = shardId;
  _srcSubShardIdList = std::move(srcSubShardIdList);
  _shardIdList = std::move(shardIdList);
  _srcIpList = std::move(srcIpList);
  _distSubShardIdList = std::move(distSubShardIdList);
  _diskId = diskId;
}

Command::command_type_t Command::getCommandType() const { return _commandType; }

Command::stripe_id_t Command::getStripeId() const { return _stripeId; }

Command::shard_id_t Command::getShardId() const { return _shardId; }

const vector<Command::sub_shard_id_t> &Command::getSrcSubShardIdList() const {
  return _srcSubShardIdList;
}

Command::compute_type_t Command::getComputeType() const { return _computeType; }

const vector<Command::shard_id_t> &Command::getShardIdList() const {
  return _shardIdList;
}

const vector<string> &Command::getSrcIpList() const { return _srcIpList; }

const vector<Command::sub_shard_id_t> &Command::getDistSubShardIdList() const {
  return _distSubShardIdList;
}

std::size_t Command::getW() const {
  if (_commandType == RS) {
    return ec::encoder::rs::Encoder{_k, _m}.get_sub_chunk_num();
  } else if (_commandType == NSYS) {
    return ec::encoder::nsys::Encoder{_k, _m}.get_sub_chunk_num();
  } else if (_commandType == Clay) {
    return ec::encoder::clay::Encoder{_k, _m}.get_sub_chunk_num();
  } else {
    throw std::runtime_error("Invalid command type");
  }
}

std::string Command::serialize() const {
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, *this);
  return std::string(sbuf.data(), sbuf.size());
}

void Command::sendToRedis(redisContext *c) const {
  err::Unimplemented();
  // std::string str = serialize();
  // auto data = std::vector<char>(str.begin(), str.end());
  // RedisUtil::rpushContent(c, "command", data);
}

void Command::display() const {
  std::cout << "Command Type: " << _commandType << '\n';
  std::cout << "Stripe Name: " << _stripeName << '\n';
  std::cout << "Stripe Id: " << _stripeId << '\n';
  std::cout << "Shard Id: " << _shardId << '\n';
  std::cout << "Sub Shard Id List: ";
  for (int subShard : _srcSubShardIdList) {
    std::cout << subShard << " ";
  }
  std::cout << '\n';
  std::cout << "src Sub Shard Id List: ";
  for (auto subShard : _srcSubShardIdList) {
    std::cout << subShard << " ";
  }
  std::cout << '\n';
  std::cout << "Compute Type: " << _computeType << '\n';
  std::cout << "Src Ip List: ";
  for (const auto &srcIp : _srcIpList) {
    std::cout << srcIp << " ";
  }
  std::cout << '\n';
  std::cout << "Dist Sub Shard Id List: ";
  for (auto distSubShardId : _distSubShardIdList) {
    std::cout << distSubShardId << " ";
  }
  std::cout << std::endl;
}
auto to_const_shared(Command cmd) -> CommandRef {
  return std::make_shared<Command>(std::move(cmd));
}
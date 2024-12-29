#include "Tasks.hh"
#include "Command.hh"
#include "task_util.hh"

#include <algorithm>
#include <boost/numeric/conversion/cast.hpp>
#include <fstream>
#include <vector>

namespace {
auto genRSPipelineRepairTaskList(
    int stripeId, int shardId, int k, int m, const std::vector<int> &diskList,
    const std::vector<std::string> &ipList,
    std::vector<std::string> &distIpList) -> std::vector<Command> {
  auto srcShardIdList = genRandomList(k + m, k, shardId);
  sort(srcShardIdList.begin(), srcShardIdList.end());
  std::vector<Command> taskList;
  for (auto srcShardId : srcShardIdList) {
    Command cmd;
    cmd.buildType0("stripeName",
                   stripeId,
                   srcShardId,
                   {0},
                   {0},
                   diskList[srcShardId],
                   k,
                   m);
    taskList.push_back(cmd);
    distIpList.push_back(ipList[srcShardId]);
  }
  Command cmd;
  cmd.buildType2("stripeName",
                 stripeId,
                 shardId,
                 {srcShardIdList[0], srcShardIdList[1]},
                 {0, 0},
                 Command::RS,
                 {ipList[srcShardIdList[0]], "127.0.0.1"},
                 {m});
  distIpList.push_back(ipList[srcShardIdList[1]]);
  for (int i = 2; i < srcShardIdList.size(); i++) {
    Command cmd;
    cmd.buildType2("stripeName",
                   stripeId,
                   shardId,
                   {shardId, srcShardIdList[i]},
                   {m, 0},
                   Command::RS,
                   {ipList[srcShardIdList[i - 1]], "127.0.0.1"},
                   {m});
    taskList.push_back(cmd);
    distIpList.push_back(ipList[srcShardIdList[i]]);
  }
  Command cmd_3;
  cmd_3.buildType3("stripeName",
                   stripeId,
                   shardId,
                   {shardId},
                   {m},
                   {ipList[srcShardIdList.size() - 1]},
                   {0},
                   diskList[shardId]);
  taskList.push_back(cmd_3);
  distIpList.push_back(ipList[shardId]);
  return taskList;
}

auto genNSYSPipelineRepairTaskList(
    int stripeId, int shardId, int k, int m, const std::vector<int> &diskList,
    const std::vector<std::string> &ipList,
    std::vector<std::string> &distIpList) -> std::vector<Command> {
  std::vector<int> srcShardIdList;
  for (int i = 0; i < k + m; i++) {
    if (i != shardId) {
      srcShardIdList.push_back(i);
    }
  }
  std::vector<Command> taskList;
  for (auto srcShardId : srcShardIdList) {
    Command cmd;
    cmd.buildType0("stripeName",
                   stripeId,
                   srcShardId,
                   {0},
                   {0},
                   diskList[srcShardId],
                   k,
                   m);
    taskList.push_back(cmd);
    distIpList.push_back(ipList[srcShardId]);
  }
  Command cmd;
  cmd.buildType2("stripeName",
                 stripeId,
                 shardId,
                 {srcShardIdList[0], srcShardIdList[1]},
                 {0, 0},
                 Command::RS,
                 {ipList[srcShardIdList[0]], "127.0.0.1"},
                 {m});
  distIpList.push_back(ipList[srcShardIdList[1]]);
  for (int i = 2; i < srcShardIdList.size(); i++) {
    Command cmd;
    cmd.buildType2("stripeName",
                   stripeId,
                   shardId,
                   {shardId, srcShardIdList[i]},
                   {m, 0},
                   Command::NSYS,
                   {ipList[srcShardIdList[i - 1]], "127.0.0.1"},
                   {m});
    taskList.push_back(cmd);
    distIpList.push_back(ipList[srcShardIdList[i]]);
  }
  Command cmd_3;
  cmd_3.buildType3("stripeName",
                   stripeId,
                   shardId,
                   {shardId},
                   {m},
                   {ipList[srcShardIdList.size() - 1]},
                   {0},
                   diskList[shardId]);
  taskList.push_back(cmd_3);
  distIpList.push_back(ipList[shardId]);
  return taskList;
}
auto genClayCommands(std::string filename,
                     std::vector<int> &distNodeList) -> std::vector<Command> {
  std::ifstream inFile(filename, std::ios::binary);
  std::string str((std::istreambuf_iterator<char>(inFile)),
                  std::istreambuf_iterator<char>());
  msgpack::object_handle oh = msgpack::unpack(str.data(), str.size());
  msgpack::object obj = oh.get();
  std::vector<std::string> vec;
  obj.convert(vec);

  std::string str1 = vec[0];
  msgpack::object_handle oh1 = msgpack::unpack(str1.data(), str1.size());
  msgpack::object obj1 = oh1.get();
  std::vector<Command> commandList;
  obj1.convert(commandList);

  std::string str2 = vec[1];
  msgpack::object_handle oh2 = msgpack::unpack(str2.data(), str2.size());
  msgpack::object obj2 = oh2.get();
  std::vector<int> nodeList;
  obj2.convert(nodeList);
  for (int i = 0; i < nodeList.size(); i++) {
    distNodeList.push_back(nodeList[i]);
  }

  return commandList;
}
auto genClayPipelineRepairTaskList(
    int stripeId, int shardId, int k, int m, const std::vector<int> &diskList,
    const std::vector<std::string> &ipList,
    std::vector<std::string> &distIpList) -> std::vector<Command> {
  std::string filepath = "./clay-config/Clay_" + std::to_string(k + m) + "_" +
                         std::to_string(shardId) + ".bin";
  std::vector<int> distNodeList;
  std::vector<Command> commandList = genClayCommands(filepath, distNodeList);

  for (int i = 0; i < distNodeList.size(); i++) {
    distIpList.push_back(ipList[distNodeList[i]]);
    std::vector<std::string> srcIpList;
    for (auto ipId : commandList[i]._srcIpList) {
      srcIpList.push_back(ipList[atoi(ipId.c_str())]);
    }
    commandList[i]._srcIpList = srcIpList;
    if (commandList[i]._commandType == 0 || commandList[i]._commandType == 3) {
      commandList[i]._diskId = diskList[distNodeList[i]];
    }
  }
  return commandList;
}
} // namespace

using boost::numeric_cast;

auto task::repair::pipeline::rs::TaskBuilder::build()
    -> std::pair<std::vector<Command>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto diskList = std::vector<int>{};
  diskList.reserve(this->diskList.value().get().size());
  std::transform(this->diskList.value().get().begin(),
                 this->diskList.value().get().end(),
                 std::back_inserter(diskList),
                 [](const auto &disk) { return numeric_cast<int>(disk); });
  auto cmds =
      genRSPipelineRepairTaskList(numeric_cast<int>(stripeId.value()),
                                  numeric_cast<int>(chunk_index.value()),
                                  numeric_cast<int>(k.value()),
                                  numeric_cast<int>(m.value()),
                                  diskList,
                                  ipList.value(),
                                  distIpList);
  return {cmds, distIpList};
}

auto task::repair::pipeline::nsys::TaskBuilder::build()
    -> std::pair<std::vector<Command>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto diskList = std::vector<int>{};
  diskList.reserve(this->diskList.value().get().size());
  std::transform(this->diskList.value().get().begin(),
                 this->diskList.value().get().end(),
                 std::back_inserter(diskList),
                 [](const auto &disk) { return numeric_cast<int>(disk); });
  auto cmds =
      genNSYSPipelineRepairTaskList(numeric_cast<int>(stripeId.value()),
                                    numeric_cast<int>(chunk_index.value()),
                                    numeric_cast<int>(k.value()),
                                    numeric_cast<int>(m.value()),
                                    diskList,
                                    ipList.value(),
                                    distIpList);
  return {cmds, distIpList};
}

auto task::repair::pipeline::clay::TaskBuilder::build()
    -> std::pair<std::vector<Command>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto diskList = std::vector<int>{};
  diskList.reserve(this->diskList.value().get().size());
  std::transform(this->diskList.value().get().begin(),
                 this->diskList.value().get().end(),
                 std::back_inserter(diskList),
                 [](const auto &disk) { return numeric_cast<int>(disk); });
  auto cmds =
      genClayPipelineRepairTaskList(numeric_cast<int>(stripeId.value()),
                                    numeric_cast<int>(chunk_index.value()),
                                    numeric_cast<int>(k.value()),
                                    numeric_cast<int>(m.value()),
                                    diskList,
                                    ipList.value(),
                                    distIpList);
  return {cmds, distIpList};
}

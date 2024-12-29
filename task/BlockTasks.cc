#include "BlockCommand.hh"
#include "Tasks.hh"
#include "ec_intf.hh"
#include "erasure_code.hh"
#include "erasure_code_factory.hpp"
#include "erasure_code_intf.hpp"
#include "meta.hpp"
#include "task_util.hh"

#include <algorithm>
#include <boost/numeric/conversion/cast.hpp>

using namespace std;
using namespace ec;

namespace {
auto genRSCentRepairTaskList(
    int stripeId, int distBlockId, int k, int m, int offset, int size,
    const std::vector<meta::disk_id_t> &diskList,
    const std::vector<std::string> &ipList,
    std::vector<string> &distIpList) -> std::vector<BlockCommand> {
  auto srcBlockIdList = genRandomList(k + m, k, distBlockId);
  sort(srcBlockIdList.begin(), srcBlockIdList.end());
  std::vector<int> srcDiskList;
  std::vector<std::string> srcIpList;
  std::vector<BlockCommand> taskList;
  for (auto srcBlockId : srcBlockIdList) {
    srcDiskList.push_back(diskList[srcBlockId]);
    srcIpList.push_back(ipList[srcBlockId]);

    BlockCommand bCmd;
    bCmd.buildType0(
        srcBlockId, offset, size, stripeId, diskList[srcBlockId], k, m);
    taskList.push_back(bCmd);
  }
  BlockCommand bCmd;
  bCmd.buildType1(distBlockId,
                  BlockCommand::RS_REPAIR,
                  srcIpList,
                  srcBlockIdList,
                  distBlockId,
                  k,
                  stripeId,
                  diskList[distBlockId],
                  k,
                  m);
  bCmd._size = size;
  taskList.push_back(bCmd);
  copy(srcIpList.begin(), srcIpList.end(), back_inserter(distIpList));
  distIpList.push_back(ipList[distBlockId]);
  return taskList;
}

auto genNSYSCentRepairTaskList(
    int stripeId, int distBlockId, int k, int m, int offset, int size,
    const std::vector<meta::disk_id_t> &diskList,
    const std::vector<std::string> &ipList,
    std::vector<std::string> &distIpList) -> std::vector<BlockCommand> {
  vector<BlockCommand::block_id_t> srcBlockIdList;
  for (int i = 0; i < k + m; i++) {
    if (i != distBlockId) {
      srcBlockIdList.push_back(i);
    }
  }
  std::vector<BlockCommand::disk_id_t> srcDiskList;
  std::vector<BlockCommand::ip_t> srcIpList;
  std::vector<BlockCommand> taskList;
  for (auto srcBlockId : srcBlockIdList) {
    srcDiskList.push_back(diskList[srcBlockId]);
    srcIpList.push_back(ipList[srcBlockId]);

    BlockCommand bCmd;
    bCmd.buildType0(
        srcBlockId, offset, size, stripeId, diskList[srcBlockId], k, m);
    taskList.push_back(bCmd);
  }
  BlockCommand bCmd;
  bCmd.buildType1(distBlockId,
                  BlockCommand::NSYS_REPAIR,
                  srcIpList,
                  srcBlockIdList,
                  distBlockId,
                  k + m - 1,
                  stripeId,
                  diskList[distBlockId],
                  k,
                  m);
  bCmd._size = size;
  taskList.push_back(bCmd);
  copy(srcIpList.begin(), srcIpList.end(), back_inserter(distIpList));
  distIpList.push_back(ipList[distBlockId]);
  return taskList;
}

auto genCLAYRead(int stripeId, int distBlockId, int k, int m, int size,
                 const std::vector<meta::disk_id_t> &diskList,
                 const std::vector<std::string> &ipList,
                 std::vector<std::string> &distIpList)
    -> std::vector<BlockCommand> {
  vector<BlockCommand::offset_t> clayOffsetList;
  int d = k + m - 1;
  int q = d - k + 1;

  std::ostringstream errors;
  ErasureCodeProfile profile;
  profile["k"] = std::to_string(k);
  profile["m"] = std::to_string(m);
  ErasureCodeInterfaceRef intf = ErasureCodeClayFactory{}.make(profile, errors);
  auto &clay = dynamic_cast<ErasureCode &>(*intf.get());

  int avail[k + m];
  for (int i = 0; i < k + m; i++)
    avail[i] = i;

  set<int> want_to_read;
  want_to_read.insert(distBlockId);
  set<int> available(avail, avail + k + m);
  available.erase(distBlockId);
  map<int, vector<pair<int, int>>> minimum;
  clay.minimum_to_decode(want_to_read, available, &minimum);

  for (auto p : minimum.begin()->second) {
    // one seperate offset for each subchunk, even if it's continuous
    // the offset is the address offset for subchunk
    for (int i = 0; i < p.second; i++) {
      clayOffsetList.push_back((p.first + i) * size);
    }
  }
  for (auto &off : clayOffsetList) {
    if (off + size >
        size * ec::encoder::clay::Encoder{k, m}.get_sub_chunk_num()) {
      throw std::invalid_argument("offset out of range");
    }
  }

  vector<BlockCommand::block_id_t> srcBlockIdList;
  for (int i = 0; i < k + m; i++) {
    if (i != distBlockId) {
      srcBlockIdList.push_back(i);
    }
  }
  std::vector<BlockCommand::disk_id_t> srcDiskList;
  std::vector<BlockCommand::ip_t> srcIpList;
  std::vector<BlockCommand> taskList;
  for (auto srcBlockId : srcBlockIdList) {
    srcDiskList.push_back(diskList[srcBlockId]);
    srcIpList.push_back(ipList[srcBlockId]);

    BlockCommand bCmd;
    bCmd.buildType0(
        srcBlockId, clayOffsetList, size, stripeId, diskList[srcBlockId], k, m);
    taskList.push_back(bCmd);
  }
  BlockCommand bCmd;
  bCmd.buildType1(distBlockId,
                  BlockCommand::CLAY_READ,
                  srcIpList,
                  srcBlockIdList,
                  distBlockId,
                  k + m - 1,
                  stripeId,
                  diskList[distBlockId],
                  k,
                  m);
  bCmd._size = size;
  taskList.push_back(bCmd);
  copy(srcIpList.begin(), srcIpList.end(), back_inserter(distIpList));
  distIpList.push_back(ipList[distBlockId]);
  return taskList;
}

auto genNSYSRead(int stripeId, int distBlockId, int k, int m, int offset,
                 int size, const std::vector<meta::disk_id_t> &diskList,
                 const std::vector<std::string> &ipList,
                 std::vector<std::string> &distIpList)
    -> std::vector<BlockCommand> {
  auto srcBlockIdList = genRandomList(k + m, k, distBlockId);
  sort(srcBlockIdList.begin(), srcBlockIdList.end());
  std::vector<int> srcDiskList;
  std::vector<string> srcIpList;
  std::vector<BlockCommand> taskList;
  for (auto srcBlockId : srcBlockIdList) {
    srcDiskList.push_back(diskList[srcBlockId]);
    srcIpList.push_back(ipList[srcBlockId]);

    BlockCommand bCmd;
    bCmd.buildType0(
        srcBlockId, offset, size, stripeId, diskList[srcBlockId], k, m);
    taskList.push_back(bCmd);
  }
  BlockCommand bCmd;
  bCmd.buildType1(distBlockId,
                  BlockCommand::NSYS_READ,
                  srcIpList,
                  srcBlockIdList,
                  distBlockId,
                  k,
                  stripeId,
                  diskList[distBlockId],
                  k,
                  m);
  bCmd._size = size;
  taskList.push_back(bCmd);
  copy(srcIpList.begin(), srcIpList.end(), back_inserter(distIpList));
  distIpList.push_back(ipList[distBlockId]);
  return taskList;
}

/// - size: subchunk size
auto genClayCentRepairTaskList(
    int stripeId, int distBlockId, int k, int m, int size,
    const std::vector<meta::disk_id_t> &diskList,
    const std::vector<std::string> &ipList,
    std::vector<std::string> &distIpList) -> std::vector<BlockCommand> {
  vector<BlockCommand::offset_t> clayOffsetList;
  int d = k + m - 1;
  int q = d - k + 1;

  std::ostringstream errors;
  ErasureCodeProfile profile;
  profile["k"] = std::to_string(k);
  profile["m"] = std::to_string(m);
  ErasureCodeInterfaceRef intf = ErasureCodeClayFactory{}.make(profile, errors);
  auto &clay = dynamic_cast<ErasureCode &>(*intf.get());

  int avail[k + m];
  for (int i = 0; i < k + m; i++)
    avail[i] = i;

  set<int> want_to_read;
  want_to_read.insert(distBlockId);
  set<int> available(avail, avail + k + m);
  available.erase(distBlockId);
  map<int, vector<pair<int, int>>> minimum;
  clay.minimum_to_decode(want_to_read, available, &minimum);

  for (auto p : minimum.begin()->second) {
    // one seperate offset for each subchunk, even if it's continuous
    // the offset is the address offset for subchunk
    for (int i = 0; i < p.second; i++) {
      clayOffsetList.push_back((p.first + i) * size);
    }
  }
  for (auto &off : clayOffsetList) {
    if (off + size >
        size * ec::encoder::clay::Encoder{k, m}.get_sub_chunk_num()) {
      throw std::invalid_argument("offset out of range");
    }
  }

  vector<BlockCommand::block_id_t> srcBlockIdList;
  for (int i = 0; i < k + m; i++) {
    if (i != distBlockId) {
      srcBlockIdList.push_back(i);
    }
  }
  std::vector<BlockCommand::disk_id_t> srcDiskList;
  std::vector<BlockCommand::ip_t> srcIpList;
  std::vector<BlockCommand> taskList;
  for (auto srcBlockId : srcBlockIdList) {
    srcDiskList.push_back(diskList[srcBlockId]);
    srcIpList.push_back(ipList[srcBlockId]);

    BlockCommand bCmd;
    bCmd.buildType0(
        srcBlockId, clayOffsetList, size, stripeId, diskList[srcBlockId], k, m);
    taskList.push_back(bCmd);
  }
  BlockCommand bCmd;
  bCmd.buildType1(distBlockId,
                  BlockCommand::CLAY_REPAIR,
                  srcIpList,
                  srcBlockIdList,
                  distBlockId,
                  k + m - 1,
                  stripeId,
                  diskList[distBlockId],
                  k,
                  m);
  bCmd._size = size;
  taskList.push_back(bCmd);
  copy(srcIpList.begin(), srcIpList.end(), back_inserter(distIpList));
  distIpList.push_back(ipList[distBlockId]);
  return taskList;
}
} // namespace

using boost::numeric_cast;

auto task::repair::centralize::clay::TaskBuilder::build()
    -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto cmds = genClayCentRepairTaskList(numeric_cast<int>(stripeId.value()),
                                        numeric_cast<int>(chunk_index.value()),
                                        numeric_cast<int>(k.value()),
                                        numeric_cast<int>(m.value()),
                                        numeric_cast<int>(size.value()),
                                        diskList.value(),
                                        ipList.value(),
                                        distIpList);
  return {std::move(cmds), std::move(distIpList)};
}
auto task::repair::centralize::rs::TaskBuilder::build()
    -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto cmds = genRSCentRepairTaskList(numeric_cast<int>(stripeId.value()),
                                      numeric_cast<int>(chunk_index.value()),
                                      numeric_cast<int>(k.value()),
                                      numeric_cast<int>(m.value()),
                                      numeric_cast<int>(offset.value()),
                                      numeric_cast<int>(size.value()),
                                      diskList.value(),
                                      ipList.value(),
                                      distIpList);
  return {std::move(cmds), std::move(distIpList)};
}

auto task::repair::centralize::nsys::TaskBuilder::build()
    -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto cmds = genNSYSCentRepairTaskList(numeric_cast<int>(stripeId.value()),
                                        numeric_cast<int>(chunk_index.value()),
                                        numeric_cast<int>(k.value()),
                                        numeric_cast<int>(m.value()),
                                        numeric_cast<int>(offset.value()),
                                        numeric_cast<int>(size.value()),
                                        diskList.value(),
                                        ipList.value(),
                                        distIpList);
  return {std::move(cmds), std::move(distIpList)};
}

auto task::read::nsys::TaskBuilder::build()
    -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto cmds = genNSYSRead(numeric_cast<int>(stripeId.value()),
                          numeric_cast<int>(chunk_index.value()),
                          numeric_cast<int>(k.value()),
                          numeric_cast<int>(m.value()),
                          numeric_cast<int>(offset.value()),
                          numeric_cast<int>(size.value()),
                          diskList.value(),
                          ipList.value(),
                          distIpList);
  return {std::move(cmds), std::move(distIpList)};
};

auto task::read::clay::TaskBuilder::build()
    -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>> {
  auto distIpList = std::vector<meta::ip_t>{};
  auto cmds = genCLAYRead(numeric_cast<int>(stripeId.value()),
                          numeric_cast<int>(chunk_index.value()),
                          numeric_cast<int>(k.value()),
                          numeric_cast<int>(m.value()),
                          numeric_cast<int>(size.value()),
                          diskList.value(),
                          ipList.value(),
                          distIpList);
  return {std::move(cmds), std::move(distIpList)};
};
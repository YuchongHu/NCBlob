#include "./common/Worker.hh"
#include "RedisUtil.hh"

#include <filesystem>
#include <gtest/gtest.h>

TEST(Worker, fetchAndComputeAndWriteBlock) {
  // coordinator
  std::string distIp = "127.0.0.1";
  auto distCtx = RedisUtil::CreateContext(distIp);
  BlockCommand bCmd1;
  // bCmd1.buildType0(1, 0, 1024 * 1024);
  bCmd1.sendToRedis(distCtx.get());

  BlockCommand bCmd2;
  // bCmd2.buildType0(2, 0, 1024 * 1024);
  bCmd2.sendToRedis(distCtx.get());

  BlockCommand bCmd3;
  // bCmd3.buildType0(3, 0, 1024 * 1024);
  bCmd3.sendToRedis(distCtx.get());

  BlockCommand bCmd4;
  // bCmd4.buildType1(0, 1, {"127.0.0.1", "127.0.0.1", "127.0.0.1"}, {1, 2, 3},
  // 0,
  //                  3);
  bCmd4.sendToRedis(distCtx.get());

  // client
  auto tmpdir = std::filesystem::temp_directory_path() / "worker_test_store";
  std::filesystem::create_directories(tmpdir);
  auto ctx = std::make_shared<WorkCtx>(12, tmpdir);
  doBlockWork(ctx);
  doBlockWork(ctx);
  doBlockWork(ctx);
  doBlockWork(ctx);
}
#include "RedisUtil.hh"
#include "Worker.hh"
#include <filesystem>
#include <gtest/gtest.h>

// TEST(Worker,readAndCache){
//     //coordinator
//     std::string distIp = "127.0.0.1";
//     redisContext* distCtx = RedisUtil::createContext(distIp);
//     Command cmd;
//     cmd.buildType0("stripeName", 1, 2, {3, 4},{5,6});
//     cmd.sendToRedis(distCtx);

//     //client

//     BS::thread_pool threadPool;
//     doWork(threadPool);
// }

// TEST(Worker,fetchAndCompute){
//     //coordinator
//     std::string distIp = "127.0.0.1";
//     redisContext* distCtx = RedisUtil::createContext(distIp);
//     Command cmd1;
//     cmd1.buildType0("stripeName", 1, 2, {3, 4},{5,6});
//     cmd1.sendToRedis(distCtx);

//     Command cmd2;
//     cmd2.buildType2("stripeName", 1, 3,{2,
//     2},{5,6},2,{"127.0.0.1","127.0.0.1"},{7,8}); cmd2.sendToRedis(distCtx);

//     //client

//     BS::thread_pool threadPool;
//     doWork(threadPool);
//     doWork(threadPool);
// }

TEST(Worker, concatenate) {
  // coordinator
  std::string distIp = "127.0.0.1";
  auto distCtx = RedisUtil::CreateContext(distIp);
  Command cmd1;
  // cmd1.buildType0("stripeName", 1, 2, {3, 4}, {5, 6});
  cmd1.sendToRedis(distCtx.get());

  Command cmd2;
  cmd2.buildType2("stripeName", 1, 3, {2, 2}, {5, 6}, 2,
                  {"127.0.0.1", "127.0.0.1"}, {7, 8});
  cmd2.sendToRedis(distCtx.get());

  Command cmd3;
  // cmd3.buildType3("stripeName", 1, 4, {3, 3}, {7, 8},
  //                 {"127.0.0.1", "127.0.0.1"}, {9, 10});
  cmd3.sendToRedis(distCtx.get());

  auto tmpdir = std::filesystem::temp_directory_path() / "worker_test_store";
  std::filesystem::create_directories(tmpdir);
  auto ctx = std::make_shared<WorkCtx>(12, tmpdir);
  // client
  doWork(ctx);
  doWork(ctx);
  doWork(ctx);
}

int main(int argc, char **argv) {
  std::cout << "Running main() from Command_test.cc\n";
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#include "./protocol/Command.hh"
#include "./protocol/RedisUtil.hh"
#include <string_view>

int main() {
  auto coordinator = RedisUtil::CreateContext("127.0.0.1");
  auto content = RedisUtil::blpopContent(coordinator.get(), "test");
  Command cmd(std::string_view{content.data(), content.size()});
  cmd.display();
  return 0;

  switch (cmd.getCommandType()) {
  case READANDCACHE:
    /* code */
    break;

  default:
    break;
  }
}
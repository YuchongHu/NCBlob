
#include "exception.hpp"
#include "worker_core.hh"

#include <boost/program_options.hpp>

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

auto main(int argc, char **argv) -> int {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] // NOLINT
              << " <worker_cfg.toml>" << std::endl;
    return EXIT_FAILURE;
  }
  auto cfg_file = std::string{argv[1]}; // NOLINT
  auto profile =
      std::make_shared<worker::Profile>(worker::Profile::ParseToml(cfg_file));
  std::cout << *profile.get() << std::endl;
  try {
    if (std::filesystem::exists(profile->working_dir)) {
      if (profile->create_new) {
        // remove all the files under working dir
        for (auto &dir :
             std::filesystem::directory_iterator(profile->working_dir)) {
          std::filesystem::remove_all(dir);
        }
      }
    } else {
      std::filesystem::create_directories(profile->working_dir);
    }
  } catch (std::exception &e) {
    std::cerr << "[Error] " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  auto ctx = std::shared_ptr<worker::WorkInterface>{};
  try {
    if (profile->block) {
      ctx = std::make_shared<worker::BlockWorkerCtx>(profile);
    } else {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
      err::Unreachable();
      ctx = std::make_shared<worker::SlicedWorkerCtx>(profile); // NOLINT
#pragma GCC diagnostic pop
    }
  } catch (std::exception &e) {
    std::cerr << "[Error] " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  try {
    ctx->run();
  } catch (std::exception &e) {
    std::cerr << "[Error] " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
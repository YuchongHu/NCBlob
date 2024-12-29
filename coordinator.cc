
#include "coord_core.hh"

#include "exception.hpp"
#include "toml11/exception.hpp"

#include <boost/numeric/conversion/cast.hpp>
#include <fmt/core.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <exception>
#include <fmt/format.h>
#include <iostream>
#include <memory>
#include <stdexcept>

inline auto throughput(size_t bytes_size,
                       std::chrono::milliseconds elapse) -> double {
  return static_cast<double>(bytes_size >> 20) /       // NOLINT
         (static_cast<double>(elapse.count()) / 1000); // NOLINT
}

auto main(int argc, char *argv[]) -> int {
  if (argc != 2) {
    std::cerr << fmt::format("Usage: {} <coord_cfg.toml>", argv[0]) // NOLINT
              << std::endl;
    std::exit(EXIT_FAILURE);
  }
  auto profile_ref = coord::profile_ref_t{};
  try {
    profile_ref = std::make_shared<coord::Profile>(
        coord::Profile::ParseToml(argv[1])); // NOLINT
  } catch (toml::exception &e) {
    std::cerr << "[Error] toml syntax error: " << e.what() << std::endl;
    std::exit(EXIT_FAILURE);
  } catch (std::invalid_argument &e) {
    std::cerr << "[Error] invalid argument: " << e.what() << std::endl;
    std::exit(EXIT_FAILURE);
  } catch (std::exception &e) {
    std::cerr << "[Error] failed to parse toml: " << e.what() << std::endl;
    std::exit(EXIT_FAILURE);
  }
  const auto &profile = *profile_ref.get();

  try {
    google::InitGoogleLogging(argv[0]); // NOLINT
    google::SetStderrLogging(google::GLOG_WARNING);
    const auto &log_file = profile.log_file.generic_string();
    google::SetLogDestination(google::GLOG_INFO,
                              fmt::format("{}.INFO", log_file).c_str());
    google::SetLogDestination(google::GLOG_WARNING,
                              fmt::format("{}.WARNING", log_file).c_str());
    google::SetLogDestination(google::GLOG_ERROR,
                              fmt::format("{}.ERROR", log_file).c_str());
    google::SetLogDestination(google::GLOG_FATAL,
                              fmt::format("{}.FATAL", log_file).c_str());
  } catch (std::exception &e) {
    std::cerr << "[Error] failed to initialize log: " << e.what() << std::endl;
    std::exit(EXIT_FAILURE);
  }

  std::cout << profile << std::flush;
  // LOG(INFO) << profile << std::flush;
  auto coord = std::unique_ptr<coord::Coordinator>{};
  try {
    coord = std::make_unique<coord::Coordinator>(profile_ref);
  } catch (std::exception &e) {
    std::cerr << fmt::format("[Error] fail to launch coordinator: {}", e.what())
              << std::endl;
    return EXIT_FAILURE;
  }

  try {
    switch (profile.action) {
    case coord::ActionType::BuildData: {
      coord->clear_meta();
      std::cout << "[Info] building data..." << std::endl;
      auto epoch = std::chrono::steady_clock::now();
      auto [stat, stripe_range, total_size] = coord->build_data();
      auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - epoch);
      std::cout << "[Info] done" << std::endl;
      std::cout << fmt::format("[Info] built {} stripes ({}..{}) in {} ms\n",
                               stripe_range.second - stripe_range.first,
                               stripe_range.first,
                               stripe_range.second,
                               elapse.count());
      std::cout << "[Info] Stripe stats:\n";
      for (auto &stat : stat) {
        const auto &[type, stripe_stat] = stat;
        std::cout << fmt::format("[Info] type: {}-{}, count: {}, size: {}MB\n",
                                 type.ec_type,
                                 type.blob_layout,
                                 stripe_stat.count,
                                 stripe_stat.size >> 20);
      }
      std::cout << fmt::format("[Info] time elapsed(ms): {}\n", elapse.count())
                << fmt::format("[Info] total size(MB): {}\n",
                               total_size >> 20) // NOLINT
                << fmt::format("[Info] throughput(MB/s): {:.2f}\n",
                               throughput(total_size, elapse))
                << std::endl;
      coord->persist();
    } break;
    case coord::ActionType::RepairChunk: {
      std::cout << fmt::format("[Info] repairing failed chunks...")
                << std::endl;
      auto epoch = std::chrono::steady_clock::now();
      coord->repair_chunk();
      auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - epoch);
      std::cout << "[Info] done" << std::endl;
      std::cout << "[Info] Repaired " << profile.test_load << " chunks in "
                << elapse.count() << "ms" << std::endl;
    } break;
    case coord::ActionType::RepairFailureDomain: {
      std::cout << "[Info] repairing failure domain..." << std::endl;
      auto epoch = std::chrono::steady_clock::now();
      auto [size] = coord->repair_failure_domain();
      auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - epoch);
      std::cout << "[Info] done" << std::endl;
      std::cout << fmt::format(
          "[Info] Repaired failure domain ({}MB) in {}ms\n",
          size >> 20, // NOLINT
          elapse.count());
      std::cout << fmt::format("[Info] throughput: {:.2f}MB/s",
                               throughput(size, elapse)) // NOLINT
                << std::endl;
    } break;
    case coord::ActionType::Read: {
      std::cout << "[Info] Reading trace..." << std::endl;
      auto epoch = std::chrono::steady_clock::now();
      auto [total_size] = coord->read();
      auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - epoch);
      std::cout << "[Info] done" << std::endl;
      std::cout << fmt::format("[Info] Read {} MB in {}ms\n",
                               total_size >> 20, // NOLINT
                               elapse.count());
      std::cout << fmt::format("[Info] throughput: {:.2f}MB/s",
                               throughput(total_size, elapse))
                << std::endl;
    } break;
    case coord::ActionType::DegradeRead: {
      std::cout << "[Info] Degrade reading trace..." << std::endl;
      auto epoch = std::chrono::steady_clock::now();
      auto [total_size] = coord->degrade_read();
      auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - epoch);
      std::cout << "[Info] done" << std::endl;
      std::cout << fmt::format("[Info] Degrade Read {} MB in {}ms\n",
                               total_size >> 20, // NOLINT
                               elapse.count());
      std::cout << fmt::format("[Info] throughput: {:.2f}MB/s",
                               throughput(total_size, elapse))
                << std::endl;
    } break;
    default:
      err::Unreachable("unknown action");
    }
  } catch (std::exception &e) {
    std::cerr << "[Error] " << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
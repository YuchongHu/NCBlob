/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-24 06:41:17
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-25 02:28:00
 * @FilePath: /tbr/task/Tasks.hh
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置
 * 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#pragma once

#include "BlockCommand.hh"
#include "Command.hh"
#include "meta.hpp"
#include <cstddef>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

// @axl TODO: the parameters of the following functions are not clear and
// misleading
// #suggestions:
// - compose a struct to hold all the parameters
// - return-by-value and return-by-reference are mixed, use one of them
//   - if you want to return multiple values, use a struct, or std::tuple, or
//   std::pair
// - some parameters are passed by value, which is not efficient
//   - pass by const reference instead, or use boost::span(see common/span.hpp)

namespace task::repair {
namespace pipeline {
namespace nsys {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<Command>, std::vector<meta::ip_t>>;
};
} // namespace nsys
namespace clay {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<Command>, std::vector<meta::ip_t>>;
};
} // namespace clay
namespace rs {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<Command>, std::vector<meta::ip_t>>;
};
} // namespace rs
} // namespace pipeline
namespace centralize {
namespace rs {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::size_t> offset;
  std::optional<std::size_t> size;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>>;
};
;
} // namespace rs
namespace clay {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::size_t> offset;
  std::optional<std::size_t> size;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>>;
};
} // namespace clay
namespace nsys {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::size_t> offset;
  std::optional<std::size_t> size;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>>;
};
} // namespace nsys
} // namespace centralize
} // namespace task::repair

namespace task::read {

namespace nsys {

struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  std::optional<std::size_t> offset;
  std::optional<std::size_t> size;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>>;
};
} // namespace nsys
namespace clay {
struct TaskBuilder {
  std::optional<meta::stripe_id_t> stripeId;
  std::optional<meta::chunk_index_t> chunk_index;
  std::optional<meta::ec_param_t> k;
  std::optional<meta::ec_param_t> m;
  // std::optional<std::size_t> offset;
  std::optional<std::size_t> size;
  std::optional<std::reference_wrapper<const std::vector<meta::disk_id_t>>>
      diskList;
  std::optional<std::reference_wrapper<const std::vector<std::string>>> ipList;
  auto build() -> std::pair<std::vector<BlockCommand>, std::vector<meta::ip_t>>;
};
} // namespace clay
} // namespace task::read
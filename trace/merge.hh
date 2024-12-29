#pragma once
/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-04 11:01:42
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-08-20 05:04:48
 * @FilePath: /lib_ec/src/merge/merge.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置
 * 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "utils.hpp"
#include <boost/core/span.hpp>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

namespace trace {
class ChunkMerge {
public:
public:
  ChunkMerge() = default;
  ChunkMerge(const ChunkMerge &) = default;
  auto operator=(const ChunkMerge &) -> ChunkMerge & = default;
  ChunkMerge(ChunkMerge &&) = default;
  auto operator=(ChunkMerge &&) -> ChunkMerge & = default;
  ~ChunkMerge() { flush_buffer(); }

  ChunkMerge(std::size_t chunk_size) : chunk_size(chunk_size) {}
  auto merge_stream(boost::span<char> in)
      -> std::pair<std::size_t, std::optional<std::vector<char>>>;
  auto flush_buffer() -> std::vector<char>;
  [[nodiscard]] auto merge_size() const -> std::size_t;

private:
  std::size_t chunk_size = 4 * MB;
  vector<char> buffer{};
};
} // namespace trace

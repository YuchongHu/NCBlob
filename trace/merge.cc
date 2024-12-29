/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-08-19 08:23:24
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-08-20 05:05:18
 * @FilePath: /lib_ec/src/tbr-merge/merge.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置
 * 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "merge.hh"
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>

auto trace::ChunkMerge::merge_stream(boost::span<char> in)
    -> std::pair<std::size_t, std::optional<std::vector<char>>> {
  if (in.size() == 0) {
    return {buffer.size(), std::nullopt};
  }

  std::vector<char> input(in.begin(), in.end());
  size_t offset = buffer.size();
  // buffer.insert(buffer.end(), input.begin(), input.end());
  std::copy(in.cbegin(), in.cend(), std::back_inserter(buffer));
  if (buffer.size() >= chunk_size) {
    auto chunk = flush_buffer();
    return {offset, chunk};
  } else {
    return {offset, std::nullopt};
  }
}

auto trace::ChunkMerge::flush_buffer() -> std::vector<char> {
  auto chunk = std::move(buffer);
  // reserve 108% of the chunk size
  buffer.reserve(chunk_size * 108 / 100); // NOLINT
  return chunk;
}
auto trace::ChunkMerge::merge_size() const -> std::size_t { return chunk_size; }

#pragma once

#include "azure_trace.hh"
#include "ec_intf.hh"
#include "exception.hpp"
#include "merge.hh"
#include "meta.hpp"
#include "size_lru_cache.hpp"

#include <boost/numeric/conversion/cast.hpp>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iterator>
#include <memory>
#include <queue>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>
namespace trace {
static inline constexpr std::size_t MIN_CHUNK_SIZE{1 << 10};
/// minimal merge size for the given ec type and parameters
inline static auto minimal_merge_size(meta::EcType ec_type, meta::ec_param_t k,
                                      meta::ec_param_t m) -> std::size_t {
  return k * MIN_CHUNK_SIZE;
}

namespace blob_stream {
struct MergeStreamInterface {
  inline static constexpr std::size_t EXTRA_SMALL_SIZE{32};
  MergeStreamInterface() = default;
  MergeStreamInterface(const MergeStreamInterface &) = default;
  auto
  operator=(const MergeStreamInterface &) -> MergeStreamInterface & = default;
  MergeStreamInterface(MergeStreamInterface &&) = default;
  auto operator=(MergeStreamInterface &&) -> MergeStreamInterface & = default;
  virtual ~MergeStreamInterface() = default;
  virtual auto
  next_merge() -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> = 0;
  virtual auto merge_size() const -> std::size_t = 0;
};

using MergeStreamInterfacePtr = std::unique_ptr<MergeStreamInterface>;

/// merge the blobs to the exactly fixed size
class FixedSizeMergeStream : virtual public MergeStreamInterface {
private:
  std::size_t merge_size_;
  std::vector<char> buf_{};
  TraceReaderPtr trace_reader_{};
  std::vector<meta::BlobMeta> blobs_{};

public:
  FixedSizeMergeStream(TraceReaderPtr trace_reader, std::size_t merge_size)
      : trace_reader_(std::move(trace_reader)), merge_size_(merge_size) {}
  [[nodiscard]] auto merge_size() const -> std::size_t override {
    return merge_size_;
  }

  auto next_merge()
      -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> override {
    thread_local std::random_device rd{};
    thread_local std::mt19937 gen(rd());
    thread_local std::uniform_int_distribution<char> dis{};
    auto rand_gen = [&]() { return static_cast<char>(dis(gen)); };
    while (true) {
      auto trace = trace_reader_->next_trace();
      if (trace.size < EXTRA_SMALL_SIZE) {
        // the blob is too small, skip
        continue;
      }
      // this is a new blob, merge
      if (trace.size > merge_size()) {
        // this blob is too large to merge, emit directly
        trace.size = merge_size();
        auto data = make_rand_data(trace);
        return std::make_pair(
            std::vector<meta::BlobMeta>{meta::BlobMeta{.blob_id = trace.blob_id,
                                                       .stripe_id = 0,
                                                       .blob_index = 0,
                                                       .size = trace.size,
                                                       .offset = 0}},
            std::move(data));
      } else if (buf_.size() + trace.size >= merge_size()) {
        // the merge buffer is full, emit the merged data
        trace.size = merge_size_ - buf_.size();
        auto offset = buf_.size();
        auto blob_index =
            boost::numeric_cast<meta::blob_index_t>(blobs_.size());
        std::generate_n(std::back_inserter(buf_), trace.size, rand_gen);
        blobs_.push_back(meta::BlobMeta{.blob_id = trace.blob_id,
                                        .stripe_id = 0,
                                        .blob_index = blob_index,
                                        .size = trace.size,
                                        .offset = offset});
        auto buf = std::move(buf_);
        buf_.reserve(merge_size());
        return std::make_pair(std::move(blobs_), buf);
      } else {
        // merge the blob to the buffer
        auto off = buf_.size();
        auto blob_index =
            boost::numeric_cast<meta::blob_index_t>(blobs_.size());
        std::generate_n(std::back_inserter(buf_), trace.size, rand_gen);
        blobs_.push_back(meta::BlobMeta{.blob_id = trace.blob_id,
                                        .stripe_id = 0,
                                        .blob_index = 0,
                                        .size = trace.size,
                                        .offset = off});
      }
    }
  }
};

/// emit the blobs immediately without merging
/// each emission is a single blob
class NoneMergeStream : virtual public MergeStreamInterface {
private:
  TraceReaderPtr azure_trace_;

public:
  NoneMergeStream(TraceReaderPtr trace_reader)
      : azure_trace_(std::move(trace_reader)) {}

  /// emit on each merge, so merge size is 0
  [[nodiscard]] auto merge_size() const -> std::size_t override { return 0; }

  auto next_merge()
      -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> override {
    while (true) {
      auto trace = azure_trace_->next_trace();
      if (trace.size < EXTRA_SMALL_SIZE) {
        // the blob is too small, skip
        continue;
      }
      // this is a new blob, emit
      return std::make_pair(
          std::vector<meta::BlobMeta>{meta::BlobMeta{.blob_id = trace.blob_id,
                                                     .stripe_id = 0,
                                                     .blob_index = 0,
                                                     .size = trace.size,
                                                     .offset = 0}},
          make_rand_data(trace));
    }
  }
};

/// emit the merged blobs when the merge buffer is full
/// # Note
/// - the blobs are not necessarily emitted in the order of arrival
/// - if a blob is larger than the merge buffer size,
///   it will be not merged and emitted immediately
/// - if a blob is larger than the rest of the merge buffer size,
///   it will be merged to this buffer and emitted immediately
class BasicMergeStream : virtual public MergeStreamInterface {
private:
  /// current blob trace under merging
  trace::ChunkMerge chunk_merge_{};
  /// log the offset of each blob under merging
  std::vector<meta::BlobMeta> blobs_{};
  TraceReaderPtr azure_trace_;

public:
  BasicMergeStream(TraceReaderPtr trace_reader, std::size_t merge_size)
      : azure_trace_(std::move(trace_reader)), chunk_merge_(merge_size) {}

  [[nodiscard]] auto merge_size() const -> std::size_t override {
    return chunk_merge_.merge_size();
  }

  auto next_merge()
      -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> override {
    try {
      // iterate the trace until exhaust
      while (true) {
        auto trace = azure_trace_->next_trace();
        if (trace.size < EXTRA_SMALL_SIZE) {
          // the blob is too small, skip
          continue;
        }
        // this is a new blob, merge
        auto data = make_rand_data(trace);
        if (trace.size > merge_size()) {
          // this blob is too large to merge, emit directly
          return std::make_pair(std::vector<meta::BlobMeta>{meta::BlobMeta{
                                    .blob_id = trace.blob_id,
                                    .stripe_id = 0,
                                    .blob_index = 0,
                                    .size = trace.size,
                                    .offset = 0}},
                                std::move(data));
        }
        auto [off, merged] = chunk_merge_.merge_stream(data);
        auto blob_index =
            boost::numeric_cast<meta::blob_index_t>(blobs_.size());
        blobs_.push_back(meta::BlobMeta{.blob_id = trace.blob_id,
                                        .stripe_id = 0,
                                        .blob_index = blob_index,
                                        .size = trace.size,
                                        .offset = off});
        if (merged.has_value()) {
          // the merge buffer is full, emit the merged data
          return std::make_pair(std::move(blobs_), std::move(merged).value());
        }
      }
    } catch (const TraceException &e) {
      if (e.error_enum() == trace_error_e::Exhaust) {
        // handle the under-merging data
        if (blobs_.empty()) {
          throw;
        }
        auto merge = chunk_merge_.flush_buffer();
        return std::make_pair(std::move(blobs_), std::move(merge));
      } else {
        throw;
      }
    }
  }
};

// emit the merged blobs when the merge buffer is full
// # Note
// - the blobs are not necessarily emitted in the order of arrival
// - if a blob is larger than the merge buffer size,
//   it will be not merged and emitted immediately
// - if the merge buffer of any id is full, it will be emitted immediately
class InterLocalityMergeStream : virtual public MergeStreamInterface {
private:
  std::size_t merge_size_{};
  std::map<std::uint64_t,
           std::pair<std::vector<meta::BlobMeta>, trace::ChunkMerge>>
      merge_map_{};
  // size_lru_cache::lru_cache<std::uint64_t> blob_lru_cache_;
  size_lru_cache::lru_cache<std::uint64_t> blob_lru_cache_;
  // the split-before-merge chunk merge, for the blobs that has no locality
  trace::ChunkMerge s_b_m_chunk_merge_{};
  std::vector<meta::BlobMeta> s_b_m_blobs_{};
  // the atomic size for the padding
  std::size_t atomic_size_{};
  /// current blob trace under merging
  // trace::ChunkMerge chunk_merge_{};
  /// log the offset of each blob under merging
  // std::vector<meta::BlobMeta> blobs_{};
  TraceReaderPtr azure_trace_;

  int hit_cnt = 0;
  int miss_cnt = 0;
  bool last_merge_has_locality_{false};

public:
  InterLocalityMergeStream(TraceReaderPtr trace_reader, std::size_t merge_size,
                           std::size_t lru_cache_size, std::size_t atomic_size)
      : azure_trace_(std::move(trace_reader)), merge_size_(merge_size),
        s_b_m_chunk_merge_(merge_size), blob_lru_cache_(lru_cache_size),
        atomic_size_(atomic_size) {}

  [[nodiscard]] auto merge_size() const -> std::size_t override {
    return merge_size_;
  }

  auto last_merge_locality() const -> bool { return last_merge_has_locality_; }

  auto next_merge()
      -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> override {
    try {
      // iterate the trace until exhaust
      while (true) {
        auto trace = azure_trace_->next_trace();
        auto contains = blob_lru_cache_.contains(trace.user_id);
        if (trace.size <= blob_lru_cache_.capacity()) {

          auto blob_size = trace.size;
          if (blob_size <= merge_size_ && !contains) {
            // padding for split-before-merge
            blob_size =
                (blob_size + atomic_size_ - 1) / atomic_size_ * atomic_size_;
          }
          blob_lru_cache_.insert(trace.user_id, blob_size);
        }

        if (trace.size < EXTRA_SMALL_SIZE) {
          // the blob is too small, skip
          continue;
        }
        // this is a new blob, merge
        auto data = make_rand_data(trace);
        if (trace.size > merge_size()) {
          // this blob is too large to merge, emit directly
          return std::make_pair(std::vector<meta::BlobMeta>{meta::BlobMeta{
                                    .blob_id = trace.blob_id,
                                    .stripe_id = 0,
                                    .blob_index = 0,
                                    .size = trace.size,
                                    .offset = 0}},
                                std::move(data));
        }

        // small blob
        if (contains) {
          // has locality, merge-before-split
          hit_cnt++;
          if (merge_map_.find(trace.user_id) == merge_map_.end()) {
            merge_map_.emplace(trace.user_id,
                               std::make_pair(std::vector<meta::BlobMeta>(),
                                              trace::ChunkMerge(merge_size_)));
          }
          auto &[blobs, chunk_merge] = merge_map_[trace.user_id];
          auto [off, merged] = chunk_merge.merge_stream(data);
          auto blob_index =
              boost::numeric_cast<meta::blob_index_t>(blobs.size());
          blobs.push_back(meta::BlobMeta{.blob_id = trace.blob_id,
                                         .stripe_id = 0,
                                         .blob_index = blob_index,
                                         .size = trace.size,
                                         .offset = off});
          if (merged.has_value()) {
            LOG(INFO) << "hit cnt ratio:" << hit_rate() << std::endl;
            // the any of the merge buffers is full, emit the merged data
            last_merge_has_locality_ = true;
            return std::make_pair(std::move(blobs), std::move(merged).value());
          }
        } else {
          // has no locality, split-before-merge
          // do the padding for each blob
          miss_cnt++;
          data.resize((data.size() + atomic_size_ - 1) / atomic_size_ *
                      atomic_size_);
          auto [off, merged] = s_b_m_chunk_merge_.merge_stream(data);
          auto blob_index =
              boost::numeric_cast<meta::blob_index_t>(s_b_m_blobs_.size());
          s_b_m_blobs_.push_back(meta::BlobMeta{.blob_id = trace.blob_id,
                                                .stripe_id = 0,
                                                .blob_index = blob_index,
                                                .size = data.size(),
                                                .offset = off});
          if (merged.has_value()) {
            // the merge buffer is full, emit the merged data
            auto raw_data = std::move(merged).value();
            // rearrange the data for the split-before-merge
            auto rearrange = std::vector<char>{};
            rearrange.reserve(raw_data.size());
            auto k = atomic_size_;
            for (meta::ec_param_t i = 0; i < k; ++i) {
              for (const auto &j : s_b_m_blobs_) {
                if (j.size % k != 0) {
                  throw std::runtime_error("blob not divisible by k");
                }
                auto offset = j.offset + i * (j.size / k);
                auto size = j.size / k;
                if (offset > raw_data.size() ||
                    offset + size > raw_data.size()) {
                  throw std::out_of_range("blob offset out of range");
                }
                auto begin =
                    raw_data.begin() +
                    boost::numeric_cast<decltype(raw_data)::difference_type>(
                        offset);
                std::copy_n(begin, size, std::back_inserter(rearrange));
              }
            }
            last_merge_has_locality_ = false;
            return std::make_pair(std::move(s_b_m_blobs_),
                                  std::move(rearrange));
          }
        }
      }
    } catch (const TraceException &e) {
      if (e.error_enum() == trace_error_e::Exhaust) {
        // handle the under-merging data
        if (merge_map_.empty()) {
          throw;
        }
        // flush each of the buffer in merge_map_ one by one
        auto [blobs, chunk_merge] = merge_map_.begin()->second;
        auto merge = chunk_merge.flush_buffer();
        merge_map_.erase(merge_map_.begin());
        return std::make_pair(std::move(blobs), std::move(merge));
      } else {
        throw;
      }
    }
  }

  [[nodiscard]] auto hit_rate() const -> double {
    return static_cast<double>(hit_cnt) / (hit_cnt + miss_cnt);
  }
};

class PaddingMergeStream : virtual public MergeStreamInterface {
private:
  /// current blob trace under merging
  trace::ChunkMerge chunk_merge_{};
  /// log the offset of each blob under merging
  std::vector<meta::BlobMeta> blobs_{};
  TraceReaderPtr azure_trace_;
  std::size_t atomic_size_{};

public:
  PaddingMergeStream(TraceReaderPtr trace_reader, std::size_t merge_size,
                     std::size_t atomic_size)
      : azure_trace_(std::move(trace_reader)), chunk_merge_(merge_size),
        atomic_size_(atomic_size) {}

  [[nodiscard]] auto merge_size() const -> std::size_t override {
    return chunk_merge_.merge_size();
  }

  auto next_merge()
      -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> override {
    try {
      // iterate the trace until exhaust
      while (true) {
        auto trace = azure_trace_->next_trace();
        if (trace.size < EXTRA_SMALL_SIZE) {
          // the blob is too small, skip
          continue;
        }
        // this is a new blob, merge
        auto data = make_rand_data(trace);

        if (trace.size > merge_size()) {
          // this blob is too large to merge, emit directly
          return std::make_pair(std::vector<meta::BlobMeta>{meta::BlobMeta{
                                    .blob_id = trace.blob_id,
                                    .stripe_id = 0,
                                    .blob_index = 0,
                                    .size = trace.size,
                                    .offset = 0}},
                                std::move(data));
        }

        // do the padding for each small blob
        data.resize((data.size() + atomic_size_ - 1) / atomic_size_ *
                    atomic_size_);

        auto [off, merged] = chunk_merge_.merge_stream(data);
        auto blob_index =
            boost::numeric_cast<meta::blob_index_t>(blobs_.size());
        blobs_.push_back(meta::BlobMeta{.blob_id = trace.blob_id,
                                        .stripe_id = 0,
                                        .blob_index = blob_index,
                                        .size = data.size(),
                                        .offset = off});
        if (merged.has_value()) {
          // the merge buffer is full, emit the merged data
          return std::make_pair(std::move(blobs_), std::move(merged).value());
        }
      }
    } catch (const TraceException &e) {
      if (e.error_enum() == trace_error_e::Exhaust) {
        // handle the under-merging data
        if (blobs_.empty()) {
          throw;
        }
        auto merge = chunk_merge_.flush_buffer();
        return std::make_pair(std::move(blobs_), std::move(merge));
      } else {
        throw;
      }
    }
  }
  // auto next_merge()
  //     -> std::pair<std::vector<meta::BlobMeta>, std::vector<char>> override
  //     {
  //   auto [blobs, raw_data] = merge_stream_.next_merge();
  //   // padding the raw data
  //   raw_data.resize((raw_data.size() + atomic_size_ - 1) / atomic_size_ *
  //                   atomic_size_);
  //   auto &last = blobs.back();
  //   last.size = raw_data.size() - last.offset;
  //   return std::make_pair(std::move(blobs), std::move(raw_data));
  // }
};

} // namespace blob_stream

namespace stripe_stream {

struct StripeStreamItem {
  std::vector<meta::BlobMeta> blobs;
  std::vector<std::vector<char>> stripe;
  meta::EcType ec_type;
  meta::BlobLayout blob_layout;
};

struct StripeStreamInterface {
  StripeStreamInterface() = default;
  StripeStreamInterface(const StripeStreamInterface &) = default;
  auto
  operator=(const StripeStreamInterface &) -> StripeStreamInterface & = default;
  StripeStreamInterface(StripeStreamInterface &&) = default;
  auto operator=(StripeStreamInterface &&) -> StripeStreamInterface & = default;
  virtual ~StripeStreamInterface() = default;
  virtual auto next_stripe() -> StripeStreamItem = 0;
};

namespace baseline {
/// apply one erasure codes for all the stripes
class StripeStream : virtual public StripeStreamInterface {
private:
  ::trace::blob_stream::MergeStreamInterfacePtr merge_stream_{};
  ec::encoder_ptr encoder_{};

public:
  StripeStream() = default;

  auto set_merge_stream(::trace::blob_stream::MergeStreamInterfacePtr stream)
      -> void {
    merge_stream_ = std::move(stream);
  }

  auto set_encoder(std::unique_ptr<ec::encoder::Encoder> encoder) -> void {
    encoder_ = std::move(encoder);
  }

  /// merge the following chunks
  /// # Return
  /// the next encoded stripe merged from the following chunks
  auto next_stripe() -> StripeStreamItem override {
    auto [blobs, raw_data] = merge_stream_->next_merge();
    // padding the raw data
    auto k = encoder_->get_km().first;
    raw_data.resize((raw_data.size() + k) / k * k);
    auto stripe = encoder_->encode(raw_data);
    return StripeStreamItem{.blobs = std::move(blobs),
                            .stripe = std::move(stripe),
                            .ec_type = encoder_->get_ec_type(),
                            .blob_layout = meta::BlobLayout::Horizontal};
  }
};

} // namespace baseline
namespace partition {
/// apply the large blob encoder for the large blob,
/// apply the small blob encoder for the merged blobs,
class StripeStream : virtual public StripeStreamInterface {
private:
  using stripe_t = std::vector<std::vector<char>>;
  ::trace::blob_stream::MergeStreamInterfacePtr merge_stream_{};
  ec::encoder_ptr large_blob_encoder_{};
  ec::encoder_ptr small_blob_encoder_{};
  std::size_t blob_cnt_{};
  std::size_t partition_size_{};
  std::queue<StripeStreamItem> remaining_stripe_{};

  /// parition the range [begin, end) recursively
  /// # example
  /// a 10.6M blob with 2MB partition size
  /// will be partitioned to { 8MB + 2MB + 0.6MB }
  auto partition(std::vector<char>::const_iterator &begin,
                 const std::vector<char>::const_iterator &end,
                 std::size_t partition_size) {
    if (std::distance(begin, end) < partition_size) {
      return;
    }
    partition(begin, end, partition_size * 2);
    if (std::distance(begin, end) < partition_size) {
      // the size left is smaller than the partition size
      return;
    }
    auto advance =
        boost::numeric_cast<std::vector<char>::difference_type>(partition_size);
    auto raw_data = std::vector<char>{begin, begin + advance};
    auto stripe = large_blob_encoder_->encode(raw_data);
    remaining_stripe_.push(
        StripeStreamItem{.blobs = {meta::BlobMeta{.blob_id = blob_cnt_++,
                                                  .stripe_id = 0,
                                                  .blob_index = 0,
                                                  .size = raw_data.size(),
                                                  .offset = 0}},
                         .stripe = std::move(stripe),
                         .ec_type = large_blob_encoder_->get_ec_type(),
                         .blob_layout = meta::BlobLayout::Horizontal});
    begin += advance;
    return;
  }

public:
  StripeStream(std::size_t partition_size) : partition_size_(partition_size) {};

  auto set_merge_stream(::trace::blob_stream::MergeStreamInterfacePtr stream)
      -> void {
    merge_stream_ = std::move(stream);
  }

  auto set_large_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    large_blob_encoder_ = std::move(encoder);
  }

  auto set_small_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    small_blob_encoder_ = std::move(encoder);
  }
  /// merge the following chunks
  /// # Return
  /// the next encoded stripe merged from the following chunks
  auto next_stripe() -> StripeStreamItem override {
    if (!remaining_stripe_.empty()) {
      auto item = std::move(remaining_stripe_.front());
      remaining_stripe_.pop();
      return item;
    }
    auto [blobs, raw_data] = merge_stream_->next_merge();
    if (raw_data.size() >= partition_size_) {
      // large blob, do partition
      auto k = large_blob_encoder_->get_km().first;
      raw_data.resize((raw_data.size() + k) / k * k);
      // partition the large data
      auto cur_off = raw_data.cbegin();
      partition(cur_off, raw_data.cend(), partition_size_);
      if (std::distance(cur_off, raw_data.cend()) > 0) {
        auto small_partition = std::vector<char>{cur_off, raw_data.cend()};
        auto stripe = small_blob_encoder_->encode(small_partition);
        remaining_stripe_.push(StripeStreamItem{
            .blobs = {meta::BlobMeta{.blob_id = blob_cnt_++,
                                     .stripe_id = 0,
                                     .blob_index = 0,
                                     .size = small_partition.size(),
                                     .offset = 0}},
            .stripe = std::move(stripe),
            .ec_type = small_blob_encoder_->get_ec_type(),
            .blob_layout = meta::BlobLayout::Horizontal});
      }

      auto item = std::move(remaining_stripe_.front());
      remaining_stripe_.pop();
      return item;
    } else {
      // small blob
      auto k = small_blob_encoder_->get_km().first;
      raw_data.resize((raw_data.size() + k) / k * k);
      auto stripe = small_blob_encoder_->encode(raw_data);
      return StripeStreamItem{.blobs = std::move(blobs),
                              .stripe = std::move(stripe),
                              .ec_type = small_blob_encoder_->get_ec_type(),
                              .blob_layout = meta::BlobLayout::Horizontal};
    }
  }
};
} // namespace partition

namespace hybrid {

/// apply the large blob encoder for the large blob,
/// apply the small blob encoder for the merged blobs,
/// we split each blob to sub-blobs before merging,
/// so the blob layout is vertical
class SplitBeforeMerge : virtual public StripeStreamInterface {
private:
  trace::blob_stream::PaddingMergeStream merge_stream_;
  ec::encoder_ptr large_blob_encoder_{};
  ec::encoder_ptr small_blob_encoder_{};

public:
  SplitBeforeMerge(TraceReaderPtr trace_reader, std::size_t merge_size,
                   std::unique_ptr<ec::encoder::Encoder> large_blob_encoder,
                   std::unique_ptr<ec::encoder::Encoder> small_blob_encoder)
      : merge_stream_(std::move(trace_reader), merge_size,
                      small_blob_encoder->get_km().first),
        large_blob_encoder_(std::move(large_blob_encoder)),
        small_blob_encoder_(std::move(small_blob_encoder)) {}

  auto set_large_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    large_blob_encoder_ = std::move(encoder);
  }

  auto set_small_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    small_blob_encoder_ = std::move(encoder);
  }

  auto next_stripe() -> StripeStreamItem override {
    auto [blobs, raw_data] = merge_stream_.next_merge();
    if (blobs.size() == 1 && raw_data.size() > merge_stream_.merge_size()) {
      // large blob
      auto stripe = large_blob_encoder_->encode(raw_data);
      return {.blobs = std::move(blobs),
              .stripe = std::move(stripe),
              .ec_type = large_blob_encoder_->get_ec_type()};
    } else {
      // merged small blobs
      auto rearrange = std::vector<char>{};
      rearrange.reserve(raw_data.size());
      auto k = small_blob_encoder_->get_km().first;
      for (meta::ec_param_t i = 0; i < k; ++i) {
        for (const auto &j : blobs) {
          // if (j.size % k != 0) {
          //   throw std::runtime_error("blob not divisible by k");
          // }
          auto offset = j.offset + i * (j.size / k);
          auto size = j.size / k;
          if (offset > raw_data.size() || offset + size > raw_data.size()) {
            throw std::out_of_range("blob offset out of range");
          }
          auto begin =
              raw_data.begin() +
              boost::numeric_cast<decltype(raw_data)::difference_type>(offset);
          std::copy_n(begin, size, std::back_inserter(rearrange));
        }
      }
      auto stripe = small_blob_encoder_->encode(rearrange);
      return {.blobs = std::move(blobs),
              .stripe = std::move(stripe),
              .ec_type = small_blob_encoder_->get_ec_type(),
              .blob_layout = meta::BlobLayout::Vertical};
    }
  };
};

/// apply the large blob encoder for the large blob,
/// apply the small blob encoder for the merged blobs,
/// we merge the blobs before splitting,
/// so the blob layout is horizontal
class MergeBeforeSplit : virtual public StripeStreamInterface {
private:
  trace::blob_stream::BasicMergeStream merge_stream_;
  ec::encoder_ptr large_blob_encoder_{};
  ec::encoder_ptr small_blob_encoder_{};

public:
  MergeBeforeSplit(TraceReaderPtr trace_reader, std::size_t merge_size,
                   std::unique_ptr<ec::encoder::Encoder> large_blob_encoder,
                   std::unique_ptr<ec::encoder::Encoder> small_blob_encoder)
      : merge_stream_(std::move(trace_reader), merge_size),
        large_blob_encoder_(std::move(large_blob_encoder)),
        small_blob_encoder_(std::move(small_blob_encoder)) {}

  auto set_large_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    large_blob_encoder_ = std::move(encoder);
  }
  auto set_small_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    small_blob_encoder_ = std::move(encoder);
  }
  auto next_stripe() -> StripeStreamItem override {
    auto [blobs, raw_data] = merge_stream_.next_merge();
    if (blobs.size() == 1 && raw_data.size() > merge_stream_.merge_size()) {
      // large blob
      auto stripe = large_blob_encoder_->encode(raw_data);
      return {.blobs = std::move(blobs),
              .stripe = std::move(stripe),
              .ec_type = large_blob_encoder_->get_ec_type()};
    } else {
      // merged small blobs
      auto stripe = small_blob_encoder_->encode(raw_data);
      return {.blobs = std::move(blobs),
              .stripe = std::move(stripe),
              .ec_type = small_blob_encoder_->get_ec_type(),
              .blob_layout = meta::BlobLayout::Horizontal};
    }
  }
};

class InterLocality : virtual public StripeStreamInterface {
private:
  trace::blob_stream::InterLocalityMergeStream merge_stream_;
  ec::encoder_ptr large_blob_encoder_{};
  ec::encoder_ptr small_blob_encoder_{};

public:
  InterLocality(TraceReaderPtr trace_reader, std::size_t merge_size,
                std::unique_ptr<ec::encoder::Encoder> large_blob_encoder,
                std::unique_ptr<ec::encoder::Encoder> small_blob_encoder,
                std::size_t lru_cache_size)
      : merge_stream_(std::move(trace_reader), merge_size, lru_cache_size,
                      small_blob_encoder->get_km().first),
        large_blob_encoder_(std::move(large_blob_encoder)),
        small_blob_encoder_(std::move(small_blob_encoder)) {}

  auto set_large_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    large_blob_encoder_ = std::move(encoder);
  }
  auto set_small_blob_encoder(std::unique_ptr<ec::encoder::Encoder> encoder)
      -> void {
    small_blob_encoder_ = std::move(encoder);
  }
  auto next_stripe() -> StripeStreamItem override {

    auto [blobs, raw_data] = merge_stream_.next_merge();
    if (blobs.size() == 1 && raw_data.size() > merge_stream_.merge_size()) {
      // large blob
      auto stripe = large_blob_encoder_->encode(raw_data);
      return {.blobs = std::move(blobs),
              .stripe = std::move(stripe),
              .ec_type = large_blob_encoder_->get_ec_type(),
              .blob_layout = meta::BlobLayout::Horizontal};
    } else {
      // small blob
      auto stripe = small_blob_encoder_->encode(raw_data);
      auto blob_layout = meta::BlobLayout::Horizontal;
      if (merge_stream_.last_merge_locality()) {
        blob_layout = meta::BlobLayout::Horizontal;
      } else {
        blob_layout = meta::BlobLayout::Vertical;
      }
      return {.blobs = std::move(blobs),
              .stripe = std::move(stripe),
              .ec_type = small_blob_encoder_->get_ec_type(),
              .blob_layout = blob_layout};
    }
  }

  auto hit_rate() const -> double { return merge_stream_.hit_rate(); }
};

} // namespace hybrid
namespace degrade_read {
/// all the blobs are the same size,
/// all the chunks are the same size,
/// and a stripe has only one blob
/// # Note
/// currently only support clay
class IntraLocality : virtual public StripeStreamInterface {
private:
  /// block size equals to the k * chunk size
  std::size_t block_size_{};
  meta::blob_id_t cur_blob_id_{};
  ec::encoder_ptr encoder_{};

public:
  IntraLocality(ec::encoder_ptr encoder, std::size_t block_size)
      : block_size_(block_size), encoder_(std::move(encoder)) {
    if (encoder_->get_ec_type() != meta::EcType::CLAY) {
      err::Unimplemented("intralocality for degrade read only suppurt clay");
    }
  }
  auto next_stripe() -> StripeStreamItem override {
    auto raw_data = std::vector<char>{};
    constexpr std::size_t RAND_SEED{0x9b648};
    raw_data.reserve(block_size_);
    std::generate_n(std::back_inserter(raw_data), block_size_, [] {
      thread_local auto gen = std::mt19937(RAND_SEED);
      thread_local auto dist = std::uniform_int_distribution<char>{};
      return dist(gen);
    });
    auto stripe = encoder_->encode(raw_data);
    auto blobs =
        std::vector<meta::BlobMeta>{meta::BlobMeta{.blob_id = cur_blob_id_++,
                                                   .stripe_id = 0,
                                                   .blob_index = 0,
                                                   .size = block_size_,
                                                   .offset = 0}};
    return {.blobs = std::move(blobs),
            .stripe = std::move(stripe),
            .ec_type = encoder_->get_ec_type(),
            .blob_layout = meta::BlobLayout::Horizontal};
  }
};

/// all the blobs are the same size,
/// all the chunks are the same size,
/// and a stripe has several blobs
/// # Note
/// currently only support NSYS
class InterLocality : virtual public StripeStreamInterface {
private:
  std::size_t block_size_{};
  std::size_t blob_size_{};
  meta::blob_id_t cur_blob_id_{};
  ec::encoder_ptr encoder_{};

public:
  InterLocality(ec::encoder_ptr encoder, std::size_t block_size,
                std::size_t blob_size)
      : block_size_(block_size), encoder_(std::move(encoder)),
        blob_size_(blob_size) {
    if (encoder_->get_ec_type() != meta::EcType::NSYS) {
      err::Unimplemented("interlocality for degrade read only suppurt nsys");
    }
    if (block_size % blob_size != 0) {
      throw std::runtime_error("block size not divisible by blob size");
    }
  }

  auto next_stripe() -> StripeStreamItem override {
    auto raw_data = std::vector<char>{};
    constexpr std::size_t RAND_SEED{0x9b648};
    raw_data.reserve(block_size_);
    std::generate_n(std::back_inserter(raw_data), block_size_, [] {
      thread_local auto gen = std::mt19937(RAND_SEED);
      thread_local auto dist = std::uniform_int_distribution<char>{};
      return dist(gen);
    });
    auto stripe = encoder_->encode(raw_data);
    auto blobs = std::vector<meta::BlobMeta>{};
    auto num_of_blobs = block_size_ / blob_size_;
    blobs.reserve(num_of_blobs);
    for (std::size_t i = 0; i < num_of_blobs; i++) {
      blobs.emplace_back(meta::BlobMeta{
          .blob_id = cur_blob_id_++,
          .stripe_id = 0,
          .blob_index = boost::numeric_cast<meta::blob_index_t>(i),
          .size = blob_size_,
          .offset = i * blob_size_});
    }
    return {.blobs = std::move(blobs),
            .stripe = std::move(stripe),
            .ec_type = encoder_->get_ec_type(),
            .blob_layout = meta::BlobLayout::Vertical};
  }
};
} // namespace degrade_read

} // namespace stripe_stream

} // namespace trace

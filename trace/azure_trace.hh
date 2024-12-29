#pragma once

#include "azure_trace.rs.h"
#include "meta.hpp"

#include <exception>
#include <filesystem>
#include <memory>
#include <unordered_set>
#include <vector>

namespace trace {
using azure_trace_rs::BlobAccessTrace;
using trace_error_e = azure_trace_rs::TraceError;

struct TraceException : std::exception {
  trace_error_e err_;
  TraceException(trace_error_e error) : err_(error) {}
  [[nodiscard]] auto error_enum() const -> trace_error_e;

  [[nodiscard]] auto what() const noexcept -> const char * override;
};

class TraceReader {
public:
  virtual ~TraceReader() = default;
  TraceReader() = default;
  TraceReader(const TraceReader &) = default;
  auto operator=(const TraceReader &) -> TraceReader & = default;
  TraceReader(TraceReader &&) = default;
  auto operator=(TraceReader &&) -> TraceReader & = default;
  virtual auto next_trace() -> BlobAccessTrace = 0;
};

using TraceReaderPtr = std::unique_ptr<TraceReader>;
class AzureTraceReader : virtual public TraceReader {
private:
  using trace_ref_t = rust::Box<azure_trace_rs::reader>;
  trace_ref_t trace_ref_;

public:
  AzureTraceReader(const std::filesystem::path &trace_file);
  AzureTraceReader(const AzureTraceReader &) = delete;
  auto operator=(const AzureTraceReader &) -> AzureTraceReader & = delete;
  AzureTraceReader(AzureTraceReader &&) = default;
  auto operator=(AzureTraceReader &&) -> AzureTraceReader & = default;
  ~AzureTraceReader() override = default;

  [[nodiscard]] auto next_trace() -> BlobAccessTrace override;
};

auto make_rand_data(const BlobAccessTrace &trace) -> std::vector<char>;

class DedupTraceReader : virtual public TraceReader {
  class BlobIdTracker {
  private:
    std::unordered_set<meta::blob_id_t> tracker_{};

  public:
    // track this blob, and return true if this blob is not tracked before
    auto track(const BlobAccessTrace &trace) -> bool;
  };

private:
  BlobIdTracker blob_tracker_{};
  std::unique_ptr<TraceReader> trace_reader_;

public:
  DedupTraceReader(std::unique_ptr<TraceReader> trace_reader)
      : trace_reader_(std::move(trace_reader)) {}

  [[nodiscard]] auto next_trace() -> BlobAccessTrace override {
    while (true) {
      auto trace = trace_reader_->next_trace();
      if (blob_tracker_.track(trace)) {
        return trace;
      }
    }
  };
};

class StepByTraceReader : virtual public TraceReader {
private:
  std::unique_ptr<TraceReader> trace_reader_;
  std::size_t step_;

public:
  StepByTraceReader(std::unique_ptr<TraceReader> trace_reader, std::size_t step)
      : trace_reader_(std::move(trace_reader)), step_(step) {}

  [[nodiscard]] auto next_trace() -> BlobAccessTrace override {
    for (std::size_t i = 0; i < step_; i++) {
      trace_reader_->next_trace();
    }
    return trace_reader_->next_trace();
  };
};

inline auto make_azure_trace(const std::filesystem::path &trace_file,
                             std::size_t step_by) -> TraceReaderPtr {
  auto trace = std::make_unique<AzureTraceReader>(trace_file);
  auto dedup = std::make_unique<DedupTraceReader>(std::move(trace));
  if (step_by > 1) {
    return std::make_unique<StepByTraceReader>(std::move(dedup), step_by);
  } else {
    return dedup;
  }
}

} // namespace trace
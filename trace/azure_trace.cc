#include "azure_trace.hh"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <iterator>
#include <random>
#include <string>
#include <utility>

auto trace::DedupTraceReader::BlobIdTracker::track(const BlobAccessTrace &trace)
    -> bool {
  return tracker_.insert(trace.blob_id).second;
}
auto trace::make_rand_data(const BlobAccessTrace &trace) -> std::vector<char> {
  std::vector<char> data{};
  data.reserve(trace.size);
  // generate random data
  constexpr std::size_t RAND_SEED{0x9b648};
  thread_local std::mt19937 gen(RAND_SEED);
  thread_local std::uniform_int_distribution<int> dis{};

  std::generate_n(std::back_inserter(data), trace.size, [&]() {
    return static_cast<char>(dis(gen));
  });
  return data;
}
auto trace::AzureTraceReader::next_trace() -> BlobAccessTrace {
  try {
    while (true) {
      auto record = trace_ref_->next_record();
      if (record.size == 0) {
        continue;
      }
      return record;
    }
  } catch (std::exception &e) {
    // convert to TraceException
    auto trace_e = azure_trace_rs::str_to_err(e.what());
    throw TraceException(trace_e);
  }
}
trace::AzureTraceReader::AzureTraceReader(
    const std::filesystem::path &trace_file)
    : trace_ref_(azure_trace_rs::open_reader(trace_file.string().c_str())) {}
auto trace::TraceException::what() const noexcept -> const char * {
  // this rust::str is 'static
  return azure_trace_rs::err_to_str(err_).data();
}
auto trace::TraceException::error_enum() const -> trace_error_e { return err_; }

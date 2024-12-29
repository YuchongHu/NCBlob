#pragma once

#include <boost/lockfree/spsc_queue.hpp>
#include <thread>
namespace util {

inline static constexpr std::size_t CHANNEL_CAP{64};

template <typename T>
using channel_ref = std::shared_ptr<
    boost::lockfree::spsc_queue<T, boost::lockfree::capacity<CHANNEL_CAP>>>;

template <typename T, bool EAGER> class ChannelSink;
template <typename T, bool EAGER> class ChannelStream;

template <typename T, bool EAGER>
auto make_channel()
    -> std::pair<ChannelSink<T, EAGER>, ChannelStream<T, EAGER>>;

template <typename T, bool EAGER = true> class ChannelSink {
private:
  friend auto make_channel<T, EAGER>()
      -> std::pair<ChannelSink<T, EAGER>, ChannelStream<T, EAGER>>;
  channel_ref<T> sink_;

  explicit ChannelSink(channel_ref<T> sink) : sink_(std::move(sink)) {};

public:
  ChannelSink() = delete;
  ChannelSink(const ChannelSink &) = default;
  auto operator=(const ChannelSink &) -> ChannelSink & = default;
  ChannelSink(ChannelSink &&) = default;
  auto operator=(ChannelSink &&) -> ChannelSink & = default;
  ~ChannelSink() = default;
  auto operator<<(const T &obj) -> ChannelSink & {
    while (sink_->write_available() == 0) {
      if constexpr (not EAGER) {
        std::this_thread::yield();
      }
    }
    sink_->push(obj);
    return *this;
  }
  auto available() -> std::size_t { return sink_->write_available(); }
};

template <typename T, bool EAGER = true> class ChannelStream {
private:
  friend auto make_channel<T, EAGER>()
      -> std::pair<ChannelSink<T, EAGER>, ChannelStream<T, EAGER>>;
  channel_ref<T> stream_;

  explicit ChannelStream(channel_ref<T> stream) : stream_(std::move(stream)) {};

public:
  ChannelStream() = delete;
  ChannelStream(const ChannelStream &) = default;
  auto operator=(const ChannelStream &) -> ChannelStream & = default;
  ChannelStream(ChannelStream &&) = default;
  auto operator=(ChannelStream &&) -> ChannelStream & = default;
  ~ChannelStream() = default;

  auto operator>>(T &obj) -> ChannelStream & {
    while (stream_->read_available() == 0) {
      if constexpr (not EAGER) {
        std::this_thread::yield();
      }
    }
    stream_->pop(obj);
    return *this;
  }

  auto available() -> std::size_t { return stream_->read_available(); }
};

template <typename T, bool EAGER>
auto make_channel()
    -> std::pair<ChannelSink<T, EAGER>, ChannelStream<T, EAGER>> {
  auto channel = std::make_shared<
      boost::lockfree::spsc_queue<T, boost::lockfree::capacity<CHANNEL_CAP>>>();
  return {ChannelSink<T, EAGER>{channel}, ChannelStream<T, EAGER>{channel}};
}
} // namespace util
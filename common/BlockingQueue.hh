#pragma once
#include <boost/circular_buffer.hpp>

#include <boost/circular_buffer/base.hpp>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <utility>

/// Blocking queue with fixed capacity
template <class T> class BlockingQueue {
private:
  using mutex_t = std::mutex;
  mutex_t mutex_{};
  std::condition_variable not_full_cv_{};
  std::condition_variable not_empty_cv_{};
  boost::circular_buffer<T> queue_{};
  std::size_t capacity_{};

public:
  static constexpr std::size_t DEFAULT_CAPACITY = 32;
  /// Build bounded blocking queue with default capacity
  BlockingQueue() : BlockingQueue(DEFAULT_CAPACITY) {}
  BlockingQueue(std::size_t capacity) : capacity_(capacity) {}

  void push(T value) {
    {
      std::unique_lock<mutex_t> lock(mutex_);
      not_full_cv_.wait(lock,
                        [this]() { return queue_.size() < this->capacity_; });
      queue_.push_back(std::move(value));
    }
    not_empty_cv_.notify_one();
  };

  T pop() {
    std::unique_lock<mutex_t> lock(mutex_);
    not_empty_cv_.wait(lock, [this] { return !this->queue_.empty(); });
    T toret{std::move(queue_.front())};
    queue_.pop_front();
    not_full_cv_.notify_one();

    return toret;
  };

  std::size_t size() const { return queue_.size(); }
  bool empty() const { return queue_.empty(); }
};

#pragma once

#include <boost/smart_ptr/make_shared_array.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace util {

class SharedVec {
private:
  // NOLINTBEGIN (cppcoreguidelines-non-private-member-variables-in-classes)
  boost::shared_ptr<std::byte[]> data_{};
  std::size_t size_{};

  SharedVec(const std::string_view data) : SharedVec(data.size()) {
    std::copy(
        data.cbegin(), data.cend(), reinterpret_cast<char *>(data_.get()));
  }

public:
  SharedVec() = default;
  static auto from_str(const std::string_view data) -> SharedVec {
    return SharedVec(data);
  }
  static auto with_size(std::size_t size) -> SharedVec {
    return SharedVec(size);
  }
  SharedVec(std::size_t size)
      : data_(boost::make_shared<std::byte[]>(size)), size_(size) {}
  SharedVec(const SharedVec &other) = default;
  SharedVec(SharedVec &&other) noexcept = default;
  auto operator=(const SharedVec &other) -> SharedVec & = default;
  auto operator=(SharedVec &&other) noexcept -> SharedVec & = default;
  ~SharedVec() = default;
  template <typename B> auto span() -> std::span<B> {
    static_assert(sizeof(B) == sizeof(std::byte),
                  "type B is not in size of std::byte");
    return std::span<B>(reinterpret_cast<B *>(data_.get()), size_);
  }
  template <typename B> auto cspan() const -> std::span<const B> {
    static_assert(sizeof(B) == sizeof(std::byte),
                  "type B is not in size of std::byte");
    return std::span<const B>(reinterpret_cast<const B *>(data_.get()), size_);
  }
  auto as_bytes() -> std::span<std::byte> { return span<std::byte>(); }
  auto as_cbytes() const -> std::span<const std::byte> {
    return cspan<std::byte>();
  }
  auto as_cstr() const -> const std::string_view {
    return std::string_view(reinterpret_cast<const char *>(data_.get()), size_);
  }
  auto as_str() -> std::string_view {
    return std::string_view(reinterpret_cast<char *>(data_.get()), size_);
  }
  auto data() const -> const std::byte * { return data_.get(); }
  auto data() -> std::byte * { return data_.get(); }
  auto u8_data() const -> const std::uint8_t * {
    return reinterpret_cast<const std::uint8_t *>(data_.get());
  }
  auto u8_data() -> std::uint8_t * {
    return reinterpret_cast<std::uint8_t *>(data_.get());
  }
  auto i8_data() const -> const std::int8_t * {
    return reinterpret_cast<const std::int8_t *>(data_.get());
  }
  auto i8_data() -> std::int8_t * {
    return reinterpret_cast<std::int8_t *>(data_.get());
  }
  auto size() const -> std::size_t { return size_; }
};
// NOLINTEND (cppcoreguidelines-non-private-member-variables-in-classes)
} // namespace util
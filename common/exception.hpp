#pragma once

#include <fmt/format.h>

#include <memory>
#include <source_location>
#include <string_view>

namespace err {

enum class ExceptionType : std::uint8_t {
  Exception = 0,
  Todo,
  Unimplemented,
};

inline auto to_string(ExceptionType type) -> std::string {
  switch (type) {
  case ExceptionType::Todo:
    return "TODO";
  case ExceptionType::Unimplemented:
    return "Unimplemented";
  case ExceptionType::Exception:
    return "Exception";
  default:
    return "Unknown";
  }
}

inline auto format_as(ExceptionType obj) { return to_string(obj); }

class Exception {
private:
  std::shared_ptr<std::string> msg_{};

public:
  Exception(
      std::source_location source_location = std::source_location::current())
      : err::Exception(err::ExceptionType::Exception, source_location) {};
  Exception(std::string_view msg, std::source_location source_location =
                                      std::source_location::current())
      : err::Exception(err::ExceptionType::Exception, msg, source_location) {};
  Exception(ExceptionType type, std::source_location source_location =
                                    std::source_location::current())
      : msg_{std::make_shared<std::string>(
            fmt::format("{} At \"{}:{}\"", type, source_location.file_name(),
                        source_location.line()))} {};
  Exception(
      ExceptionType type, std::string_view msg,
      std::source_location source_location = std::source_location::current())
      : msg_{std::make_shared<std::string>(fmt::format(
            "{} {} At \"{}:{}\"", type, msg, source_location.file_name(),
            source_location.line()))} {};
  Exception(const Exception &other) = default;
  Exception(Exception &&other) noexcept = default;
  auto operator=(const Exception &other) -> Exception & {
    if (this != &other) {
      msg_ = other.msg_;
    }
    return *this;
  };
  auto operator=(Exception &&other) noexcept -> Exception & {
    if (this != &other) {
      msg_ = std::move(other.msg_);
    }
    return *this;
  };
  ~Exception() = default;
};

inline auto
Todo(std::string_view msg = "TODO",
     std::source_location source_location = std::source_location::current()) {
  throw Exception{ExceptionType::Todo, msg, source_location};
}

inline auto Unimplemented(
    std::string_view msg = "Unimplemented",
    std::source_location source_location = std::source_location::current()) {
  throw Exception{ExceptionType::Unimplemented, msg, source_location};
}

inline auto Unreachable(
    std::string_view msg = "Unreachable",
    std::source_location source_location = std::source_location::current()) {
  throw Exception{ExceptionType::Exception, "Unreachable"};
}

} // namespace err
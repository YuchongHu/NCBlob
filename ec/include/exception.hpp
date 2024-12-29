
/*!
 * @file exception.hpp
 * @brief exception definition for libec
 */
#ifndef LIBEC_EXCEPTION_HPP
#define LIBEC_EXCEPTION_HPP

#include <exception>
#include <iomanip>
#include <ostream>
// #include <source_location>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

namespace ec {

class EcException : public std::exception {
public:
  EcException(const EcException &) = default;
  EcException(EcException &&) noexcept = default;
  auto operator=(const EcException &) -> EcException & = default;
  auto operator=(EcException &&) noexcept -> EcException & = default;

  explicit EcException(
      std::string description = std::string{"libec exception"})
      : description_(std::move(description)) {}

  auto to_string() -> std::string {
    std::stringstream buf{};
    // clang-format off
    buf << std::quoted(this->description_);
    // clang-format on
    return std::move(buf).str();
  }

private:
  std::string description_;
};

inline auto operator<<(std::ostream &os, EcException &e) -> std::ostream & {
  os << e.to_string();
  return os;
}

inline auto EcMsgAssert(bool pred, std::string msg) -> void {
  if (not pred) {
    throw EcException{std::move(msg)};
  }
}

inline auto EcAssert(bool pred) -> void {
  EcMsgAssert(pred, "libec assertion fail");
}

namespace detail {
inline auto Panic(std::string panic_name, std::string_view description) -> void {
  if (not description.empty()) {
    panic_name.append(": ");
    panic_name.append(description);
  }
  throw EcException{panic_name};
}
} // namespace detail

inline auto
Todo(std::string_view description = "")
    -> void {
  detail::Panic(std::string{"todo"}, description);
}

inline auto Unimplemented(std::string_view description = "") -> void {
  detail::Panic(std::string{"unimplemented"}, description);
}

}; // namespace ec
#endif
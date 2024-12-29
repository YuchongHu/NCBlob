#pragma once

#include <memory>
#include <string>

namespace meta {

class Exception : public std::exception {
private:
  std::shared_ptr<std::string> msg_;

public:
  explicit Exception(const std::string &msg)
      : msg_{std::make_shared<std::string>("Meta Error: " + msg)} {}

  [[nodiscard]] const char *what() const noexcept override {
    return msg_->c_str();
  }
};

class NotFound : public Exception {
public:
  explicit NotFound(const std::string &msg) : Exception("not found") {}
};

} // namespace meta
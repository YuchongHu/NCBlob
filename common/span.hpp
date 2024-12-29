#pragma once

#include <span>

namespace util {
using bytes_span = std::span<const char>;
using bytes_mut_span = std::span<char>;
} // namespace util
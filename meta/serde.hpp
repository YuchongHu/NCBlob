#pragma once

#include "span.hpp"

#include <msgpack.hpp>

namespace serde {
template <typename T> void serialize(const T &obj, std::ostream &stream) {
  msgpack::pack(stream, obj);
};

template <typename T> void deserialize(util::bytes_span buf, T &obj) {
  msgpack::object_handle oh = msgpack::unpack(buf.data(), buf.size());
  msgpack::object obj_ = oh.get();
  obj_.convert(obj);
};
} // namespace serde
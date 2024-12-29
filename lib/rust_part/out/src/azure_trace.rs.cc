#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

namespace rust {
inline namespace cxxbridge1 {
// #include "rust/cxx.h"

namespace {
template <typename T>
class impl;
} // namespace

class String;

#ifndef CXXBRIDGE1_RUST_STR
#define CXXBRIDGE1_RUST_STR
class Str final {
public:
  Str() noexcept;
  Str(const String &) noexcept;
  Str(const std::string &);
  Str(const char *);
  Str(const char *, std::size_t);

  Str &operator=(const Str &) &noexcept = default;

  explicit operator std::string() const;

  const char *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  Str(const Str &) noexcept = default;
  ~Str() noexcept = default;

  using iterator = const char *;
  using const_iterator = const char *;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  bool operator==(const Str &) const noexcept;
  bool operator!=(const Str &) const noexcept;
  bool operator<(const Str &) const noexcept;
  bool operator<=(const Str &) const noexcept;
  bool operator>(const Str &) const noexcept;
  bool operator>=(const Str &) const noexcept;

  void swap(Str &) noexcept;

private:
  class uninit;
  Str(uninit) noexcept;
  friend impl<Str>;

  std::array<std::uintptr_t, 2> repr;
};
#endif // CXXBRIDGE1_RUST_STR

#ifndef CXXBRIDGE1_RUST_BOX
#define CXXBRIDGE1_RUST_BOX
template <typename T>
class Box final {
public:
  using element_type = T;
  using const_pointer =
      typename std::add_pointer<typename std::add_const<T>::type>::type;
  using pointer = typename std::add_pointer<T>::type;

  Box() = delete;
  Box(Box &&) noexcept;
  ~Box() noexcept;

  explicit Box(const T &);
  explicit Box(T &&);

  Box &operator=(Box &&) &noexcept;

  const T *operator->() const noexcept;
  const T &operator*() const noexcept;
  T *operator->() noexcept;
  T &operator*() noexcept;

  template <typename... Fields>
  static Box in_place(Fields &&...);

  void swap(Box &) noexcept;

  static Box from_raw(T *) noexcept;

  T *into_raw() noexcept;

  /* Deprecated */ using value_type = element_type;

private:
  class uninit;
  class allocation;
  Box(uninit) noexcept;
  void drop() noexcept;

  friend void swap(Box &lhs, Box &rhs) noexcept { lhs.swap(rhs); }

  T *ptr;
};

template <typename T>
class Box<T>::uninit {};

template <typename T>
class Box<T>::allocation {
  static T *alloc() noexcept;
  static void dealloc(T *) noexcept;

public:
  allocation() noexcept : ptr(alloc()) {}
  ~allocation() noexcept {
    if (this->ptr) {
      dealloc(this->ptr);
    }
  }
  T *ptr;
};

template <typename T>
Box<T>::Box(Box &&other) noexcept : ptr(other.ptr) {
  other.ptr = nullptr;
}

template <typename T>
Box<T>::Box(const T &val) {
  allocation alloc;
  ::new (alloc.ptr) T(val);
  this->ptr = alloc.ptr;
  alloc.ptr = nullptr;
}

template <typename T>
Box<T>::Box(T &&val) {
  allocation alloc;
  ::new (alloc.ptr) T(std::move(val));
  this->ptr = alloc.ptr;
  alloc.ptr = nullptr;
}

template <typename T>
Box<T>::~Box() noexcept {
  if (this->ptr) {
    this->drop();
  }
}

template <typename T>
Box<T> &Box<T>::operator=(Box &&other) &noexcept {
  if (this->ptr) {
    this->drop();
  }
  this->ptr = other.ptr;
  other.ptr = nullptr;
  return *this;
}

template <typename T>
const T *Box<T>::operator->() const noexcept {
  return this->ptr;
}

template <typename T>
const T &Box<T>::operator*() const noexcept {
  return *this->ptr;
}

template <typename T>
T *Box<T>::operator->() noexcept {
  return this->ptr;
}

template <typename T>
T &Box<T>::operator*() noexcept {
  return *this->ptr;
}

template <typename T>
template <typename... Fields>
Box<T> Box<T>::in_place(Fields &&...fields) {
  allocation alloc;
  auto ptr = alloc.ptr;
  ::new (ptr) T{std::forward<Fields>(fields)...};
  alloc.ptr = nullptr;
  return from_raw(ptr);
}

template <typename T>
void Box<T>::swap(Box &rhs) noexcept {
  using std::swap;
  swap(this->ptr, rhs.ptr);
}

template <typename T>
Box<T> Box<T>::from_raw(T *raw) noexcept {
  Box box = uninit{};
  box.ptr = raw;
  return box;
}

template <typename T>
T *Box<T>::into_raw() noexcept {
  T *raw = this->ptr;
  this->ptr = nullptr;
  return raw;
}

template <typename T>
Box<T>::Box(uninit) noexcept {}
#endif // CXXBRIDGE1_RUST_BOX

#ifndef CXXBRIDGE1_RUST_ERROR
#define CXXBRIDGE1_RUST_ERROR
class Error final : public std::exception {
public:
  Error(const Error &);
  Error(Error &&) noexcept;
  ~Error() noexcept override;

  Error &operator=(const Error &) &;
  Error &operator=(Error &&) &noexcept;

  const char *what() const noexcept override;

private:
  Error() noexcept = default;
  friend impl<Error>;
  const char *msg;
  std::size_t len;
};
#endif // CXXBRIDGE1_RUST_ERROR

#ifndef CXXBRIDGE1_RUST_OPAQUE
#define CXXBRIDGE1_RUST_OPAQUE
class Opaque {
public:
  Opaque() = delete;
  Opaque(const Opaque &) = delete;
  ~Opaque() = delete;
};
#endif // CXXBRIDGE1_RUST_OPAQUE

#ifndef CXXBRIDGE1_IS_COMPLETE
#define CXXBRIDGE1_IS_COMPLETE
namespace detail {
namespace {
template <typename T, typename = std::size_t>
struct is_complete : std::false_type {};
template <typename T>
struct is_complete<T, decltype(sizeof(T))> : std::true_type {};
} // namespace
} // namespace detail
#endif // CXXBRIDGE1_IS_COMPLETE

#ifndef CXXBRIDGE1_LAYOUT
#define CXXBRIDGE1_LAYOUT
class layout {
  template <typename T>
  friend std::size_t size_of();
  template <typename T>
  friend std::size_t align_of();
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return T::layout::size();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_size_of() {
    return sizeof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      size_of() {
    return do_size_of<T>();
  }
  template <typename T>
  static typename std::enable_if<std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return T::layout::align();
  }
  template <typename T>
  static typename std::enable_if<!std::is_base_of<Opaque, T>::value,
                                 std::size_t>::type
  do_align_of() {
    return alignof(T);
  }
  template <typename T>
  static
      typename std::enable_if<detail::is_complete<T>::value, std::size_t>::type
      align_of() {
    return do_align_of<T>();
  }
};

template <typename T>
std::size_t size_of() {
  return layout::size_of<T>();
}

template <typename T>
std::size_t align_of() {
  return layout::align_of<T>();
}
#endif // CXXBRIDGE1_LAYOUT

class Str::uninit {};
inline Str::Str(uninit) noexcept {}

namespace repr {
using Fat = ::std::array<::std::uintptr_t, 2>;

struct PtrLen final {
  void *ptr;
  ::std::size_t len;
};
} // namespace repr

namespace detail {
template <typename T, typename = void *>
struct operator_new {
  void *operator()(::std::size_t sz) { return ::operator new(sz); }
};

template <typename T>
struct operator_new<T, decltype(T::operator new(sizeof(T)))> {
  void *operator()(::std::size_t sz) { return T::operator new(sz); }
};
} // namespace detail

template <typename T>
union MaybeUninit {
  T value;
  void *operator new(::std::size_t sz) { return detail::operator_new<T>{}(sz); }
  MaybeUninit() {}
  ~MaybeUninit() {}
};

namespace {
template <>
class impl<Str> final {
public:
  static Str new_unchecked(repr::Fat repr) noexcept {
    Str str = Str::uninit{};
    str.repr = repr;
    return str;
  }
};

template <>
class impl<Error> final {
public:
  static Error error(repr::PtrLen repr) noexcept {
    Error error;
    error.msg = static_cast<char const *>(repr.ptr);
    error.len = repr.len;
    return error;
  }
};
} // namespace
} // namespace cxxbridge1
} // namespace rust

namespace azure_trace_rs {
  enum class BlobType : ::std::uint8_t;
  enum class TraceError : ::std::uint8_t;
  struct BlobAccessTrace;
  struct reader;
}

namespace azure_trace_rs {
#ifndef CXXBRIDGE1_ENUM_azure_trace_rs$BlobType
#define CXXBRIDGE1_ENUM_azure_trace_rs$BlobType
enum class BlobType : ::std::uint8_t {
  Application = 0,
  Image = 1,
  Text = 2,
  None = 3,
  Other = 4,
};
#endif // CXXBRIDGE1_ENUM_azure_trace_rs$BlobType

#ifndef CXXBRIDGE1_ENUM_azure_trace_rs$TraceError
#define CXXBRIDGE1_ENUM_azure_trace_rs$TraceError
enum class TraceError : ::std::uint8_t {
  Exhaust = 0,
  BadRecord = 1,
  Io = 2,
  Other = 3,
};
#endif // CXXBRIDGE1_ENUM_azure_trace_rs$TraceError

#ifndef CXXBRIDGE1_STRUCT_azure_trace_rs$BlobAccessTrace
#define CXXBRIDGE1_STRUCT_azure_trace_rs$BlobAccessTrace
struct BlobAccessTrace final {
  ::std::uint64_t time_stamp;
  ::std::uint64_t region_id;
  ::std::uint64_t user_id;
  ::std::uint64_t app_id;
  ::std::uint64_t func_id;
  ::std::size_t blob_id;
  ::azure_trace_rs::BlobType blob_type;
  ::std::uint64_t version_tag;
  ::std::size_t size;
  bool read;
  bool write;

  using IsRelocatable = ::std::true_type;
};
#endif // CXXBRIDGE1_STRUCT_azure_trace_rs$BlobAccessTrace

#ifndef CXXBRIDGE1_STRUCT_azure_trace_rs$reader
#define CXXBRIDGE1_STRUCT_azure_trace_rs$reader
struct reader final : public ::rust::Opaque {
  ::azure_trace_rs::BlobAccessTrace next_record();
  ~reader() = delete;

private:
  friend ::rust::layout;
  struct layout {
    static ::std::size_t size() noexcept;
    static ::std::size_t align() noexcept;
  };
};
#endif // CXXBRIDGE1_STRUCT_azure_trace_rs$reader

extern "C" {
::std::size_t azure_trace_rs$cxxbridge1$reader$operator$sizeof() noexcept;
::std::size_t azure_trace_rs$cxxbridge1$reader$operator$alignof() noexcept;

::rust::repr::PtrLen azure_trace_rs$cxxbridge1$open_reader(::rust::Str file, ::rust::Box<::azure_trace_rs::reader> *return$) noexcept;

::rust::repr::PtrLen azure_trace_rs$cxxbridge1$reader$next_record(::azure_trace_rs::reader &self, ::azure_trace_rs::BlobAccessTrace *return$) noexcept;

::azure_trace_rs::TraceError azure_trace_rs$cxxbridge1$str_to_trace_err(::rust::Str string) noexcept;

::rust::repr::Fat azure_trace_rs$cxxbridge1$trace_err_to_str(::azure_trace_rs::TraceError err) noexcept;
} // extern "C"

::std::size_t reader::layout::size() noexcept {
  return azure_trace_rs$cxxbridge1$reader$operator$sizeof();
}

::std::size_t reader::layout::align() noexcept {
  return azure_trace_rs$cxxbridge1$reader$operator$alignof();
}

::rust::Box<::azure_trace_rs::reader> open_reader(::rust::Str file) {
  ::rust::MaybeUninit<::rust::Box<::azure_trace_rs::reader>> return$;
  ::rust::repr::PtrLen error$ = azure_trace_rs$cxxbridge1$open_reader(file, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

::azure_trace_rs::BlobAccessTrace reader::next_record() {
  ::rust::MaybeUninit<::azure_trace_rs::BlobAccessTrace> return$;
  ::rust::repr::PtrLen error$ = azure_trace_rs$cxxbridge1$reader$next_record(*this, &return$.value);
  if (error$.ptr) {
    throw ::rust::impl<::rust::Error>::error(error$);
  }
  return ::std::move(return$.value);
}

::azure_trace_rs::TraceError str_to_err(::rust::Str string) noexcept {
  return azure_trace_rs$cxxbridge1$str_to_trace_err(string);
}

::rust::Str err_to_str(::azure_trace_rs::TraceError err) noexcept {
  return ::rust::impl<::rust::Str>::new_unchecked(azure_trace_rs$cxxbridge1$trace_err_to_str(err));
}
} // namespace azure_trace_rs

extern "C" {
::azure_trace_rs::reader *cxxbridge1$box$azure_trace_rs$reader$alloc() noexcept;
void cxxbridge1$box$azure_trace_rs$reader$dealloc(::azure_trace_rs::reader *) noexcept;
void cxxbridge1$box$azure_trace_rs$reader$drop(::rust::Box<::azure_trace_rs::reader> *ptr) noexcept;
} // extern "C"

namespace rust {
inline namespace cxxbridge1 {
template <>
::azure_trace_rs::reader *Box<::azure_trace_rs::reader>::allocation::alloc() noexcept {
  return cxxbridge1$box$azure_trace_rs$reader$alloc();
}
template <>
void Box<::azure_trace_rs::reader>::allocation::dealloc(::azure_trace_rs::reader *ptr) noexcept {
  cxxbridge1$box$azure_trace_rs$reader$dealloc(ptr);
}
template <>
void Box<::azure_trace_rs::reader>::drop() noexcept {
  cxxbridge1$box$azure_trace_rs$reader$drop(this);
}
} // namespace cxxbridge1
} // namespace rust

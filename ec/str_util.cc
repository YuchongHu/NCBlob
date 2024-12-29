#include "str_util.hh"

#include <climits>
#include <cstdlib>
#include <sstream>
#include <strings.h>
#include <system_error>

namespace ec {

auto strict_strtob(const char *str, std::string *err) -> bool {
  if (strcasecmp(str, "false") == 0) {
    return false;
  } else if (strcasecmp(str, "true") == 0) {
    return true;
  } else {
    int b = strict_strtol(str, 10, err);
    return (bool)!!b;
  }
}

auto strict_strtoll(std::string_view str, int base, std::string *err)
    -> long long {
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  long long ret = strtoll(str.data(), &endptr, base);
  if (endptr == str.data() || endptr != str.data() + str.size()) {
    *err = (std::string{"Expected option value to be integer, got '"} +
            std::string{str} + "'");
    return 0;
  }
  if (errno) {
    *err = (std::string{"The option value '"} + std::string{str} +
            "' seems to be invalid");
    return 0;
  }
  *err = "";
  return ret;
}

auto strict_strtol(std::string_view str, int base, std::string *err) -> int {
  long long ret = strict_strtoll(str, base, err);
  if (!err->empty())
    return 0;
  if ((ret < INT_MIN) || (ret > INT_MAX)) {
    std::ostringstream errStr;
    errStr << "The option value '" << str << "' seems to be invalid";
    *err = errStr.str();
    return 0;
  }
  return static_cast<int>(ret);
}

auto strict_strtol(const char *str, int base, std::string *err) -> int {
  return strict_strtol(std::string_view(str), base, err);
}

auto strict_strtod(std::string_view str, std::string *err) -> double {
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  double ret = strtod(str.data(), &endptr);
  if (errno == ERANGE) {
    std::ostringstream oss;
    oss << "strict_strtod: floating point overflow or underflow parsing '"
        << str << "'";
    *err = oss.str();
    return 0.0;
  }
  if (endptr == str) {
    std::ostringstream oss;
    oss << "strict_strtod: expected double, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    std::ostringstream oss;
    oss << "strict_strtod: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  *err = "";
  return ret;
}

auto strict_strtof(std::string_view str, std::string *err) -> float {
  char *endptr;
  errno = 0; /* To distinguish success/failure after call (see man page) */
  float ret = strtof(str.data(), &endptr);
  if (errno == ERANGE) {
    std::ostringstream oss;
    oss << "strict_strtof: floating point overflow or underflow parsing '"
        << str << "'";
    *err = oss.str();
    return 0.0;
  }
  if (endptr == str) {
    std::ostringstream oss;
    oss << "strict_strtof: expected float, got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  if (*endptr != '\0') {
    std::ostringstream oss;
    oss << "strict_strtof: garbage at end of string. got: '" << str << "'";
    *err = oss.str();
    return 0;
  }
  *err = "";
  return ret;
}


} // namespace ec
/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-25 11:29:17
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-31 07:51:50
 * @FilePath: /tbr/ec/str_util.hh
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include <cstdint>
#include <string_view>

namespace ec {
auto strict_strtoll(std::string_view str, int base, std::string *err)
    -> long long;

auto strict_strtob(const char *str, std::string *err) -> bool;

auto strict_strtol(std::string_view str, int base, std::string *err) -> int;

auto strict_strtod(std::string_view str, std::string *err) -> double;

auto strict_strtof(std::string_view str, std::string *err) -> float;

auto strict_iecstrtoll(std::string_view str, std::string *err) -> uint64_t;


} // namespace ec
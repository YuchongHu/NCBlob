/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-08 02:13:21
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-09 02:27:30
 * @FilePath: /lib_ec/include/utils.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#ifndef UTILS_HPP
#define UTILS_HPP

#include <iostream>
#include <vector>
#include <fstream>
#include <sys/stat.h>
#include <string>
#include <rados/buffer.h>
#include <filesystem>

using namespace ceph;
using namespace ceph::buffer;
using namespace std;
namespace fs = std::filesystem;

constexpr std::size_t KB = 1024;
constexpr std::size_t MB = 1024 * 1024;

/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-08 02:13:21
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-08 02:14:52
 * @FilePath: /lib_ec/include/utils.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
std::vector<std::string> getFilesInDirectory(const std::string& directory);

int read_file_to_bl(string filename, bufferlist &in);
int read_file_to_bl(string filename, unsigned off, unsigned len, bufferlist &in);

bool createDirectoriesRecursively(const std::string& path);

std::string getLastSubstringAfterSlash(const std::string& str);

#endif
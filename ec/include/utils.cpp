#include "utils.hpp"
#include <memory>
/*
 * @Author: Edgar gongpengyu7@gmail.com
 * @Date: 2024-07-08 02:13:21
 * @LastEditors: Edgar gongpengyu7@gmail.com
 * @LastEditTime: 2024-07-09 02:43:10
 * @FilePath: /lib_ec/include/utils.hpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
std::vector<std::string> getFilesInDirectory(const std::string& directory) {
    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file()) {
            files.push_back(entry.path().string());
        }
    }
    return files;
}

int read_file_to_bl(string filename, bufferlist &in){
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Error: Failed to open file " << filename << std::endl;
        // 适当的错误处理：抛出异常或返回错误码
        throw std::runtime_error("Failed to open file " + filename);
    }
    // 获取文件大小
    std::streampos size = file.tellg();
    if(size == 0)
      return -1;
    file.seekg(0, std::ios::beg);

    char* payload = new char[size];

    file.read(payload, size);

    file.close();
    
    // const char *payload =
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    bufferptr in_ptr(ceph::buffer::create_page_aligned(size));
    in_ptr.zero();
    in_ptr.set_length(0);
    in_ptr.append(payload, size);
    // bufferlist in;
    in.push_back(in_ptr);

    delete [] payload;
    return 0;
}

int read_file_to_bl(string filename, unsigned off, unsigned len, bufferlist &in){
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Error: Failed to open file " << filename << std::endl;
        // 适当的错误处理：抛出异常或返回错误码
        throw std::runtime_error("Failed to open file " + filename);
    }
    // 获取文件大小
    std::streampos size = file.tellg();
    if(size == 0)
      return -1;

    if (off + len > static_cast<unsigned>(size)) {
        std::cerr << "Error: Read beyond the end of the file" << std::endl;
        return -1;
    }

    
    file.seekg(off, std::ios::beg);

    // char* payload = new char[len];

    // std::unique_ptr<char[]> payload(new char[len]);
    unique_ptr<char[]> payload = make_unique<char[]>(len);


    file.read(payload.get(), len);

    file.close();
    
    // const char *payload =
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    // "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    bufferptr in_ptr(ceph::buffer::create_page_aligned(len));
    in_ptr.zero();
    in_ptr.set_length(0);
    in_ptr.append(payload.get(), len);
    // bufferlist in;
    in.push_back(in_ptr);

    // delete [] payload;
    return 0;
}

bool createDirectoriesRecursively(const std::string& path) {
    std::vector<std::string> components;
    std::string::size_type start = 0;
    std::string::size_type pos = path.find_first_of('/', start);

    while (pos != std::string::npos) {
        components.push_back(path.substr(start, pos - start));
        start = pos + 1;
        pos = path.find_first_of('/', start);
    }
    components.push_back(path.substr(start)); // Add the last component
    // for(auto str : components)
    //   std::cout<<str<<std::endl;
    std::string currentPath = "/";
    for (const auto& component : components) {
        currentPath += component + "/";
        if (mkdir(currentPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) != 0) {
            if (errno != EEXIST) {
                // 如果不是因为目录已存在错误，则返回false
                return false;
            }
        }
    }
    return true;
}


std::string getLastSubstringAfterSlash(const std::string& str) {
    // 查找字符串中最后一个'/'的位置
    size_t pos = str.find_last_of('/');

    // 如果找到了'/'
    if (pos != std::string::npos) {
        // 提取'/'之后的子串
        return str.substr(pos + 1);
    } else {
        // 如果没有找到'/', 返回原字符串
        return str;
    }
}
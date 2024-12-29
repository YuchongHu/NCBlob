// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <iostream>
#include <algorithm>
#include <cerrno>
#include <cstring>

#include "erasure_code.hh"
#include "str_util.hh"

#define DEFAULT_RULE_ROOT "default"
#define DEFAULT_RULE_FAILURE_DOMAIN "host"

using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ec::bufferlist;

namespace ec {
std::vector<char> mergeMatrixChunk(const std::vector<int> &matrix_row, const std::vector<char> &chunk) {
  std::vector<char> result;

  // 计算需要的总大小
  size_t totalSize = matrix_row.size() * sizeof(int) + chunk.size() * sizeof(char);
  result.reserve(totalSize);

  // 将 int 矢量复制到结果中
  for (const int &i : matrix_row) {
    const auto *bytePointer = reinterpret_cast<const char *>(&i);
    result.insert(result.end(), bytePointer, bytePointer + sizeof(int));
  }

  // 将 char 矢量复制到结果中
  result.insert(result.end(), chunk.begin(), chunk.end());

  return result;
}

/**
 * @description: 
 * @param {vector<char>} &data : the data merge the matrix_row and the chunk_data
 * @param {vector<int>} &matrix_row : the matrix_row prepare to be restored
 * @param {vector<char>} &chunk : the chunk data prepare to be restored
 * @param {int} intNum : the matrix_row int num in the `data`
 * @return {*}
 */
void splitMatrixChunk(const std::vector<char> &data, std::vector<int> &matrix_row, std::vector<char> &chunk, int intNum) {
  size_t intSize = sizeof(int);

  matrix_row.clear();
  chunk.clear();

  // 恢复 int 矢量
  for (size_t i = 0; i < intNum * intSize; i += intSize) {
    int value;
    std::memcpy(&value, &data[i], intSize);
    matrix_row.push_back(value);
  }

  // 恢复 char 矢量
  size_t charStart = intNum * intSize;
  chunk.insert(chunk.end(), data.begin() + charStart, data.end());
}

namespace buffer = ceph::buffer;
const unsigned ErasureCode::SIMD_ALIGN = 32;

auto ErasureCode::init(ErasureCodeProfile &profile, std::ostream *ss) -> int {
  _profile = profile;
  return 0;
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
auto ErasureCode::sanity_check_k_m(int k, int m, ostream *ss) -> int {
  if (k < 2) {
    *ss << "k=" << k << " must be >= 2" << std::endl;
    return -EINVAL;
  }
  if (m < 1) {
    *ss << "m=" << m << " must be >= 1" << std::endl;
    return -EINVAL;
  }
  return 0;
}

auto ErasureCode::chunk_index(unsigned int i) const -> int {
  return chunk_mapping.size() > i ? chunk_mapping[i] : i;
}

auto ErasureCode::_minimum_to_decode(const set<int> &want_to_read,
                                     const set<int> &available_chunks,
                                     set<int> *minimum) -> int {
  if (includes(available_chunks.begin(), available_chunks.end(),
               want_to_read.begin(), want_to_read.end())) {
    *minimum = want_to_read;
  } else {
    unsigned int k = get_data_chunk_count();
    if (available_chunks.size() < (unsigned)k)
      return -EIO;
    set<int>::iterator i;
    unsigned j;
    for (i = available_chunks.begin(), j = 0; j < (unsigned)k; ++i, j++)
      minimum->insert(*i);
  }
  return 0;
}

auto ErasureCode::minimum_to_decode(const set<int> &want_to_read,
                                    const set<int> &available_chunks,
                                    map<int, vector<pair<int, int>>> *minimum)
    -> int {
  set<int> minimum_shard_ids;
  int r =
      _minimum_to_decode(want_to_read, available_chunks, &minimum_shard_ids);
  if (r != 0) {
    return r;
  }
  vector<pair<int, int>> default_subchunks;
  default_subchunks.emplace_back(0, get_sub_chunk_count());
  for (auto &&id : minimum_shard_ids) {
    minimum->insert(make_pair(id, default_subchunks));
  }
  return 0;
}

auto ErasureCode::minimum_to_decode_with_cost(const set<int> &want_to_read,
                                              const map<int, int> &available,
                                              set<int> *minimum) -> int {
  set<int> available_chunks;
  for (auto i : available)
    available_chunks.insert(i.first);
  return _minimum_to_decode(want_to_read, available_chunks, minimum);
}

auto ErasureCode::encode_prepare(const bufferlist &raw,
                                 map<int, bufferlist> &encoded) const -> int {
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned blocksize = get_chunk_size(raw.length());
  unsigned padded_chunks = k - raw.length() / blocksize;
  const bufferlist &prepared = raw;

  for (unsigned int i = 0; i < k - padded_chunks; i++) {
    bufferlist &chunk = encoded[chunk_index(i)];
    chunk.substr_of(prepared, i * blocksize, blocksize);
    chunk.rebuild_aligned_size_and_memory(blocksize, SIMD_ALIGN);
    assert(chunk.is_contiguous());
  }
  if (padded_chunks) {
    unsigned remainder = raw.length() - (k - padded_chunks) * blocksize;
    bufferptr buf(buffer::create_aligned(blocksize, SIMD_ALIGN));

    raw.begin((k - padded_chunks) * blocksize).copy(remainder, buf.c_str());
    buf.zero(remainder, blocksize - remainder);
    encoded[chunk_index(k - padded_chunks)].push_back(std::move(buf));

    for (unsigned int i = k - padded_chunks + 1; i < k; i++) {
      bufferptr buf(buffer::create_aligned(blocksize, SIMD_ALIGN));
      buf.zero();
      encoded[chunk_index(i)].push_back(std::move(buf));
    }
  }
  for (unsigned int i = k; i < k + m; i++) {
    bufferlist &chunk = encoded[chunk_index(i)];
    chunk.push_back(buffer::create_aligned(blocksize, SIMD_ALIGN));
  }

  return 0;
}

auto ErasureCode::encode(const set<int> &want_to_encode, const bufferlist &in,
                         map<int, bufferlist> *encoded) -> int {
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  bufferlist out;
  // std::cout << "encode prepare" << std::endl;
  int err = encode_prepare(in, *encoded);
  // std::cout << "after encode prepare" << std::endl;
  
  if (err)
    return err;
  encode_chunks(want_to_encode, encoded);
  for (unsigned int i = 0; i < k + m; i++) {
    if (want_to_encode.count(i) == 0)
      encoded->erase(i);
  }
  return 0;
}

auto ErasureCode::_decode(const set<int> &want_to_read,
                          const map<int, bufferlist> &chunks,
                          map<int, bufferlist> *decoded) -> int {
  vector<int> have;
  have.reserve(chunks.size());
  for (const auto &chunk : chunks) {
    have.push_back(chunk.first);
  }

  ErasureCode::Type type = get_class_name();
  // 查看数据块是否缺失，如果不缺失且不是Lonse，直接返回
  if (includes(have.begin(), have.end(), want_to_read.begin(),
               want_to_read.end())
               && type != "ErasureCodeLonse") {
    for (int i : want_to_read) {
      (*decoded)[i] = chunks.find(i)->second;
    }
    return 0;
  }

  // 如果是Lonse，则不缺失也需要decode来normal read
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned blocksize = (*chunks.begin()).second.length();
  for (unsigned int i = 0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      // i lost, align space for decoded[i]
      bufferlist tmp;
      bufferptr ptr(buffer::create_aligned(blocksize, SIMD_ALIGN));
      tmp.push_back(ptr);
      tmp.claim_append((*decoded)[i]);
      (*decoded)[i].swap(tmp);
    } else {
      // i exist, (*decoded)[i] = chunks.find(i)->second
      (*decoded)[i] = chunks.find(i)->second;
      (*decoded)[i].rebuild_aligned(SIMD_ALIGN);
    }
  }
  return decode_chunks(want_to_read, chunks, decoded);
}

auto ErasureCode::decode(const set<int> &want_to_read,
                         const map<int, bufferlist> &chunks,
                         map<int, bufferlist> *decoded, int chunk_size) -> int {
  return _decode(want_to_read, chunks, decoded);
}

auto ErasureCode::parse(const ErasureCodeProfile &profile, ostream *ss) -> int {
  return to_mapping(profile, ss);
}

auto ErasureCode::get_chunk_mapping() const -> const vector<int> & {
  return chunk_mapping;
}

auto ErasureCode::to_mapping(const ErasureCodeProfile &profile, ostream *ss)
    -> int {
  if (profile.find("mapping") != profile.end()) {
    std::string mapping = profile.find("mapping")->second;
    int position = 0;
    vector<int> coding_chunk_mapping;
    for (char &it : mapping) {
      if (it == 'D')
        chunk_mapping.push_back(position);
      else
        coding_chunk_mapping.push_back(position);
      position++;
    }
    chunk_mapping.insert(chunk_mapping.end(), coding_chunk_mapping.begin(),
                         coding_chunk_mapping.end());
  }
  return 0;
}

auto ErasureCode::to_int(const std::string &name, ErasureCodeProfile &profile,
                         int *value, const std::string &default_value,
                         ostream *ss) -> int {
  if (profile.find(name) == profile.end() || profile.find(name)->second.empty())
    profile[name] = default_value;
  std::string p = profile.find(name)->second;
  std::string err;
  int r = strict_strtol(p, 10, &err);
  if (!err.empty()) {
    *ss << "could not convert " << name << "=" << p << " to int because " << err
        << ", set to default " << default_value << std::endl;
    *value = strict_strtol(default_value, 10, &err);
    return -EINVAL;
  }
  *value = r;
  return 0;
}

auto ErasureCode::to_bool(const std::string &name, ErasureCodeProfile &profile,
                          bool *value, const std::string &default_value,
                          ostream *ss) -> int {
  if (profile.find(name) == profile.end() || profile.find(name)->second.empty())
    profile[name] = default_value;
  const std::string p = profile.find(name)->second;
  *value = (p == "yes") || (p == "true");
  return 0;
}

auto ErasureCode::to_string(const std::string &name,
                            ErasureCodeProfile &profile, std::string *value,
                            const std::string &default_value, ostream *ss)
    -> int {
  if (profile.find(name) == profile.end() || profile.find(name)->second.empty())
    profile[name] = default_value;
  *value = profile[name];
  return 0;
}

auto ErasureCode::decode_concat(const map<int, bufferlist> &chunks,
                                bufferlist *decoded) -> int {
  set<int> want_to_read;

  for (unsigned int i = 0; i < get_data_chunk_count(); i++) {
    want_to_read.insert(chunk_index(i));
  }
  map<int, bufferlist> decoded_map;
  int r = _decode(want_to_read, chunks, &decoded_map);
  if (r == 0) {
    for (unsigned int i = 0; i < get_data_chunk_count(); i++) {
      decoded->claim_append(decoded_map[chunk_index(i)]);
    }
  }
  return r;
}
} // namespace ec

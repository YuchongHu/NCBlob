// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#include "erasure_code_Lonse.hh"
#include "erasure_code.hh"
#include "exception.hpp"
#include <algorithm>
#include <boost/numeric/conversion/cast.hpp>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

// #include "include/intarith.h"

extern "C" {
#include "jerasure.h"
}

#define LARGEST_VECTOR_WORDSIZE 16

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

using std::map;
using std::ostream;
using std::set;

using std::make_pair;
using std::pair;
using std::string;
using std::vector;

using ceph::bufferlist;
// using ceph::ErasureCodeProfile;

namespace ec {
static ostream &_prefix(std::ostream *_dout) {
  return *_dout << "ErasureCodeLonse: ";
}

template <typename T, typename U>
constexpr static inline auto
round_up_to(T n, U d) -> std::make_unsigned_t<std::common_type_t<T, U>> {
  return (n % d ? (n + d - n % d) : n);
}

int ErasureCodeLonse::init(ErasureCodeProfile &profile, ostream *ss) {
  int err = 0;
  // dout(10) << "technique=" << technique << dendl;
  // profile["technique"] = technique;
  err |= parse(profile, ss);
  if (err)
    return err;
  // prepare();
  // init _encode_matrix
  generate_matrix(_encode_matrix, (k + m) * m, k * m, 8);
  // init _repair_matrix
  generate_matrix(_repair_matrix, m, k + m - 1, 8);

  return ErasureCode::init(profile, ss);
}

void ErasureCodeLonse::generate_matrix(int *matrix, int rows, int cols, int w) {
  using namespace std;
  // rows = (k+m)*m
  // cols = k*m
  int k = cols;
  int n = rows;

  memset(matrix, 0, rows * cols * sizeof(int));

  for (int i = 0; i < rows; i++) {
    int tmp = 1;
    for (int j = 0; j < cols; j++) {
      matrix[i * cols + j] = tmp;
      tmp = Computation::singleMulti(tmp, i + 1, w);
    }
  }
}

int ErasureCodeLonse::parse(ErasureCodeProfile &profile, ostream *ss) {
  int err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  n = k + m;
  if (chunk_mapping.size() > 0 && (int)chunk_mapping.size() != k + m) {
    *ss << "mapping " << profile.find("mapping")->second << " maps "
        << chunk_mapping.size() << " chunks instead of"
        << " the expected " << k + m << " and will be ignored" << std::endl;
    chunk_mapping.clear();
    err = -EINVAL;
  }
  err |= sanity_check_k_m(k, m, ss);
  sub_chunk_no = m;
  return err;
}

// make sure the chunk size is a multiple of m
unsigned int ErasureCodeLonse::get_chunk_size(unsigned int object_size) const {
  // unsigned alignment = get_alignment();
  // unsigned alignment = k*w*sizeof(int);
  // if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
  //   alignment = k*w*LARGEST_VECTOR_WORDSIZE;
  // unsigned tail = object_size % alignment;
  // unsigned padded_length = object_size + ( tail ?  ( alignment - tail ) : 0
  // ); ceph_assert(padded_length % k == 0); return padded_length / k;
  unsigned int alignment = m * k;

  return round_up_to(object_size, alignment) / k;
}

/**
 * @description: if it needs repair, return 1; if it's just normal read, return
 * 0
 * @return {*}
 */
auto ErasureCodeLonse::is_repair(const set<int> &want_to_read,
                                 const set<int> &available_chunks) const
    -> int {
  if (includes(available_chunks.begin(),
               available_chunks.end(),
               want_to_read.begin(),
               want_to_read.end()))
    return 0;

  if (want_to_read.size() > 1)
    // in the case of normal read, want_to_read.size() == k
    return 0;

  return 1;
}

/**
 * @description:
 * @param minimum: key: chunk id, value: subchunk id
 * @return {*}
 */
auto ErasureCodeLonse::minimum_to_repair(
    const set<int> &want_to_read, const set<int> &available_chunks,
    map<int, vector<pair<int, int>>> *minimum) -> int {
  for (auto iter : available_chunks) {
    (*minimum)[iter] = {std::make_pair(_row_idx, 1)};
  }
  return 0;
}

int ErasureCodeLonse::minimum_to_decode(
    const set<int> &want_to_read, const set<int> &available,
    map<int, vector<pair<int, int>>> *minimum) {
  if (is_repair(want_to_read, available)) {
    return minimum_to_repair(want_to_read, available, minimum);
  }
  // // dout(0) << "minimum_to_decode invoked" << dendl;
  else
    return ErasureCode::minimum_to_decode(want_to_read, available, minimum);
}

int ErasureCodeLonse::encode_chunks(const set<int> &want_to_encode,
                                    map<int, bufferlist> *encoded) {
  int data_size = (*encoded)[0].length();
  assert(data_size % m == 0);

  unsigned sub_chunksize = data_size / m;
  // std::cout<<"sub_chunksize:"<<sub_chunksize<<std::endl;

  char *coding_ptrs[n * m];
  for (int i = 0; i < n * m; i++) {
    coding_ptrs[i] = new char[sub_chunksize];
  }

  char *data_ptrs[k * m];
  for (int i = 0; i < k; i++) {
    char *temp = (*encoded)[i].c_str();
    for (int j = 0; j < m; j++) {
      data_ptrs[i * m + j] = temp;
      temp += sub_chunksize;
    }
  }

  // std::cout<<"data before normal read"<<std::endl;
  // for (int i = 0; i < k*m; i++) {
  //       std::cout<<i << ": ";
  //       //   std::cout << data_ptrs[i] << std::endl;
  //       for(int j = 0; j < sub_chunksize; j++){
  //             std::cout<<unsigned(data_ptrs[i][j])%(256) << " ";
  //       }
  //       std::cout << std::endl;
  // }

  jerasure_matrix_encode(
      k * m, n * m, 8, _encode_matrix, data_ptrs, coding_ptrs, sub_chunksize);
  // copy the n*m code blocks into encoded
  for (int i = 0; i < n; i++) {
    (*encoded)[i].clear();
    for (int j = 0; j < m; j++) {
      (*encoded)[i].append(*(coding_ptrs + i * m + j), sub_chunksize);
    }
  }

  // free(data_ptrs);

  for (int i = 0; i < n * m; i++) {
    delete[] coding_ptrs[i];
  }

  return 0;
}

/**
 * @description:
 * @param helper: the subchunk data
 * @param decoded: the want to read chunk
 * @return {*}
 */
auto ErasureCodeLonse::repair(const set<int> &want_to_repair,
                              const map<int, bufferlist> &helper,
                              map<int, bufferlist> *recovered,
                              int chunk_size) -> int {
  EcAssert((want_to_repair.size() == 1) &&
           (helper.size() == (unsigned)(n - 1)));
  unsigned sub_chunksize = (*helper.begin()).second.length();
  std::cout << "sub_chunksize:" << sub_chunksize << std::endl;
  EcAssert(chunk_size == sub_chunksize * sub_chunk_no);
  // int erasures[k + m + 1];
  // int erasures_count = 0;

  std::cout << "row_idx:" << _row_idx << std::endl;
  int lostidx = *want_to_repair.begin();

  EcAssert(lostidx >= 0 && lostidx < n);
  EcAssert(helper.find(lostidx) == helper.end());
  // for (int i =  0; i < k + m; i++) {
  //   if (helper.find(i) == helper.end()) {
  //     lostidx = i;
  // erasures[erasures_count] = i;
  // erasures_count++;
  //     continue;
  //   }
  // }

  std::cout << "lostidx:" << lostidx << std::endl;
  // the n-1 slices from code blocks
  char *coding_slice[n - 1];

  for (int i = 0; i < n - 1; i++)
    coding_slice[i] = new char[sub_chunksize];

  int cnt = 0;
  for (int i = 0; i < k + m; i++) {
    if (i == lostidx)
      continue;
    // coding_slice指向decoded中除丢失块外的n-1的块中各自一个子块
    auto it = helper.find(i);
    auto &cBufferList = it->second;
    auto str = cBufferList.to_str();
    // coding_slice[cnt] = cBufferList.c_str()+row_idx*sub_chunksize;
    memcpy(coding_slice[cnt], str.data(), sub_chunksize);
    cnt++;
  }

  int tmp_matrix[(k + m - 1) * (k * m)]; // k+m-1 rows, k*m cols

  cnt = 0;
  // 从_encode_matrix中选k+m-1行
  for (int i = 0; i < k + m; i++) {
    if (i == lostidx)
      continue;
    int row =
        i * m * (k * m) +
        _row_idx * k * m; // 每m行选第_row_idx行为coef，这里的_row_idx是咋选的

    memcpy(tmp_matrix + (cnt * k * m),
           _encode_matrix + (row), // 从row开始复制_k*_m
           sizeof(int) * k * m);   // 复制一行_k*_m

    cnt++;
  }

  // _repair_matrix 和 tmp_matrix 相乘得到 new_encode_matrix
  // new_encode_matrix是用来填充_encode_matrix的
  int *new_encode_matrix = jerasure_matrix_multiply(
      _repair_matrix, tmp_matrix, m, k + m - 1, k + m - 1, k * m, 8);
  // update _encode_matrix
  // 为新生成的块更新编码矩阵，即用生成的new_encode_matrix填充进原来encode_matrix坏掉的块的对应的m行
  for (int i = 0; i < m; i++) {
    int row = lostidx * m * k * m + i * m * k;
    memcpy(_encode_matrix + row,
           new_encode_matrix + (i * k * m),
           sizeof(int) * k * m);
  }

  free(new_encode_matrix);

  // using namespace std;
  // cout << "new encode matrix" << endl;
  // for (int i=0; i<(k+m)*m; i++) {
  //     for (int j=0; j<k*m; j++) {
  //         cout << _encode_matrix[i*k*m+j] << "\t";
  //         cout << "\t";
  //     }
  //     cout << endl;

  // }
  /**repair lost chunks*/

  std::cout << "repair lost chunks" << std::endl;
  // buffer for the lost chunk
  char *new_coding[m];

  for (int i = 0; i < m; i++) {
    new_coding[i] = new char[sub_chunksize];
  }

  // repair the lost chunk
  // n-1个分片与修复矩阵中的对应行相乘，恢复出丢失的含m个分片的块
  for (int i = 0; i < m; i++) {
    int dest_id = i + (k + m - 1);
    int matrix_row = i * (k + m - 1);
    jerasure_matrix_dotprod(k + m - 1,
                            8,
                            _repair_matrix + matrix_row,
                            NULL,
                            dest_id,
                            coding_slice,
                            new_coding,
                            sub_chunksize);
  }

  // align the lost chunk
  // auto blocksize = sub_chunksize * m;
  // bufferlist tmp;
  // bufferptr ptr(ceph::buffer::create_aligned(blocksize, SIMD_ALIGN));
  // tmp.push_back(ptr);
  // tmp.claim_append((*recovered)[lostidx]);
  // (*recovered)[lostidx].swap(tmp);

  for (int i = 0; i < m; i++)
    (*recovered)[lostidx].append(new_coding[i], sub_chunksize);

  for (int i = 0; i < n - 1; i++) {
    delete[] coding_slice[i];
  }

  for (int i = 0; i < m; i++) {
    delete[] new_coding[i];
  }

  // _row_idx = (_row_idx + 1) % m; //
  // 更新_row_idx，因为是nsys，所以解码的时候不需要用更新后的encode_matrix，只需要用之前的repair_matrix生成一个新的线性组合就可以了

  return 0;
}

/**
 * @description:
 * @param chunks : the k chunks
 * @return {*}
 */
auto ErasureCodeLonse::normal_read(
    const std::set<int> &want_to_read,
    const std::map<int, ceph::bufferlist> &chunks,
    std::map<int, ceph::bufferlist> *decoded, int chunk_size) -> int {
  std::vector<int> want_to_read_vec(want_to_read.begin(), want_to_read.end());
  if (chunks.size() == 0) {
    return 0;
  }
  // cout<<"normal read" << endl;
  unsigned chunksize = chunks.begin()->second.length();
  if (chunksize < sizeof(std::size_t))
    return 0;
  bool padding = false;
  unsigned padding_length = 0;

  if (chunksize % m != 0) {
    padding = true;
    padding_length = m - (chunksize % m);
    chunksize = chunksize + padding_length;
  }
  string padding_str(padding_length, '\0');
  for (const auto i : want_to_read) {
    // i exist, (*decoded)[i] = chunks.find(i)->second
    (*decoded)[i] = chunks.find(i)->second;
    if (padding)
      (*decoded)[i].append(padding_str);
    (*decoded)[i].rebuild_aligned(SIMD_ALIGN);
  }

  //   EcAssert((unsigned)chunk_size == chunksize);
  // TODO: the case that chunk_size % m != 0
  // EcAssert(chunksize % m == 0);
  unsigned sub_chunksize = chunksize / sub_chunk_no;

  int select_matrix[(k * m) * (k * m)];
  int invert_matrix[(k * m) * (k * m)];

  // select _m*_k row from _encode_matrix
  // 正常读才需要从encode_matrix里面取出矩阵进行解码得到原始数据块
  for (int i = 0; i < k * m; i++) {
    // matrix中选的行和编码块中选的分片对应
    memcpy(select_matrix + i * k * m,
           _encode_matrix + i * k * m,
           sizeof(int) * k * m);
  }

  jerasure_invert_matrix(select_matrix, invert_matrix, k * m, 8);

  // the k*m slices from code blocks
  char *coding_blocks[k * m];
  char *data[k * m];

  int i = 0;
  for (auto &[node, bl] : *decoded) {
    char *temp = bl.c_str();
    auto len = bl.length();
    auto padding_len = boost::numeric_cast<unsigned int>(
        std::max(boost::numeric_cast<int>(sub_chunksize * m) -
                     boost::numeric_cast<int>(len),
                 0));
    //     bl.append_zero(padding_len);
    len = bl.length();
    EcAssert(len >= sub_chunksize * m);
    for (int j = 0; j < m; j++) {
      // 将coding_blocks指向decoded各个子块
      coding_blocks[i * m + j] = new char[sub_chunksize];
      memcpy(coding_blocks[i * m + j], temp, sub_chunksize);
      //       coding_blocks[i * m + j] = temp;
      temp += sub_chunksize;
    }
    i++;
  }

  // 给data分配空间
  // 不能直接将data指向decoded，而是需要重新分配空间，计算后再复制到decoded
  // 这是为了避免jerasure计算过程中将计算结果填充到decoded中而影响计算结果
  for (int i = 0; i < k; i++) {
    for (int j = 0; j < m; j++) {
      // data[i*m+j] = static_cast<char *>(malloc(sub_chunksize));
      data[i * m + j] = new char[sub_chunksize];
    }
  }

  for (int i = 0; i < k * m; i++) {
    int dest_id = i + k * m;
    int matrix_row = i * (k * m);
    jerasure_matrix_dotprod(k * m,
                            8,
                            invert_matrix + matrix_row,
                            NULL,
                            dest_id,
                            coding_blocks,
                            data,
                            sub_chunksize);
  }

  for (int i = 0; i < k; i++) {
    for (int j = 0; j < m; j++) {
      (*decoded)[want_to_read_vec[i]].clear();
      (*decoded)[want_to_read_vec[i]].append(data[i * m + j], sub_chunksize);
    }
  }

  for (int i = 0; i < k * m; i++) {
    delete[] coding_blocks[i];
    delete[] data[i];
  }
  return 0;
  //   std::cout<<"data after normal read"<<std::endl;
  //   for (int i = 0; i < k*m; i++) {
  //       std::cout<<i << ": ";
  // //
}

/**
 * @description:
 * @param:chunks: the helpers, but not the full chunks
 * @return {*}
 */
auto ErasureCodeLonse::decode(const set<int> &want_to_read,
                              const map<int, bufferlist> &chunks,
                              map<int, bufferlist> *decoded,
                              int chunk_size) -> int {
  // cout<<"decode" << endl;
  set<int> avail;
  for ([[maybe_unused]] auto &[node, bl] : chunks) {
    avail.insert(node);
    (void)bl; // silence -Wunused-variable
  }
  if (is_repair(want_to_read, avail)) {
    return repair(want_to_read, chunks, decoded, chunk_size);
  } else {
    return normal_read(want_to_read, chunks, decoded, chunk_size);
  }
}

int ErasureCodeLonse::decode_chunks(const set<int> &want_to_read,
                                    const map<int, bufferlist> &chunks,
                                    map<int, bufferlist> *decoded) {
  // init _encode_matrix
  // (k+m)*m rows, k*m cols
  generate_matrix(_encode_matrix, (k + m) * m, k * m, 8);
  unsigned blocksize = (*chunks.begin()).second.length();

  // TODO: align the sub-block if block%m != 0
  assert(blocksize % m == 0);

  unsigned sub_chunksize = blocksize / m;
  std::cout << "sub_chunksize:" << sub_chunksize << std::endl;
  int erasures[k + m + 1];
  int erasures_count = 0;

  std::cout << "row_idx:" << _row_idx << std::endl;
  int lostidx = -1;
  for (int i = 0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      lostidx = i;
      erasures[erasures_count] = i;
      erasures_count++;
      continue;
    }
  }

  if (lostidx >= 0) {
    std::cout << "lostidx:" << lostidx << std::endl;
    // the n-1 slices from code blocks
    char *coding_slice[n];

    for (int i = 0; i < n - 1; i++)
      coding_slice[i] = new char[sub_chunksize];

    int cnt = 0;
    for (int i = 0; i < k + m; i++) {
      if (i == lostidx)
        continue;
      // coding_slice指向decoded中除丢失块外的n-1的块中各自一个子块
      auto it = chunks.find(i);
      auto &cBufferList = it->second;
      auto str = cBufferList.to_str();
      // coding_slice[cnt] = cBufferList.c_str()+row_idx*sub_chunksize;
      memcpy(coding_slice[cnt],
             str.data() + _row_idx * sub_chunksize,
             sub_chunksize);
      cnt++;
    }

    int tmp_matrix[(k + m - 1) * (k * m)]; // k+m-1 rows, k*m cols

    cnt = 0;
    for (int i = 0; i < k + m; i++) {
      if (i == lostidx)
        continue;
      int row =
          i * m * (k * m) +
          _row_idx * k * m; // 每m行选第_row_idx行为coef，这里的_row_idx是咋选的

      memcpy(tmp_matrix + (cnt * k * m),
             _encode_matrix + (row), // 从row开始复制_k*_m
             sizeof(int) * k * m);   // 复制一行_k*_m

      cnt++;
    }

    // new_encode_matrix是用来填充_encode_matrix的
    int *new_encode_matrix = jerasure_matrix_multiply(
        _repair_matrix, tmp_matrix, m, k + m - 1, k + m - 1, k * m, 8);
    // update _encode_matrix
    // 为新生成的块更新编码矩阵，即用生成的new_encode_matrix填充进原来encode_matrix坏掉的块的对应的m行
    for (int i = 0; i < m; i++) {
      int row = lostidx * m * k * m + i * m * k;
      memcpy(_encode_matrix + row,
             new_encode_matrix + (i * k * m),
             sizeof(int) * k * m);
    }

    free(new_encode_matrix);

    using namespace std;
    cout << "new encode matrix" << endl;
    for (int i = 0; i < (k + m) * m; i++) {
      for (int j = 0; j < k * m; j++) {
        cout << _encode_matrix[i * k * m + j] << "\t";
        cout << "\t";
      }
      cout << endl;
    }
    /**repair lost chunks*/

    std::cout << "repair lost chunks" << std::endl;
    // buffer for the lost chunk
    char *new_coding[m];

    for (int i = 0; i < m; i++) {
      new_coding[i] = new char[sub_chunksize];
    }

    // repair the lost chunk
    std::cout << "jerasure dotprod" << std::endl;
    for (int i = 0; i < m; i++) {
      std::cout << i << std::endl;
      int dest_id = i + (k + m - 1);
      int matrix_row = i * (k + m - 1);
      jerasure_matrix_dotprod(k + m - 1,
                              8,
                              _repair_matrix + matrix_row,
                              NULL,
                              dest_id,
                              coding_slice,
                              new_coding,
                              sub_chunksize);
    }

    std::cout << "decoded append" << std::endl;
    (*decoded)[lostidx].clear();
    for (int i = 0; i < m; i++)
      (*decoded)[lostidx].append(new_coding[i], sub_chunksize);

    for (int i = 0; i < n - 1; i++) {
      delete[] coding_slice[i];
    }

    for (int i = 0; i < m; i++) {
      delete[] new_coding[i];
    }
    // row_idx = (row_idx + 1) % m; //
    // 更新_row_idx，因为是nsys，所以解码的时候不需要用更新后的encode_matrix，只需要用之前的repair_matrix生成一个新的线性组合就可以了
  }

  std::cout << "decoded size after repair:" << decoded->size() << std::endl;
  // normal read

  std::cout << "normal read" << std::endl;

  int select_matrix[(k * m) * (k * m)];
  int invert_matrix[(k * m) * (k * m)];

  // select _m*_k row from _encode_matrix
  // 正常读才需要从encode_matrix里面取出矩阵进行解码得到原始数据块
  for (int i = 0; i < k * m; i++) {
    // matrix中选的行和编码块中选的分片对应
    memcpy(select_matrix + i * k * m,
           _encode_matrix + i * k * m,
           sizeof(int) * k * m);
  }

  jerasure_invert_matrix(select_matrix, invert_matrix, k * m, 8);

  // the k*m slices from code blocks
  char *coding_blocks[k * m];
  char *data[k * m];

  for (int i = 0; i < k; i++) {
    char *temp = (*decoded)[i].c_str();
    for (int j = 0; j < m; j++) {
      // 将coding_blocks指向decoded各个子块
      coding_blocks[i * m + j] = temp;
      temp += sub_chunksize;
    }
  }

  // 给data分配空间
  // 不能直接将data指向decoded，而是需要重新分配空间，计算后再复制到decoded
  // 这是为了避免jerasure计算过程中将计算结果填充到decoded中而影响计算结果
  for (int i = 0; i < k; i++) {
    for (int j = 0; j < m; j++) {
      // data[i*m+j] = static_cast<char *>(malloc(sub_chunksize));
      data[i * m + j] = new char[sub_chunksize];
    }
  }

  for (int i = 0; i < k * m; i++) {
    int dest_id = i + k * m;
    int matrix_row = i * (k * m);
    jerasure_matrix_dotprod(k * m,
                            8,
                            invert_matrix + matrix_row,
                            NULL,
                            dest_id,
                            coding_blocks,
                            data,
                            sub_chunksize);
  }

  //   std::cout<<"data after normal read"<<std::endl;
  //   for (int i = 0; i < k*m; i++) {
  //       std::cout<<i << ": ";
  // //        std::cout << coding_ptrs[i] << std::endl;
  //       for(int j = 0; j < sub_chunksize; j++){
  //           std::cout<<j<<":"<<unsigned(data[i][j])%(256) << " ";
  //       }
  //       std::cout << std::endl;
  //   }

  for (int i = 0; i < k; i++) {
    (*decoded)[i].clear();
    for (int j = 0; j < m; j++) {
      (*decoded)[i].append(data[i * m + j], sub_chunksize);
    }
  }

  std::cout << "decoded size == k + m:" << (decoded->size() == k + m)
            << std::endl;

  for (int i = 0; i < k * m; i++) {
    delete[] data[i];
  }

  return 0;
}

} // namespace ec

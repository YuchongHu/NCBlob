/*
 * Forked from:
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef LIBEC_ERASURE_CODE_HH
#define LIBEC_ERASURE_CODE_HH

/*!
 * @file erasure_code.hh
 * @brief Base class for erasure code plugins implementors
 */

#include "erasure_code_intf.hpp"
#include <vector>

namespace ec {

std::vector<char> mergeMatrixChunk(const std::vector<int> &matrix_row, const std::vector<char> &chunk);

void splitMatrixChunk(const std::vector<char> &data, std::vector<int> &matrix_row, std::vector<char> &chunk, int intNum);


class ErasureCode : public ErasureCodeInterface {
public:
  static const unsigned SIMD_ALIGN;

  std::vector<int> chunk_mapping;
  ErasureCodeProfile _profile;

  ~ErasureCode() override = default;

  auto init(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;

  [[nodiscard]] auto get_profile() const
      -> const ErasureCodeProfile & override {
    return _profile;
  }

  auto sanity_check_k_m(int k, int m, std::ostream *ss) -> int;

  [[nodiscard]] auto get_coding_chunk_count() const -> unsigned int override {
    return get_chunk_count() - get_data_chunk_count();
  }

  using Type = const std::string;
    virtual Type get_class_name() const {
      return "ErasureCode";// 默认实现
    }; 

  virtual void set_row_idx(int row_idx){};

  virtual void set_encode_matrix(const std::vector<std::vector<int>> &encode_matrix){};

  virtual void get_encode_matrix(std::vector<std::vector<int>> &encode_matrix){};

  auto get_sub_chunk_count() -> int override { return 1; }

  virtual auto _minimum_to_decode(const std::set<int> &want_to_read,
                                  const std::set<int> &available_chunks,
                                  std::set<int> *minimum) -> int;

  auto minimum_to_decode(
      const std::set<int> &want_to_read, const std::set<int> &available,
      std::map<int, std::vector<std::pair<int, int>>> *minimum) -> int override;

  auto minimum_to_decode_with_cost(const std::set<int> &want_to_read,
                                   const std::map<int, int> &available,
                                   std::set<int> *minimum) -> int override;

  auto encode_prepare(const bufferlist &raw,
                      std::map<int, bufferlist> &encoded) const -> int;

  auto encode(const std::set<int> &want_to_encode, const bufferlist &in,
              std::map<int, bufferlist> *encoded) -> int override;

  auto decode(const std::set<int> &want_to_read,
              const std::map<int, bufferlist> &chunks,
              std::map<int, bufferlist> *decoded, int chunk_size)
      -> int override;

  virtual auto _decode(const std::set<int> &want_to_read,
                       const std::map<int, bufferlist> &chunks,
                       std::map<int, bufferlist> *decoded) -> int;

  [[nodiscard]] auto get_chunk_mapping() const
      -> const std::vector<int> & override;

  auto to_mapping(const ErasureCodeProfile &profile, std::ostream *ss) -> int;

  static auto to_int(const std::string &name, ErasureCodeProfile &profile,
                     int *value, const std::string &default_value,
                     std::ostream *ss) -> int;

  static auto to_bool(const std::string &name, ErasureCodeProfile &profile,
                      bool *value, const std::string &default_value,
                      std::ostream *ss) -> int;

  static auto to_string(const std::string &name, ErasureCodeProfile &profile,
                        std::string *value, const std::string &default_value,
                        std::ostream *ss) -> int;

  auto decode_concat(const std::map<int, bufferlist> &chunks,
                     bufferlist *decoded) -> int override;

protected:
  auto parse(const ErasureCodeProfile &profile, std::ostream *ss) -> int;

private:
  [[nodiscard]] auto chunk_index(unsigned int i) const -> int;
};
} // namespace ec

#endif

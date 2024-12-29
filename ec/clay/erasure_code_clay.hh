/*
 * Forked from:
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Indian Institute of Science <office.ece@iisc.ac.in>
 *
 * Author: Myna Vajha <mynaramana@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_CLAY_H
#define CEPH_ERASURE_CODE_CLAY_H

#include "erasure_code.hh"

#include "rados/buffer_fwd.h"

namespace ec {
class ErasureCodeClay final : public ErasureCode {
private:
  std::string DEFAULT_K{"4"};
  std::string DEFAULT_M{"2"};
  std::string DEFAULT_W{"8"};
  int k = 0, m = 0, d = 0, w = 8;
  int q = 0, t = 0, nu = 0;
  int sub_chunk_no = 0;

  // U_buf is introduced to store the sub-chunks associated with the uncoupled
  std::map<int, ceph::bufferlist> U_buf;

  struct ScalarMDS {
    ec::ErasureCodeInterfaceRef erasure_code;
    ec::ErasureCodeProfile profile;
  };
  ScalarMDS mds;
  ScalarMDS pft;

public:
  explicit ErasureCodeClay() = default;

  ~ErasureCodeClay() override;

  Type get_class_name() const override {
      return "ErasureCodeClay";
  }

  [[nodiscard]] auto get_chunk_count() const -> unsigned int override {
    return k + m;
  }

  [[nodiscard]] auto get_data_chunk_count() const -> unsigned int override {
    return k;
  }

  auto get_sub_chunk_count() -> int override { return sub_chunk_no; }

  [[nodiscard]] auto get_chunk_size(unsigned int object_size) const
      -> unsigned int override;

  auto minimum_to_decode(
      const std::set<int> &want_to_read, const std::set<int> &available,
      std::map<int, std::vector<std::pair<int, int>>> *minimum) -> int override;

  auto decode(const std::set<int> &want_to_read,
              const std::map<int, ceph::bufferlist> &chunks,
              std::map<int, ceph::bufferlist> *decoded, int chunk_size)
      -> int override;

  auto encode_chunks(const std::set<int> &want_to_encode,
                     std::map<int, ceph::bufferlist> *encoded) -> int override;

  auto decode_chunks(const std::set<int> &want_to_read,
                     const std::map<int, ceph::bufferlist> &chunks,
                     std::map<int, ceph::bufferlist> *decoded) -> int override;

  auto init(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;

  [[nodiscard]] auto is_repair(const std::set<int> &want_to_read,
                               const std::set<int> &available_chunks) const
      -> int;

  [[nodiscard]] auto
  get_repair_sub_chunk_count(const std::set<int> &want_to_read) const -> int;

  virtual auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int;

private:
  auto minimum_to_repair(
      const std::set<int> &want_to_read, const std::set<int> &available_chunks,
      std::map<int, std::vector<std::pair<int, int>>> *minimum) -> int;

  auto repair(const std::set<int> &want_to_read,
              const std::map<int, ceph::bufferlist> &chunks,
              std::map<int, ceph::bufferlist> *recovered, int chunk_size)
      -> int;

  auto decode_layered(std::set<int> &erased_chunks,
                      std::map<int, ceph::bufferlist> *chunks) -> int;

  auto repair_one_lost_chunk(
      std::map<int, ceph::bufferlist> &recovered_data,
      std::set<int> &aloof_nodes, std::map<int, ceph::bufferlist> &helper_data,
      int repair_blocksize,
      std::vector<std::pair<int, int>> &repair_sub_chunks_ind) -> int;

  void get_repair_subchunks(
      const int &lost_node,
      std::vector<std::pair<int, int>> &repair_sub_chunks_ind) const;

  auto decode_erasures(const std::set<int> &erased_chunks, int z,
                       std::map<int, ceph::bufferlist> *chunks, int sc_size)
      -> int;

  auto decode_uncoupled(const std::set<int> &erased_chunks, int z, int sc_size)
      -> int;

  void set_planes_sequential_decoding_order(int *order,
                                            std::set<int> &erasures);

  void recover_type1_erasure(std::map<int, ceph::bufferlist> *chunks, int x,
                             int y, int z, const int *z_vec, int sc_size);

  void get_uncoupled_from_coupled(std::map<int, ceph::bufferlist> *chunks,
                                  int x, int y, int z, const int *z_vec,
                                  int sc_size);

  void get_coupled_from_uncoupled(std::map<int, ceph::bufferlist> *chunks,
                                  int x, int y, int z, const int *z_vec,
                                  int sc_size);

  void get_plane_vector(int z, int *z_vec) const;

  auto get_max_iscore(std::set<int> &erased_chunks) const -> int;
};

} // namespace ec
#endif

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#ifndef EC_ERASURE_CODE_JERASURE_HH
#define EC_ERASURE_CODE_JERASURE_HH

#include "erasure_code.hh"

namespace ec {

class ErasureCodeJerasure : public ec::ErasureCode {
public:
  int k{0};
  std::string DEFAULT_K;
  int m{0};
  std::string DEFAULT_M;
  int w{0};
  std::string DEFAULT_W;
  const char *technique;
  std::string rule_root;
  std::string rule_failure_domain;
  bool per_chunk_alignment{false};

  explicit ErasureCodeJerasure(const char *_technique)
      : DEFAULT_K("2"), DEFAULT_M("1"), DEFAULT_W("8"), technique(_technique) {}

  ~ErasureCodeJerasure() override = default;

  Type get_class_name() const override {
      return "ErasureCodeJerasure";
  }

  [[nodiscard]] auto get_chunk_count() const -> unsigned int override {
    return k + m;
  }

  [[nodiscard]] auto get_data_chunk_count() const -> unsigned int override {
    return k;
  }

  [[nodiscard]] auto get_chunk_size(unsigned int object_size) const
      -> unsigned int override;

  auto encode_chunks(const std::set<int> &want_to_encode,
                     std::map<int, ec::bufferlist> *encoded) -> int override;

  auto decode_chunks(const std::set<int> &want_to_read,
                     const std::map<int, ec::bufferlist> &chunks,
                     std::map<int, ec::bufferlist> *decoded) -> int override;

  auto init(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;

  virtual void jerasure_encode(char **data, char **coding, int blocksize) = 0;
  virtual auto jerasure_decode(int *erasures, char **data, char **coding,
                               int blocksize) -> int = 0;
  [[nodiscard]] virtual auto get_alignment() const -> unsigned = 0;
  virtual void prepare() = 0;
  static auto is_prime(int value) -> bool;

protected:
  virtual auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int;
};
class ErasureCodeJerasureReedSolomonVandermonde : public ErasureCodeJerasure {
public:
  int *matrix{nullptr};

  ErasureCodeJerasureReedSolomonVandermonde()
      : ErasureCodeJerasure("reed_sol_van") {
    DEFAULT_K = "7";
    DEFAULT_M = "3";
    DEFAULT_W = "8";
  }
  ~ErasureCodeJerasureReedSolomonVandermonde() override {
    if (matrix)
      free(matrix);
  }

  void jerasure_encode(char **data, char **coding, int blocksize) override;
  auto jerasure_decode(int *erasures, char **data, char **coding, int blocksize)
      -> int override;
  [[nodiscard]] auto get_alignment() const -> unsigned override;
  void prepare() override;

private:
  auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;
};

class ErasureCodeJerasureReedSolomonRAID6 : public ErasureCodeJerasure {
public:
  int *matrix{nullptr};

  ErasureCodeJerasureReedSolomonRAID6()
      : ErasureCodeJerasure("reed_sol_r6_op") {
    DEFAULT_K = "7";
    DEFAULT_M = "2";
    DEFAULT_W = "8";
  }
  ~ErasureCodeJerasureReedSolomonRAID6() override {
    if (matrix)
      free(matrix);
  }

  void jerasure_encode(char **data, char **coding, int blocksize) override;
  auto jerasure_decode(int *erasures, char **data, char **coding, int blocksize)
      -> int override;
  [[nodiscard]] auto get_alignment() const -> unsigned override;
  void prepare() override;

private:
  auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;
};

#define DEFAULT_PACKETSIZE "2048"

class ErasureCodeJerasureCauchy : public ErasureCodeJerasure {
public:
  int *bitmatrix{nullptr};
  int **schedule{nullptr};
  int packetsize{0};

  explicit ErasureCodeJerasureCauchy(const char *technique)
      : ErasureCodeJerasure(technique) {
    DEFAULT_K = "7";
    DEFAULT_M = "3";
    DEFAULT_W = "8";
  }
  ~ErasureCodeJerasureCauchy() override;

  void jerasure_encode(char **data, char **coding, int blocksize) override;
  auto jerasure_decode(int *erasures, char **data, char **coding, int blocksize)
      -> int override;
  [[nodiscard]] auto get_alignment() const -> unsigned override;
  void prepare_schedule(int *matrix);

private:
  auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;
};

class ErasureCodeJerasureCauchyOrig : public ErasureCodeJerasureCauchy {
public:
  ErasureCodeJerasureCauchyOrig() : ErasureCodeJerasureCauchy("cauchy_orig") {}

  void prepare() override;
};

class ErasureCodeJerasureCauchyGood : public ErasureCodeJerasureCauchy {
public:
  ErasureCodeJerasureCauchyGood() : ErasureCodeJerasureCauchy("cauchy_good") {}

  void prepare() override;
};

class ErasureCodeJerasureLiberation : public ErasureCodeJerasure {
public:
  int *bitmatrix{nullptr};
  int **schedule{nullptr};
  int packetsize{0};

  explicit ErasureCodeJerasureLiberation(const char *technique = "liberation")
      : ErasureCodeJerasure(technique) {
    DEFAULT_K = "2";
    DEFAULT_M = "2";
    DEFAULT_W = "7";
  }
  ~ErasureCodeJerasureLiberation() override;

  void jerasure_encode(char **data, char **coding, int blocksize) override;
  auto jerasure_decode(int *erasures, char **data, char **coding, int blocksize)
      -> int override;
  [[nodiscard]] auto get_alignment() const -> unsigned override;
  virtual auto check_k(std::ostream *ss) const -> bool;
  virtual auto check_w(std::ostream *ss) const -> bool;
  virtual auto check_packetsize_set(std::ostream *ss) const -> bool;
  virtual auto check_packetsize(std::ostream *ss) const -> bool;
  virtual auto revert_to_default(ec::ErasureCodeProfile &profile,
                                 std::ostream *ss) -> int;
  void prepare() override;

private:
  auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;
};

class ErasureCodeJerasureBlaumRoth : public ErasureCodeJerasureLiberation {
public:
  ErasureCodeJerasureBlaumRoth()
      : ErasureCodeJerasureLiberation("blaum_roth") {}

  auto check_w(std::ostream *ss) const -> bool override;
  void prepare() override;
};

class ErasureCodeJerasureLiber8tion : public ErasureCodeJerasureLiberation {
public:
  ErasureCodeJerasureLiber8tion()
      : ErasureCodeJerasureLiberation("liber8tion") {
    DEFAULT_K = "2";
    DEFAULT_M = "2";
    DEFAULT_W = "8";
  }

  void prepare() override;

private:
  auto parse(ec::ErasureCodeProfile &profile, std::ostream *ss) -> int override;
};

} // namespace ec
#endif

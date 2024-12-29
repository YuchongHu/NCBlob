#include "ec_intf.hh"
#include "erasure_code.hh"
#include "erasure_code_factory.hpp"
#include "erasure_code_intf.hpp"
#include "meta.hpp"

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

using namespace ec;
void ec::encode(meta::EcType ec_type, int k, int m,
                const std::vector<char> &raw_data,
                std::vector<std::vector<char>> &matrix_encoded) {
  std::ostringstream errors;
  ErasureCodeProfile profile;

  profile["k"] = std::to_string(k);
  profile["m"] = std::to_string(m);
  // prepare input data bufferlist
  auto size_in_bytes = raw_data.size();
  bufferlist in;
  bufferptr in_ptr(ceph::buffer::create_page_aligned(size_in_bytes));
  in_ptr.zero();
  in_ptr.set_length(0);
  in_ptr.append(raw_data.data(), size_in_bytes);
  // bufferlist in;
  in.push_back(in_ptr);

  std::set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }

  // start to encode
  if (ec_type == meta::EcType::CLAY) {
    ErasureCodeInterfaceRef intf =
        ErasureCodeClayFactory{}.make(profile, errors);
    auto &clay = dynamic_cast<ErasureCode &>(*intf.get());

    std::map<int, bufferlist> encoded;
    if (clay.encode(want_to_encode, in, &encoded) != 0) {
      std::cerr << "Clay encode error" << std::endl;
    };

    // for Clay, don't need to merge the matrix_row before chunk data
    for (int i = 0; i < encoded.size(); i++) {
      std::vector<char> chunk(encoded[i].c_str(),
                              encoded[i].c_str() + encoded[i].length());
      matrix_encoded.push_back(chunk);
    }

  } else if (ec_type == meta::EcType::RS) {
    ErasureCodeInterfaceRef intf =
        ErasureCodeJerasureFactory{}.make(profile, errors);
    auto &jerasure = dynamic_cast<ErasureCode &>(*intf.get());

    std::map<int, bufferlist> encoded;
    if (jerasure.encode(want_to_encode, in, &encoded) != 0) {
      std::cerr << "RS encode error" << std::endl;
    };

    // for RS, don't need to merge the matrix_row before chunk data
    for (int i = 0; i < encoded.size(); i++) {
      std::vector<char> chunk(encoded[i].c_str(),
                              encoded[i].c_str() + encoded[i].length());
      matrix_encoded.push_back(chunk);
    }

  } else if (ec_type == meta::EcType::NSYS) {
    ErasureCodeInterfaceRef intf =
        ErasureCodeLonseFactory{}.make(profile, errors);
    auto &NSYS = dynamic_cast<ErasureCode &>(*intf.get());

    std::map<int, bufferlist> encoded;
    if (NSYS.encode(want_to_encode, in, &encoded) != 0) {
      std::cerr << "NSYS encode error" << std::endl;
    }
    std::vector<std::vector<int>> encode_matrix;
    NSYS.get_encode_matrix(encode_matrix);

    const int row = encode_matrix.size();    // n*m
    const int col = encode_matrix[0].size(); // k*m

    assert(row == (k + m) * m);
    assert(col == k * m);

    // TODOfor NSYS, there should be matrix_row before the encoded chunk data
    for (int i = 0; i < encoded.size(); i++) {
      std::vector<char> chunk(encoded[i].c_str(),
                              encoded[i].c_str() + encoded[i].length());
      matrix_encoded.push_back(chunk);
    }
    // for (int i = 0; i < k + m; i++) {
    //   std::vector<int> matrix_row;
    //   for (int j = 0; j < m; j++) {
    //     matrix_row.insert(matrix_row.end(), encode_matrix[i * m + j].begin(),
    //                       encode_matrix[i * m + j].end());
    //   }
    //   std::vector<char> chunk(encoded[i].c_str(),
    //                           encoded[i].c_str() + encoded[i].length());
    //   auto matrix_chunk = mergeMatrixChunk(matrix_row, chunk);
    //   matrix_encoded.push_back(matrix_chunk);
    // }
  }
}

auto ec::encoder::rs::Encoder::encode(const std::vector<char> &raw_data)
    -> std::vector<std::vector<char>> {
  auto [k, m] = get_km();
  assert(raw_data.size() % k == 0);
  auto stripe = std::vector<std::vector<char>>{};
  stripe.reserve(k + m);
  ::ec::encode(meta::EcType::RS, k, m, raw_data, stripe);
  return stripe;
}

auto ec::encoder::nsys::Encoder::encode(const std::vector<char> &raw_data)
    -> std::vector<std::vector<char>> {
  auto [k, m] = get_km();
  // Modified by Edgar: the `encode` will do the padding automatically
  // assert(raw_data.size() % k == 0);
  auto stripe = std::vector<std::vector<char>>{};
  stripe.reserve(k + m);
  ::ec::encode(meta::EcType::NSYS, k, m, raw_data, stripe);
  return stripe;
}

auto ec::encoder::clay::Encoder::encode(const std::vector<char> &raw_data)
    -> std::vector<std::vector<char>> {
  auto [k, m] = get_km();
  // Modified by Edgar: the `encode` will do the padding automatically
  // assert(raw_data.size() % k == 0);
  auto stripe = std::vector<std::vector<char>>{};
  stripe.reserve(k + m);
  ::ec::encode(meta::EcType::CLAY, k, m, raw_data, stripe);
  return stripe;
}

auto ec::encoder::rs::Encoder::get_sub_chunk_num() -> std::size_t { return 1; }
auto ec::encoder::nsys::Encoder::get_sub_chunk_num() -> std::size_t {
  return get_km().second;
}
auto ec::encoder::clay::Encoder::get_sub_chunk_num() -> std::size_t {
  auto [k, m] = get_km();
  if (k == 4 && m == 2) {
    return 8;
  } else if (k == 6 && m == 3) {
    return 27;
  } else if (k == 8 && m == 4) {
    return 64;
  } else if (k == 10 && m == 4) {
    return 256;
  } else {
    throw std::runtime_error("Unsupported clay code parameters");
  }
}
auto ec::make_encoder(meta::EcType ec_type, meta::ec_param_t k,
                      meta::ec_param_t m) -> std::unique_ptr<encoder::Encoder> {
  switch (ec_type) {
  case meta::EcType::RS:
    return std::make_unique<encoder::rs::Encoder>(k, m);
  case meta::EcType::NSYS:
    return std::make_unique<encoder::nsys::Encoder>(k, m);
  case meta::EcType::CLAY:
    return std::make_unique<encoder::clay::Encoder>(k, m);
  default:
    throw std::invalid_argument{"unsupported ec type"};
  }
};
auto ec::encoder::rs::Encoder::get_ec_type() -> meta::EcType {
  return meta::EcType::RS;
};
auto ec::encoder::nsys::Encoder::get_ec_type() -> meta::EcType {
  return meta::EcType::NSYS;
};
auto ec::encoder::clay::Encoder::get_ec_type() -> meta::EcType {
  return meta::EcType::CLAY;
};
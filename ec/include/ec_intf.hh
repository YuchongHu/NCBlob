#pragma once

#include "meta.hpp"

#include <boost/numeric/conversion/cast.hpp>
#include <cassert>
#include <cstddef>
#include <vector>

namespace ec {

namespace encoder {
class Encoder {
private:
  int k_{0};
  int m_{0};

public:
  Encoder() = default;
  Encoder(meta::ec_param_t k, meta::ec_param_t m)
      : k_(boost::numeric_cast<int>(k)), m_(boost::numeric_cast<int>(m)) {}
  Encoder(const Encoder &) = default;
  auto operator=(const Encoder &) -> Encoder & = default;
  Encoder(Encoder &&) = default;
  auto operator=(Encoder &&) -> Encoder & = default;
  virtual ~Encoder() = default;
  virtual auto encode(const std::vector<char> &raw_data)
      -> std::vector<std::vector<char>> = 0;
  virtual auto get_sub_chunk_num() -> std::size_t = 0;
  virtual auto get_ec_type() -> meta::EcType = 0;
  auto get_km() -> std::pair<meta::ec_param_t, meta::ec_param_t> {
    return {boost::numeric_cast<meta::ec_param_t>(k_),
            boost::numeric_cast<meta::ec_param_t>(m_)};
  }
};

namespace rs {
class Encoder : virtual public encoder::Encoder {
public:
  Encoder(meta::ec_param_t k, meta::ec_param_t m)
      : ec::encoder::Encoder(k, m) {}

  auto encode(const std::vector<char> &raw_data)
      -> std::vector<std::vector<char>> override;
  auto get_sub_chunk_num() -> std::size_t override;
  auto get_ec_type() -> meta::EcType override;
};
} // namespace rs
namespace nsys {
class Encoder : virtual public encoder::Encoder {
public:
  Encoder(meta::ec_param_t k, meta::ec_param_t m)
      : ec::encoder::Encoder(k, m) {}
  auto encode(const std::vector<char> &raw_data)
      -> std::vector<std::vector<char>> override;
  auto get_sub_chunk_num() -> std::size_t override;
  auto get_ec_type() -> meta::EcType override;
};
} // namespace nsys

namespace clay {
class Encoder : virtual public encoder::Encoder {
public:
  Encoder(meta::ec_param_t k, meta::ec_param_t m)
      : ec::encoder::Encoder(k, m) {}
  auto encode(const std::vector<char> &raw_data)
      -> std::vector<std::vector<char>> override;
  auto get_sub_chunk_num() -> std::size_t override;
  auto get_ec_type() -> meta::EcType override;
};
} // namespace clay
} // namespace encoder
/**
 * @description:
 * @param {meta::EcType} ec_type : Clay / RS / NSYS
 * @param {int} k
 * @param {int} m
 * @param {vector<char>} &raw_data
 * @param {vector<std::vector<char>>} &matrix_encoded : the encode result data.
 * Put the matrix before the data only for NSYS
 */
void encode(meta::EcType ec_type, int k, int m,
            const std::vector<char> &raw_data,
            std::vector<std::vector<char>> &matrix_encoded);

using encoder_ptr = std::unique_ptr<encoder::Encoder>;
auto make_encoder(meta::EcType ec_type, meta::ec_param_t k,
                  meta::ec_param_t m) -> encoder_ptr;
} // namespace ec

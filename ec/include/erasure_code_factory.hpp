#ifndef ERASURE_CODE_FACTORY_HPP
#define ERASURE_CODE_FACTORY_HPP

#include "erasure_code_intf.hpp"

namespace ec {

class ErasureCodeFactory {
public:
  virtual ~ErasureCodeFactory() noexcept = default;
  ErasureCodeFactory() noexcept = default;
  ErasureCodeFactory(const ErasureCodeFactory &) noexcept = default;
  ErasureCodeFactory(ErasureCodeFactory &&) noexcept = delete;
  auto operator=(const ErasureCodeFactory &) noexcept
      -> ErasureCodeFactory & = default;
  auto operator=(ErasureCodeFactory &&) noexcept
      -> ErasureCodeFactory & = delete;

  virtual auto make(ErasureCodeProfile profile, std::ostream &os)
      -> ErasureCodeInterfaceRef = 0;
};

class ErasureCodeJerasureFactory : public ErasureCodeFactory {
public:
  ErasureCodeJerasureFactory() noexcept = default;
  ErasureCodeJerasureFactory(const ErasureCodeJerasureFactory &) noexcept =
      default;
  ErasureCodeJerasureFactory(ErasureCodeJerasureFactory &&) noexcept = delete;
  auto operator=(const ErasureCodeJerasureFactory &) noexcept
      -> ErasureCodeJerasureFactory & = default;
  auto operator=(ErasureCodeJerasureFactory &&) noexcept
      -> ErasureCodeJerasureFactory & = delete;
  ~ErasureCodeJerasureFactory() noexcept override = default;

  auto make(ErasureCodeProfile profile, std::ostream &os)
      -> ErasureCodeInterfaceRef override;
};

class ErasureCodeClayFactory : public ErasureCodeFactory {

public:
  ErasureCodeClayFactory() noexcept = default;
  ErasureCodeClayFactory(const ErasureCodeClayFactory &) noexcept = default;
  ErasureCodeClayFactory(ErasureCodeClayFactory &&) noexcept = delete;
  auto operator=(const ErasureCodeClayFactory &) noexcept
      -> ErasureCodeClayFactory & = default;
  auto operator=(ErasureCodeClayFactory &&) noexcept
      -> ErasureCodeClayFactory & = delete;
  ~ErasureCodeClayFactory() noexcept override = default;

  auto make(ErasureCodeProfile profile, std::ostream &os)
      -> ErasureCodeInterfaceRef override;
};

class ErasureCodeLonseFactory : public ErasureCodeFactory {

public:
  ErasureCodeLonseFactory() noexcept = default;
  ErasureCodeLonseFactory(const ErasureCodeLonseFactory &) noexcept = default;
  ErasureCodeLonseFactory(ErasureCodeLonseFactory &&) noexcept = delete;
  auto operator=(const ErasureCodeLonseFactory &) noexcept
      -> ErasureCodeLonseFactory & = default;
  auto operator=(ErasureCodeLonseFactory &&) noexcept
      -> ErasureCodeLonseFactory & = delete;
  ~ErasureCodeLonseFactory() noexcept override = default;

  auto make(ErasureCodeProfile profile, std::ostream &os)
      -> ErasureCodeInterfaceRef override;
};
} // namespace ec
#endif
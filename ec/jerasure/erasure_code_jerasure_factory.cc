

#include "erasure_code_factory.hpp"
#include "erasure_code_jerasure.hh"
#include <memory>
#include <sstream>

namespace ec {

auto ErasureCodeJerasureFactory::make(
    ErasureCodeProfile profile, std::ostream &os) -> ErasureCodeInterfaceRef {
  std::unique_ptr<ErasureCodeJerasure> interface;
  std::string t;
  if (profile.find("technique") != profile.end()) {
    t = profile.find("technique")->second;
  } else {
    // default
    t = "reed_sol_van";
  }
  if (t == "reed_sol_van") {
    interface = std::make_unique<ErasureCodeJerasureReedSolomonVandermonde>();
  } else if (t == "reed_sol_r6_op") {
    interface = std::make_unique<ErasureCodeJerasureReedSolomonRAID6>();
  } else if (t == "cauchy_orig") {
    interface = std::make_unique<ErasureCodeJerasureCauchyOrig>();
  } else if (t == "cauchy_good") {
    interface = std::make_unique<ErasureCodeJerasureCauchyGood>();
  } else if (t == "liberation") {
    interface = std::make_unique<ErasureCodeJerasureLiberation>();
  } else if (t == "blaum_roth") {
    interface = std::make_unique<ErasureCodeJerasureBlaumRoth>();
  } else if (t == "liber8tion") {
    interface = std::make_unique<ErasureCodeJerasureLiber8tion>();
  } else {
    std::stringstream ss{};
    ss << "technique=" << t << " is not a valid coding technique. "
       << " Choose one of the following: "
       << "reed_sol_van, reed_sol_r6_op, cauchy_orig, "
       << "cauchy_good, liberation, blaum_roth, liber8tion";
    os << ss.rdbuf();
    return nullptr;
  }
  std::stringstream ss{};
  ss << __func__ << ": " << profile << std::endl;
  os << ss.rdbuf();
  int r = interface->init(profile, &os);
  if (r) {
    return nullptr;
  }
  return {std::move(interface)};
};
} // namespace ec

#include "erasure_code_Lonse.hh"
#include "erasure_code_factory.hpp"
#include "exception.hpp"
#include <utility>
namespace ec {
auto ErasureCodeLonseFactory::make(ErasureCodeProfile profile,
                                                     std::ostream &os) -> ErasureCodeInterfaceRef {
  auto interface = std::make_unique<ErasureCodeLonse>();
  if (interface->init(profile, &os) != 0) {
    return nullptr;
  }
  return {std::move(interface)};
};
} // namespace ec
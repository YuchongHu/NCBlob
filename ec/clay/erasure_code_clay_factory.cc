#include "erasure_code_clay.hh"
#include "erasure_code_factory.hpp"
#include "exception.hpp"
#include <utility>
namespace ec {
auto ErasureCodeClayFactory::make(ErasureCodeProfile profile,
                                                     std::ostream &os) -> ErasureCodeInterfaceRef {
  auto interface = std::make_unique<ErasureCodeClay>();
  if (interface->init(profile, &os) != 0) {
    return nullptr;
  }
  return {std::move(interface)};
};
} // namespace ec

add_library(ec OBJECT "")
target_link_libraries(ec rados ec_jerasure)
set(EC_PUBLIC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(EC_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})

target_sources(ec PRIVATE 
    ${EC_SRC_DIR}/erasure_code.cc
    ${EC_SRC_DIR}/ec_intf.cc
    ${EC_SRC_DIR}/str_util.cc
    ${EC_SRC_DIR}/clay/erasure_code_clay.cc
    ${EC_SRC_DIR}/clay/erasure_code_clay_factory.cc
    ${EC_SRC_DIR}/Lonse/erasure_code_Lonse.cc
    ${EC_SRC_DIR}/Lonse/erasure_code_Lonse_factory.cc
    ${EC_SRC_DIR}/Lonse/Computation.cc
    ${EC_SRC_DIR}/jerasure/erasure_code_jerasure_factory.cc
    ${EC_SRC_DIR}/jerasure/erasure_code_jerasure.cc
)
target_include_directories(ec PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../meta)
target_include_directories(ec PUBLIC ${EC_PUBLIC_INCLUDE_DIR})


# lonse plugin

set(Lonse_utils_src
  ErasureCodePluginLonse.cc
  ErasureCodeLonse.cc)

add_library(Lonse_utils OBJECT ${Lonse_utils_src})


set(gf_srcs
  gf-complete/src/gf_cpu.c
  gf-complete/src/gf_wgen.c
  gf-complete/src/gf_w16.c
  gf-complete/src/gf.c
  gf-complete/src/gf_w32.c
  gf-complete/src/gf_w64.c
  gf-complete/src/gf_w128.c
  gf-complete/src/gf_general.c
  gf-complete/src/gf_w4.c
  gf-complete/src/gf_rand.c
  gf-complete/src/gf_w8.c)

if(HAVE_ARM_NEON OR HAVE_ARMV8_SIMD)
  list(APPEND gf_srcs
    gf-complete/src/neon/gf_w4_neon.c
    gf-complete/src/neon/gf_w8_neon.c
    gf-complete/src/neon/gf_w16_neon.c
    gf-complete/src/neon/gf_w32_neon.c
    gf-complete/src/neon/gf_w64_neon.c)
endif()

add_library(gf_objs OBJECT ${gf_srcs})
set_target_properties(gf_objs PROPERTIES 
  COMPILE_FLAGS "${SIMD_COMPILE_FLAGS}")
set_target_properties(gf_objs PROPERTIES 
  COMPILE_DEFINITIONS "${GF_COMPILE_FLAGS}")

set(j_srcs
  jerasure/src/cauchy.c
  jerasure/src/galois.c
  jerasure/src/jerasure.c
  jerasure/src/liberation.c
  jerasure/src/reed_sol.c
  Computation.cc)
add_library(j_objs OBJECT ${j_srcs})

# set(j_srcs
#   galois.c
#   jerasure.c
#   Computation.cc
#   include.hh)
# add_library(j_objs OBJECT ${j_srcs}) 

set(ec_Lonse_objs
  $<TARGET_OBJECTS:gf_objs>
  $<TARGET_OBJECTS:j_objs>
  $<TARGET_OBJECTS:Lonse_utils>
  $<TARGET_OBJECTS:erasure_code_objs>)

add_library(ec_Lonse SHARED ${ec_Lonse_objs})
set_target_properties(ec_Lonse PROPERTIES
  INSTALL_RPATH "")
install(TARGETS ec_Lonse DESTINATION ${erasure_plugin_dir})

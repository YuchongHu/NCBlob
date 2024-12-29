# Set the CFLAGS correctly for gf-complete based on SIMD compiler support
set(GF_COMPILE_FLAGS)
if(HAVE_ARMV8_SIMD)
  list(APPEND GF_COMPILE_FLAGS ARM_NEON ARCH_AARCH64)
endif()
if(HAVE_ARM_NEON)
  list(APPEND GF_COMPILE_FLAGS ARM_NEON)
endif()
if(HAVE_INTEL_SSE)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE)
endif()
if(HAVE_INTEL_SSE2)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE2)
endif()
if(HAVE_INTEL_SSE3)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE3)
endif()
if(HAVE_INTEL_SSSE3)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSSE3)
endif()
if(HAVE_INTEL_PCLMUL)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4_PCLMUL)
endif()
if(HAVE_INTEL_SSE4_1)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4)
endif()
if(HAVE_INTEL_SSE4_2)
  list(APPEND GF_COMPILE_FLAGS INTEL_SSE4)
endif()

# set this to TRUE to enable debugging of SIMD detection
# inside gf-complete. gf-complete will printf the SIMD
# instructions detected to stdout.
if (FALSE)
  list(APPEND GF_COMPILE_FLAGS DEBUG_CPU_DETECTION)
endif()

set(gf-complete_srcs
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
  list(APPEND gf-complete_srcs
    gf-complete/src/neon/gf_w4_neon.c
    gf-complete/src/neon/gf_w8_neon.c
    gf-complete/src/neon/gf_w16_neon.c
    gf-complete/src/neon/gf_w32_neon.c
    gf-complete/src/neon/gf_w64_neon.c)
endif()

add_library(gf-complete_objs OBJECT ${gf-complete_srcs})
target_include_directories(gf-complete_objs SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/gf-complete/include)
set_target_properties(gf-complete_objs PROPERTIES 
  COMPILE_FLAGS "${SIMD_COMPILE_FLAGS}")
set_target_properties(gf-complete_objs PROPERTIES 
  COMPILE_DEFINITIONS "${GF_COMPILE_FLAGS}")

set(jerasure_srcs
  jerasure/src/cauchy.c
  jerasure/src/galois.c
  jerasure/src/jerasure.c
  jerasure/src/liberation.c
  jerasure/src/reed_sol.c
  )
add_library(jerasure_objs OBJECT ${jerasure_srcs}) 
target_include_directories(jerasure_objs SYSTEM PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/jerasure/include ${CMAKE_CURRENT_SOURCE_DIR}/gf-complete/include)

set(ec_jerasure_objs
  $<TARGET_OBJECTS:gf-complete_objs>
  $<TARGET_OBJECTS:jerasure_objs>)

add_library(ec_jerasure STATIC ${ec_jerasure_objs})
target_include_directories(ec_jerasure SYSTEM PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/gf-complete/include
  ${CMAKE_CURRENT_SOURCE_DIR}/jerasure/include
  )


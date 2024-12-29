cmake_minimum_required(VERSION 3.15)


set(CMAKE_CXX_STANDARD 17)
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo build --release)
    set(TARGET_DIR "release")
endif ()

set(CRATE_NAME "tbr_rs")
set(CARGO_TARGET ${CMAKE_BINARY_DIR}/target)
set(CXX_BRIDGE ${CARGO_TARGET}/cxxbridge/${CRATE_NAME})
set(OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/out)
set(CARGO_FEATURES "--features=default")

set(RUST_PART_LIB "${CARGO_TARGET}/${TARGET_DIR}/lib${CRATE_NAME}.a")
set(RUST_PART_LIB_SHARED "${CARGO_TARGET}/${TARGET_DIR}/lib${CRATE_NAME}.so")

option(FEAT_SQLITE "Enable SQLITE backend feature" OFF)

set(RUST_PART_CXX 
    ${CXX_BRIDGE}/src/ffi/local_file_system.rs.cc
    ${CXX_BRIDGE}/src/ffi/memory_cache.rs.cc
    ${CXX_BRIDGE}/src/ffi/memmap.rs.cc
    ${CXX_BRIDGE}/src/ffi/azure_trace.rs.cc
)

if (FEAT_SQLITE)
    message(STATUS "Enabling SQLITE backend feature")
    list(APPEND RUST_PART_CXX  ${CXX_BRIDGE}/src/ffi/sqlite.rs.cc)
    list(APPEND CARGO_FEATURES "--features=sqlite")
endif()

add_library(tbr-rs EXCLUDE_FROM_ALL ${RUST_PART_CXX})
target_include_directories(tbr-rs PUBLIC ${OUT_DIR}/include/)
target_link_libraries(tbr-rs ${RUST_PART_LIB})
add_custom_command(
    OUTPUT ${RUST_PART_CXX} ${RUST_PART_LIB} ${RUST_PART_LIB_SHARED}
    # COMMAND ${CMAKE_COMMAND} -E echo "Environment variable RUST_FLAGS is $ENV{RUSTFLAGS}"
    COMMAND CARGO_TARGET_DIR=${CARGO_TARGET} ${CARGO_CMD} ${CARGO_FEATURES}
    DEPENDS 
        ${CMAKE_CURRENT_SOURCE_DIR}/build.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/local_file_system.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/memory_cache.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/sqlite.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/memmap.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/azure_trace.rs
    COMMENT "Building tbr Rust artifacts"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target(
    dump_tbr_ts_out
    # OUTPUT ${RUST_PART_CXX} ${RUST_PART_LIB}
    # COMMAND ${CMAKE_COMMAND} -E echo "Environment variable RUST_FLAGS is $ENV{RUSTFLAGS}"
    COMMAND CARGO_TARGET_DIR=${CARGO_TARGET} ${CARGO_CMD} ${CARGO_FEATURES}
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/local_file_system.rs.h ${OUT_DIR}/include/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/local_file_system.rs.cc ${OUT_DIR}/src/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/memory_cache.rs.h ${OUT_DIR}/include/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/memory_cache.rs.cc ${OUT_DIR}/src/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/sqlite.rs.h ${OUT_DIR}/include/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/sqlite.rs.cc ${OUT_DIR}/src/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/memmap.rs.h ${OUT_DIR}/include/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/memmap.rs.cc ${OUT_DIR}/src/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/azure_trace.rs.h ${OUT_DIR}/include/
    COMMAND ${CMAKE_COMMAND} -E copy ${CXX_BRIDGE}/src/ffi/azure_trace.rs.cc ${OUT_DIR}/src/
    COMMAND ${CMAKE_COMMAND} -E copy ${RUST_PART_LIB} ${OUT_DIR}/lib/
    COMMAND ${CMAKE_COMMAND} -E copy ${RUST_PART_LIB_SHARED} ${OUT_DIR}/lib/
    DEPENDS 
        ${CMAKE_CURRENT_SOURCE_DIR}/build.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/local_file_system.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/memory_cache.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/sqlite.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/memmap.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/src/ffi/azure_trace.rs
    COMMENT "Copying tbr Rust artifacts to the output directory"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(tbr-rs ${RUST_PART_LIB})

set(rs-cxx-src 
    ${OUT_DIR}/src/local_file_system.rs.cc
    ${OUT_DIR}/src/memory_cache.rs.cc
    ${OUT_DIR}/src/memmap.rs.cc
    ${OUT_DIR}/src/azure_trace.rs.cc
)
if (FEAT_SQLITE)
    list(APPEND rs-cxx-src ${OUT_DIR}/src/sqlite.rs.cc)
endif()

add_library(tbr-rs-bundle STATIC EXCLUDE_FROM_ALL ${rs-cxx-src})
target_include_directories(tbr-rs-bundle PUBLIC ${OUT_DIR}/include/)
target_link_libraries(tbr-rs-bundle ${OUT_DIR}/lib/lib${CRATE_NAME}.a)

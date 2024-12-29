cmake_minimum_required(VERSION 3.12)

project(tbr)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin/${CMAKE_BUILD_TYPE})

set(CMAKE_CXX_STANDARD 20)

include(FetchContent)


FetchContent_Declare(
  toml11
  GIT_REPOSITORY https://github.com/ToruNiina/toml11.git
  GIT_TAG        v4.2.0
)

# use lld linker if using clnag toolchain
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(STATUS "using Clang toolchain: ${CMAKE_CXX_COMPILER_ID}")
  add_link_options(-fuse-ld=lld)
  # add_link_options(-stdlib=libc++ -lc++abi)
endif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
set(CMAKE_POSITION_INDEPENDENT_CODE ON) # enable -fPIC

# enable link time optimization for cross language optimization
option(ENABLE_LTO "Enable cross language linking time optimization" ON)
if(ENABLE_LTO)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    include(CheckIPOSupported)
    check_ipo_supported(RESULT supported OUTPUT error)
    if(supported)
      message(STATUS "IPO / LTO enabled")
      set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
      add_link_options(-fuse-ld=lld)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto")
      set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
      set(RUST_FLAGS "-Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=lld")
      set(ENV{RUSTFLAGS} "${RUST_FLAGS}")
    else(supported)
      message(STATUS "IPO / LTO not supported: <${error}>")
    endif(supported)
  else(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "IPO / LTO requires Clang compiler")
  endif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
endif(ENABLE_LTO)

# sys-lib: boost
# Set policy CMP0167 to OLD to use FindBoost module
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()
find_package(Boost REQUIRED COMPONENTS program_options)

# sys-lib: msgpack-cxx
set(MSGPACK_CXX17 ON)
find_package(msgpack-cxx REQUIRED NAMES msgpack msgpack-cxx)

# sys-lib: threads
find_package(Threads REQUIRED)

# sys-lib: leveldb
find_package(leveldb REQUIRED)

# sys-lib: glog
find_package(glog REQUIRED)

# sys-lib: fmt
find_package(fmt REQUIRED)

# lib: toml
# add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/lib/toml)
FetchContent_MakeAvailable(toml11)

# lib: thread_pool
set(THREAD_POOL_HEADER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lib/thread_pool/include)

# lib: tbr-rs
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/lib/rust_part)

option(USE_BUNDLE_OPT "Use pre-build blob-store lib" ON)
if(USE_BUNDLE_OPT)
  set(rust-part-lib tbr-rs-bundle)
  message(STATUS "Using pre-build rust lib")
else()
  set(rust-part-lib tbr-rs)
  message(STATUS "Using native build rust lib")
endif()

# lib: ec_jerasure
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/lib/ec)

# objs: ec
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/ec)

# config macros
set (config_macros)
option(OPT_ENABLE_TRAFFIC_CONTROL "Enable traffic control" ON)
if(OPT_ENABLE_TRAFFIC_CONTROL)
  list(APPEND config_macros "-DM_CFG_ENABLE_TRAFFIC_CONTROL")
endif()


add_library(tbr
  "./core/coord_core.cc"
  "./core/worker_core.cc"
  "./core/coord_prof.cc"
  "./protocol/Command.cc"
  "./protocol/BlockCommand.cc"
  "./task/BlockTasks.cc"
  "./task/Tasks.cc"
  "./task/task_util.cc"
  "./trace/merge.cc"
  "./trace/azure_trace.cc"
)
target_link_libraries(tbr PUBLIC ${rust-part-lib} ec toml11)
target_link_libraries(tbr PRIVATE hiredis leveldb::leveldb Threads::Threads msgpack-cxx)
target_link_libraries(tbr PUBLIC fmt::fmt glog::glog)
target_include_directories(tbr PUBLIC ./common ./protocol ./meta ./task ./trace ./core)
target_include_directories(tbr PUBLIC ${THREAD_POOL_HEADER_DIR})
target_compile_definitions(tbr PUBLIC ${config_macros})

add_executable(coordinator coordinator.cc)
target_link_libraries(coordinator tbr)

add_executable(data_worker data_worker.cc)
target_link_libraries(data_worker tbr Boost::program_options)

# Test
find_package(GTest)
if(GTest_FOUND)
    message(STATUS "GTest enabled")
    # add_executable (worker_test worker_test.cc)
    # target_link_libraries(worker_test tbr GTest::GTest GTest::Main)

    # add_executable (blockWorker_test blockWorker_test.cc)
    # target_link_libraries(blockWorker_test tbr GTest::GTest GTest::Main)
endif()

#
# CMakeLists.txt  cmake for mercury-runner
# 14-Feb-2017  chuck@ece.cmu.edu
#

#
# specify the location of mercury with CMAKE_INSTALL_PATH or
# CMAKE_PREFIX_PATH...
#
cmake_minimum_required(VERSION 3.1)

project (mercury-runner C CXX)

set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (MPI "OFF" CACHE BOOL "Enable MPI-related flags")

message (STATUS "MPI Support: ${MPI}")

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
# XXX: cmake 3.1 and newer define a Threads::Threads imported target
# that we should switch too when we are ready to require 3.1 or better.
# (3.1 was released late 2014)
find_package (Threads REQUIRED)
find_package (mercury CONFIG REQUIRED)
if (MPI)
    find_package (MPI MODULE REQUIRED)
endif ()

add_executable (mercury-runner mercury-runner.cc)
if (MERCURY_INCLUDE_DIR)
    # XXX: Oct 2017 backward compat, newer mercury doesn't do this anymore...
    target_include_directories (mercury-runner PUBLIC ${MERCURY_INCLUDE_DIR})
endif ()
target_link_libraries (mercury-runner mercury Threads::Threads)

#
# MPI handling
#
if (MPI)
  target_compile_options (mercury-runner PRIVATE -DMPI_RUNNER)
  foreach (lcv ${MPI_CXX_COMPILE_FLAGS})
      if (NOT ${lcv} STREQUAL "")
          target_compile_options (mercury-runner PUBLIC
                                  $<BUILD_INTERFACE:${lcv}>)
      endif ()
  endforeach ()

  foreach (lcv ${MPI_CXX_INCLUDE_PATH})
      target_include_directories (mercury-runner PUBLIC
                                  $<BUILD_INTERFACE:${lcv}>)
  endforeach ()

  foreach (lcv ${MPI_CXX_LIBRARIES})
      target_link_libraries(mercury-runner $<BUILD_INTERFACE:${lcv}>)
  endforeach ()

  set_property (TARGET mercury-runner APPEND PROPERTY LINK_FLAGS
                ${MPI_CXX_LINK_FLAGS})
  get_property (link_flags TARGET mercury-runner PROPERTY LINK_FLAGS)
endif ()

#
# "make install" rule
#
install (TARGETS mercury-runner RUNTIME DESTINATION bin)

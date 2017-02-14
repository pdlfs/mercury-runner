#
# CMakeLists.txt  cmake for mercury-runner
# 14-Feb-2017  chuck@ece.cmu.edu
#

#
# specify the location of mercury with CMAKE_INSTALL_PATH or
# CMAKE_PREFIX_PATH...
#
cmake_minimum_required(VERSION 3.1)

project (mercury-runner CXX)

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
# XXX: cmake 3.1 and newer define a Threads::Threads imported target
# that we should switch too when we are ready to require 3.1 or better.
# (3.1 was released late 2014)
find_package (Threads REQUIRED)

find_package (mercury CONFIG REQUIRED)

add_executable (mercury-runner mercury-runner.cc)
target_include_directories (mercury-runner PUBLIC ${MERCURY_INCLUDE_DIR})
target_link_libraries (mercury-runner mercury Threads::Threads)

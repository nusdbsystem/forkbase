#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(ustore)

LIST(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)
include(cmake/Utils.cmake)

# --- [ Options
OPTION(USE_SHA256 "Choose SHA-256 hash for calculating versions" ON)
OPTION(USE_LEVELDB "Use LevelDB as chunk storage" OFF)
OPTION(TEST_NODEBUILDER "Test node builder" ON)
OPTION(ENABLE_DEBUG "Enable debug mode" OFF)
OPTION(MOCK_TEST "Use MockWorker" OFF)
OPTION(USE_RDMA "Use RDMA" OFF)

if(TEST_NODEBUILDER)
  message(STATUS "Test Node Builder with Specific Rolling Hasher")
  add_definitions(-DTEST_NODEBUILDER)
endif()

if(USE_SHA256)
  message(STATUS "Use SHA-256 hash")
  add_definitions(-DUSE_SHA256)
endif()

if(ENABLE_DEBUG)
  message(STATUS "Use debug mode")
  add_definitions(-DDEBUG)
endif()
if(MOCK_TEST)
  message(STATUS "Use MockWorker")
  add_definitions(-DMOCK_TEST)
endif()
if(USE_RDMA)
  message(STATUS "Use RDMA")
  add_definitions(-DUSE_RDMA)
endif()

# ---[ Dependencies
include(cmake/Dependencies.cmake)

enable_testing()
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fPIC -pthread -Wreturn-type")

if(ENABLE_DEBUG)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()
# SET(PROTOBUF_PROTOC_EXECUTABLE "protoc")

MESSAGE(STATUS "${PROTOBUF_PROTOC_EXECUTABLE}")
LIST(APPEND USTORE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")
INCLUDE_DIRECTORIES(${USTORE_INCLUDE_DIR})
MESSAGE(STATUS "include: ${USTORE_INCLUDE_DIR}")

SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

# copy the conf directory
file(COPY conf DESTINATION ${PROJECT_BINARY_DIR}) 

ADD_SUBDIRECTORY(test)
ADD_SUBDIRECTORY(src)

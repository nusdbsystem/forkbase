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

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/include)

ADD_LIBRARY(gtest STATIC EXCLUDE_FROM_ALL "gtest/gtest.h" "gtest/gtest-all.cc")
AUX_SOURCE_DIRECTORY(ustore ustore_test_source)

ADD_EXECUTABLE(test_ustore "gtest/gtest_main.cc" ${ustore_test_source})
ADD_DEPENDENCIES(test_ustore copy_protobuf)
ADD_DEPENDENCIES(test_ustore ustore)
TARGET_LINK_LIBRARIES(test_ustore gtest ustore)
SET_TARGET_PROPERTIES(test_ustore PROPERTIES LINK_FLAGS "${LINK_FLAGS}")

ADD_TEST(
	NAME test
  COMMAND ${CMAKE_BINARY_DIR}/bin/test_ustore )
	
ADD_EXECUTABLE(bench_bin "benchmark/bench_main.cc")
ADD_DEPENDENCIES(bench_bin copy_protobuf)
ADD_DEPENDENCIES(bench_bin ustore)
TARGET_LINK_LIBRARIES(bench_bin ustore)
SET_TARGET_PROPERTIES(bench_bin PROPERTIES LINK_FLAGS "${LINK_FLAGS}")

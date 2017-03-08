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

AUX_SOURCE_DIRECTORY(. sources)

LIST(APPEND ustore_srcs ${sources})

ADD_LIBRARY(ustore_objs OBJECT ${ustore_srcs})

ADD_LIBRARY(ustore SHARED $<TARGET_OBJECTS:ustore_objs>)
TARGET_LINK_LIBRARIES(ustore ${USTORE_LINKER_LIBS})
SET_TARGET_PROPERTIES(ustore PROPERTIES LINK_FLAGS "")

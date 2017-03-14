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

AUX_SOURCE_DIRECTORY(chunk chunk_source)
LIST(APPEND ustore_srcs ${chunk_source})

AUX_SOURCE_DIRECTORY(hash hash_source)
LIST(APPEND ustore_srcs ${hash_source})

AUX_SOURCE_DIRECTORY(node node_source)
LIST(APPEND ustore_srcs ${node_source})

AUX_SOURCE_DIRECTORY(store store_source)
LIST(APPEND ustore_srcs ${store_source})

AUX_SOURCE_DIRECTORY(types types_source)
LIST(APPEND ustore_srcs ${types_source})

AUX_SOURCE_DIRECTORY(utils utils_source)
LIST(APPEND ustore_srcs ${utils_source})

ADD_LIBRARY(ustore_objs OBJECT ${ustore_srcs})

ADD_LIBRARY(ustore SHARED $<TARGET_OBJECTS:ustore_objs>)
TARGET_LINK_LIBRARIES(ustore ${USTORE_LINKER_LIBS})
SET_TARGET_PROPERTIES(ustore PROPERTIES LINK_FLAGS "")

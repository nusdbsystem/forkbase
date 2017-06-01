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

#AUX_SOURCE_DIRECTORY(. sources)
#LIST(APPEND ustore_srcs ${sources})

FILE(GLOB proto_files proto/*.proto)
protobuf_generate_cpp(proto_srcs proto_hdrs ${proto_files})
LIST(APPEND ustore_srcs ${proto_srcs} ${proto_hdrs})
# MESSAGE(STATUS ${ustore_srcs})
# MESSAGE(STATUS ${CMAKE_BINARY_DIR})

ADD_CUSTOM_TARGET(
  copy_protobuf ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_BINARY_DIR}/include/proto"
  DEPENDS ${proto_hdrs}
  COMMENT "Copying Protobuf headers"
)
FOREACH(fil ${proto_hdrs})
  GET_FILENAME_COMPONENT(filename ${fil} NAME)
  ADD_CUSTOM_COMMAND(
    TARGET copy_protobuf POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${fil}
      "${CMAKE_BINARY_DIR}/include/proto/${filename}"
    DEPENDS ${proto_files}
  )
ENDFOREACH()

AUX_SOURCE_DIRECTORY(benchmark benchmark_source)
LIST(APPEND ustore_srcs ${benchmark_source})

AUX_SOURCE_DIRECTORY(chunk chunk_source)
LIST(APPEND ustore_srcs ${chunk_source})

AUX_SOURCE_DIRECTORY(cluster cluster_source)
LIST(APPEND ustore_srcs ${cluster_source})

AUX_SOURCE_DIRECTORY(hash hash_source)
LIST(APPEND ustore_srcs ${hash_source})

AUX_SOURCE_DIRECTORY(net net_source)
LIST(APPEND ustore_srcs ${net_source})

AUX_SOURCE_DIRECTORY(node node_source)
LIST(APPEND ustore_srcs ${node_source})

AUX_SOURCE_DIRECTORY(recovery recovery_source)
LIST(APPEND ustore_srcs ${recovery_source})

AUX_SOURCE_DIRECTORY(spec spec_source)
LIST(APPEND ustore_srcs ${spec_source})

AUX_SOURCE_DIRECTORY(store store_source)
LIST(APPEND ustore_srcs ${store_source})

AUX_SOURCE_DIRECTORY(types types_source)
LIST(APPEND ustore_srcs ${types_source})

AUX_SOURCE_DIRECTORY(types/client types_client_source)
LIST(APPEND ustore_srcs ${types_client_source})

AUX_SOURCE_DIRECTORY(types/server types_server_source)
LIST(APPEND ustore_srcs ${types_server_source})

AUX_SOURCE_DIRECTORY(utils utils_source)
LIST(APPEND ustore_srcs ${utils_source})

AUX_SOURCE_DIRECTORY(worker worker_source)
LIST(APPEND ustore_srcs ${worker_source})

AUX_SOURCE_DIRECTORY(cli cli_source)
LIST(APPEND ustore_srcs ${cli_source})

AUX_SOURCE_DIRECTORY(http http_source)
LIST(APPEND ustore_srcs ${http_source})

ADD_LIBRARY(ustore_objs OBJECT ${ustore_srcs})
ADD_DEPENDENCIES(ustore_objs copy_protobuf)

if(USE_RDMA)
  set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} ibverbs)
  set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} boost_thread)
endif()
#set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} czmq)
#set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} zmq)
#set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} rt)
#set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} gflags)
set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} boost_system)

ADD_LIBRARY(ustore SHARED $<TARGET_OBJECTS:ustore_objs>)
LIST(APPEND USTORE_LINKER_LIBS ${PROTOBUF_LIBRARY})
# MESSAGE(STATUS "LINKER LIBS ${USTORE_LINKER_LIBS}")
TARGET_LINK_LIBRARIES(ustore ${USTORE_LINKER_LIBS})
#SET_TARGET_PROPERTIES(ustore PROPERTIES LINK_FLAGS "")

ADD_EXECUTABLE(ustored "main.cc")
ADD_DEPENDENCIES(ustored ustore)
TARGET_LINK_LIBRARIES(ustored ustore)
SET_TARGET_PROPERTIES(ustored PROPERTIES LINK_FLAGS "${LINK_FLAGS}")

ADD_EXECUTABLE(ustore_cli "cli_main.cc")
ADD_DEPENDENCIES(ustore_cli ustore)
TARGET_LINK_LIBRARIES(ustore_cli ustore)
SET_TARGET_PROPERTIES(ustore_cli PROPERTIES LINK_FLAGS "${LINK_FLAGS}")

ADD_EXECUTABLE(ustore_http "http_main.cc")
ADD_DEPENDENCIES(ustore_http ustore)
TARGET_LINK_LIBRARIES(ustore_http ustore)
SET_TARGET_PROPERTIES(ustore_http PROPERTIES LINK_FLAGS "${LINK_FLAGS}")

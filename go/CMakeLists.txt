OPTION(USE_BLOB "Use BLOB as the storage type, String by default" ON)
if (USE_BLOB)
  message(STATUS "Use BLOB as the storage type in the Golang Binding")
  add_definitions(-DGO_USE_BLOB)
else()
  message(STATUS "Use String as the storage type in the Golang Binding")
endif()

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/include)
INCLUDE_DIRECTORIES(kvdb/include)

AUX_SOURCE_DIRECTORY(kvdb/src kvdb_src)

if(USE_RDMA)
  set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} ibverbs)
  set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} boost_thread)
endif()
set(USTORE_LINKER_LIBS ${USTORE_LINKER_LIBS} boost_system)
LIST(APPEND USTORE_LINKER_LIBS ${PROTOBUF_LIBRARY})

if (NOT DYNAMIC_LIB)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
endif()

ADD_LIBRARY(ustore_kv SHARED $<TARGET_OBJECTS:ustore_objs> ${kvdb_src})
TARGET_LINK_LIBRARIES(ustore_kv ${USTORE_LINKER_LIBS})

if (NOT DEFINED ENV{GOPATH}) 
  MESSAGE(FATAL_ERROR "Not defined environment variable: GOPATH.
    Please check the installation of Golang")
endif()

SET(USTORE_GOPATH $ENV{GOPATH}/src/github.com/hyperledger/fabric/ustore)

file(REMOVE_RECURSE ${USTORE_GOPATH})
file(MAKE_DIRECTORY ${USTORE_GOPATH})
file(GLOB go_unittest_files unit_test/*.go)
file(COPY ${go_unittest_files} DESTINATION ${USTORE_GOPATH})
file(GLOB api_wrapper_srcs api_wrapper/*.i)
file(COPY ${api_wrapper_srcs} DESTINATION ${USTORE_GOPATH})

add_custom_target(
  ustore_go_wrapper ALL
  COMMAND swig -go -cgo -c++ -intgosize 64 
  -outdir ${USTORE_GOPATH}
  ${USTORE_GOPATH}/ustore.i)

add_custom_target(
  go ALL
  DEPENDS ustore_go_wrapper ustore_kv 
  COMMAND 
  ${CMAKE_COMMAND} -E env CGO_LDFLAGS='-L${PROJECT_BINARY_DIR}/lib -lustore_kv -lboost_system'
  ${CMAKE_COMMAND} -E env CGO_CPPFLAGS='-I${PROJECT_SOURCE_DIR}/include -I${PROJECT_SOURCE_DIR}/build/include -std=c++11
      -I${PROJECT_SOURCE_DIR}/go/kvdb/include'
  go install -x
  WORKING_DIRECTORY ${USTORE_GOPATH}
  COMMENT "Installing UStore Golang package"
  )

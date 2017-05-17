INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/example)
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/include)

AUX_SOURCE_DIRECTORY(${CMAKE_SOURCE_DIR}/example/ca example_ca_source)
list(REMOVE_ITEM example_ca_source "${CMAKE_SOURCE_DIR}/example/ca/main.cc")

AUX_SOURCE_DIRECTORY(ca example_ca_test)

ADD_EXECUTABLE(test_ustore_ca "../gtest/gtest_main.cc" ${example_ca_source} ${example_ca_test})

ADD_DEPENDENCIES(test_ustore_ca copy_protobuf)
ADD_DEPENDENCIES(test_ustore_ca ustore)
TARGET_LINK_LIBRARIES(test_ustore_ca gtest ustore)

SET_TARGET_PROPERTIES(test_ustore_ca PROPERTIES LINK_FLAGS "${LINK_FLAGS}")

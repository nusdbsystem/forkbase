
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/include)

AUX_SOURCE_DIRECTORY(ca example_ca_source)

MESSAGE(STATUS "example ${example_ca_source}")

ADD_EXECUTABLE(ustore_example_ca "ca/example_ca.cc" ${example_ca_source})
ADD_DEPENDENCIES(ustore_example_ca ustore)
TARGET_LINK_LIBRARIES(ustore_example_ca ustore)

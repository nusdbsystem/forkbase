
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/include)
INCLUDE_DIRECTORIES(${USTORE_INCLUDE_DIR})

AUX_SOURCE_DIRECTORY(ca example_ca_source)
# MESSAGE(STATUS "include path ${USTORE_INCLUDE_DIR}")

ADD_EXECUTABLE(ustore_example_ca "ca/main.cc" ${example_ca_source})
ADD_DEPENDENCIES(ustore_example_ca ustore)
TARGET_LINK_LIBRARIES(ustore_example_ca ustore)

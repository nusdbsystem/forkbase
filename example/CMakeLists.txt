INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/include)
INCLUDE_DIRECTORIES(${USTORE_INCLUDE_DIR})

AUX_SOURCE_DIRECTORY(ca example_ca_source)
# MESSAGE(STATUS "include path ${USTORE_INCLUDE_DIR}")

ADD_EXECUTABLE(ustore_example_ca "ca/ca_main.cc" ${example_ca_source})
ADD_DEPENDENCIES(ustore_example_ca ustore)
TARGET_LINK_LIBRARIES(ustore_example_ca ustore)

AUX_SOURCE_DIRECTORY(huawei example_huawei_source)
LIST(REMOVE_ITEM example_huawei_source "huawei/mpp.cc")
LIST(REMOVE_ITEM example_huawei_source "huawei/hbase.cc")
LIST(REMOVE_ITEM example_huawei_source "huawei/deduplication.cc")

ADD_EXECUTABLE(ustore_mpp "huawei/mpp.cc" ${example_huawei_source})
ADD_DEPENDENCIES(ustore_mpp ustore)
TARGET_LINK_LIBRARIES(ustore_mpp ustore)

ADD_EXECUTABLE(ustore_hbase "huawei/hbase.cc" ${example_huawei_source})
ADD_DEPENDENCIES(ustore_hbase ustore)
TARGET_LINK_LIBRARIES(ustore_hbase ustore)

ADD_EXECUTABLE(ustore_deduplication "huawei/deduplication.cc" ${example_huawei_source})
ADD_DEPENDENCIES(ustore_deduplication ustore)
TARGET_LINK_LIBRARIES(ustore_deduplication ustore)

AUX_SOURCE_DIRECTORY(table_op example_table_op_source)
ADD_EXECUTABLE(ustore_example_table_op "table_op/table_op_main.cc" ${example_table_op_source})
ADD_DEPENDENCIES(ustore_example_table_op ustore)
TARGET_LINK_LIBRARIES(ustore_example_table_op ustore)

AUX_SOURCE_DIRECTORY(lucene_cli lucene_cli_source)
ADD_EXECUTABLE(ustore_lucene_cli "lucene_cli/lucene_cli_main.cc" ${lucene_cli_source})
ADD_DEPENDENCIES(ustore_lucene_cli ustore)
TARGET_LINK_LIBRARIES(ustore_lucene_cli ustore)

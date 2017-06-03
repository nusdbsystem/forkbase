SET(USTORE_INCLUDE_DIRS "")
SET(USTORE_LINKER_LIBS "")
SET(USTORE_DEFINITIONS "")

FIND_PACKAGE(Protobuf REQUIRED)
if (NOT GEN_DYNAMIC)
  SET(Boost_USE_STATIC_LIBS ON)
endif()
FIND_PACKAGE(Boost REQUIRED COMPONENTS ${BOOST_COMPONENT})
IF(Boost_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${Boost_INCLUDE_DIRS})
    LIST(APPEND USTORE_LINKER_LIBS ${Boost_LIBRARIES})
ENDIF()

# --- [ LevelDB
IF (USE_LEVELDB)
  MESSAGE(STATUS "Use LevelDB as chunk storage")
  FIND_PACKAGE(LevelDB REQUIRED)
  IF(LEVELDB_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${LevelDB_INCLUDES})
    LIST(APPEND USTORE_LINKER_LIBS ${LevelDB_LIBRARIES})
    ADD_DEFINITIONS(-DUSE_LEVELDB)
  ENDIF() 

  FIND_PACKAGE(Snappy REQUIRED)
  IF(Snappy_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${Snappy_INCLUDE_DIR})
    LIST(APPEND USTORE_LINKER_LIBS ${Snappy_LIBRARIES})
  ENDIF()
ENDIF()

FIND_PACKAGE(GFlags REQUIRED )
IF(GFLAGS_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${GFLAGS_INCLUDE_DIR})
    LIST(APPEND USTORE_LINKER_LIBS ${GFLAGS_LIBRARIES})
ENDIF()

FIND_PACKAGE(Czmq REQUIRED )
IF(CZMQ_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${Czmq_INCLUDE_DIR})
    LIST(APPEND USTORE_LINKER_LIBS ${Czmq_LIBRARIES})
ENDIF()

INCLUDE_DIRECTORIES(SYSTEM ${USTORE_INCLUDE_DIRS})
# MESSAGE(STATUS "INCLUDE ${USTORE_INCLUDE_DIRS}")

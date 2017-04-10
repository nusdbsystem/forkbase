SET(USTORE_INCLUDE_DIRS "")
SET(USTORE_LINKER_LIBS "")
SET(USTORE_DEFINITIONS "")

FIND_PACKAGE(Protobuf REQUIRED)
FIND_PACKAGE(Boost REQUIRED COMPONENTS filesystem)
IF(Boost_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${Boost_INCLUDE_DIRS})
    LIST(APPEND USTORE_LINKER_LIBS ${Boost_LIBRARIES})
ENDIF()

# --- [ LevelDB
IF (USE_LEVELDB)
  MESSAGE(STATUS "Use LevelDB as chunk storage")
  FIND_PACKAGE(LevelDB REQUIRED)
  IF(LevelDB_FOUND)
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

FIND_PACKAGE(Glog REQUIRED )
IF(GLOG_FOUND)
    #MESSAGE(STATUS "GLOG FOUND at ${GLOG_INCLUDE_DIR}")
    #ADD_DEFINITIONS("-DUSE_GLOG")
    LIST(APPEND USTORE_INCLUDE_DIRS ${GLOG_INCLUDE_DIR})
    LIST(APPEND USTORE_LINKER_LIBS ${GLOG_LIBRARIES})
ENDIF()

FIND_PACKAGE(GFlags REQUIRED )
IF(GFlags_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${GFlags_INCLUDE_DIR})
    LIST(APPEND USTORE_LINKER_LIBS ${GFlags_LIBRARIES})
ENDIF()

FIND_PACKAGE(Czmq REQUIRED )
IF(Czmq_FOUND)
    LIST(APPEND USTORE_INCLUDE_DIRS ${CZMQ_INCLUDE_DIR})
    LIST(APPEND USTORE_LINKER_LIBS ${CZMQ_LIBRARIES})
ENDIF()



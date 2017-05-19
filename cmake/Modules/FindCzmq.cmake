
FIND_PATH(Czmq_INCLUDE_DIR NAMES czmq.h PATHS "$ENV{Czmq_ROOT_DIR}/include")
FIND_LIBRARY(Czmq_LIBRARIES NAMES czmq PATHS "$ENV{Czmq_ROOT_DIR}/lib")

INCLUDE(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Czmq DEFAULT_MSG Czmq_INCLUDE_DIR Czmq_LIBRARIES)
IF(CZMQ_FOUND)
  MESSAGE(STATUS "Found czmq at ${Czmq_INCLUDE_DIR}")
  MARK_AS_ADVANCED(Czmq_INCLUDE_DIR Czmq_LIBRARIES)

  if(EXISTS "${Czmq_INCLUDE_DIR}/czmq_library.h")
    file(STRINGS "${Czmq_INCLUDE_DIR}/czmq_library.h" __version_lines
           REGEX "#define[ \t]+CZMQ_VERSION_[^V]+[ \t]+[0-9]+")

    foreach(__line ${__version_lines})
      if(__line MATCHES "#define[ \t]+CZMQ_VERSION_MAJOR[ \t]+([0-9]+)")
        set(Czmq_VERSION_MAJOR ${CMAKE_MATCH_1})
      elseif(__line MATCHES "#define[ \t]+CZMQ_VERSION_MINOR[ \t]+([0-9]+)")
        set(Czmq_VERSION_MINOR ${CMAKE_MATCH_1})
      elseif(__line MATCHES "#define[ \t]+CZMQ_VERSION_PATCH[ \t]+([0-9]+)")
        set(Czmq_VERSION_PATCH ${CMAKE_MATCH_1})
      endif()
    endforeach()

    set(Czmq_VERSION 
      "${Czmq_VERSION_MAJOR}.${Czmq_VERSION_MINOR}.${Czmq_VERSION_PATCH}")
    if(NOT Czmq_VERSION STREQUAL "")
      message(STATUS "Czmq Version: ${Czmq_VERSION}")
    endif()

    ustore_clear_vars(__line __version_lines)
  endif()
ELSE()
    MESSAGE(FATAL_ERROR "Czmq NOT FOUND")
ENDIF()

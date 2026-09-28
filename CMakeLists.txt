cmake_minimum_required(VERSION 3.5...3.29)

set(TARGET_NAME databricks)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)

include_directories(src/include)
add_subdirectory(src)

# Object libraries compile their own sources, so they need the third-party include
# paths. Linking them PRIVATE does not replace the LINK_ONLY link on the extension.
target_link_libraries(
  databricks_ext PRIVATE CURL::libcurl OpenSSL::SSL OpenSSL::Crypto nlohmann_json::nlohmann_json
                         "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")

# clang-tidy invokes Clang, which rejects this GCC flag.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND NOT CLANG_TIDY)
  foreach(databricks_target databricks_ext databricks_ext_storage)
    target_compile_options(${databricks_target} PRIVATE -fno-gnu-unique)
  endforeach()
endif()

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} " " ${ALL_OBJECT_FILES})

set(DATABRICKS_LINK_LIBS
    CURL::libcurl
    OpenSSL::SSL
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
    "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
if(WIN32)
  list(APPEND DATABRICKS_LINK_LIBS ws2_32 crypt32)
endif()

function(link_libraries_to_extension target_name)
  target_link_libraries(${target_name} ${DATABRICKS_LINK_LIBS})
  get_target_property(target_type ${target_name} TYPE)
  if(target_type STREQUAL "STATIC_LIBRARY")
    get_target_property(interface_link_libraries ${target_name} INTERFACE_LINK_LIBRARIES)
    if(interface_link_libraries STREQUAL "interface_link_libraries-NOTFOUND")
      set(interface_link_libraries "")
    endif()
    foreach(link_dep IN LISTS DATABRICKS_LINK_LIBS)
      list(REMOVE_ITEM interface_link_libraries "${link_dep}")
      list(APPEND interface_link_libraries "$<LINK_ONLY:${link_dep}>")
    endforeach()
    set_property(TARGET ${target_name} PROPERTY INTERFACE_LINK_LIBRARIES "${interface_link_libraries}")
  endif()
endfunction()

link_libraries_to_extension(${EXTENSION_NAME})
link_libraries_to_extension(${LOADABLE_EXTENSION_NAME})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

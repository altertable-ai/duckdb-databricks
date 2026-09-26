add_subdirectory(storage)

add_library(
  databricks_ext OBJECT
  databricks_arrow.cpp
  databricks_auth.cpp
  databricks_config.cpp
  databricks_execute.cpp
  databricks_expression.cpp
  databricks_extension.cpp
  databricks_http.cpp
  databricks_literal.cpp
  databricks_query.cpp
  databricks_scanner.cpp
  databricks_secrets.cpp
  databricks_statement.cpp
  databricks_type_mapping_function.cpp
  databricks_types.cpp)

target_compile_features(databricks_ext PUBLIC cxx_std_17)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:databricks_ext>
    PARENT_SCOPE)

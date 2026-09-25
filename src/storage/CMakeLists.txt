add_library(
  databricks_ext_storage OBJECT
  databricks_catalog.cpp
  databricks_catalog_set.cpp
  databricks_clear_cache.cpp
  databricks_ddl.cpp
  databricks_dml.cpp
  databricks_insert.cpp
  databricks_optimizer.cpp
  databricks_schema_entry.cpp
  databricks_schema_set.cpp
  databricks_storage_extension.cpp
  databricks_table_entry.cpp
  databricks_table_set.cpp
  databricks_transaction.cpp)

target_compile_features(databricks_ext_storage PUBLIC cxx_std_17)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:databricks_ext_storage>
    PARENT_SCOPE)

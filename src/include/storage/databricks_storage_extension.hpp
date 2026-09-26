#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class DatabricksStorageExtension : public StorageExtension {
public:
	DatabricksStorageExtension();
};

} // namespace duckdb

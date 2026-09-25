#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {
class ClientContext;
class DatabricksCatalog;
struct AlterInfo;
struct CreateSchemaInfo;
struct CreateTableInfo;
struct DropInfo;

struct DatabricksDdl {
	static void Execute(ClientContext &context, DatabricksCatalog &catalog, const string &sql);
	static void CreateSchema(ClientContext &context, DatabricksCatalog &catalog, CreateSchemaInfo &info);
	static void DropSchema(ClientContext &context, DatabricksCatalog &catalog, DropInfo &info);
	static void CreateTable(ClientContext &context, DatabricksCatalog &catalog, const string &schema,
	                        CreateTableInfo &info);
	static void DropTable(ClientContext &context, DatabricksCatalog &catalog, DropInfo &info);
	static void Alter(ClientContext &context, DatabricksCatalog &catalog, AlterInfo &info);
};

} // namespace duckdb

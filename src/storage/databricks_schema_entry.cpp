#include "storage/databricks_schema_entry.hpp"

#include "databricks_ddl.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "storage/databricks_catalog.hpp"

namespace duckdb {

[[noreturn]] static void ThrowUseExecute(const string &what) {
	throw NotImplementedException("%s is not supported for Databricks databases; use databricks_execute()", what);
}

DatabricksSchemaEntry::DatabricksSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this, catalog) {
}

optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	auto &dbx_catalog = catalog.Cast<DatabricksCatalog>();
	DatabricksDdl::CreateTable(transaction.GetContext(), dbx_catalog, name, info.Base());
	if (info.Base().on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT || !tables.Remember(info.Base())) {
		tables.ClearEntries();
	}
	return LookupEntry(transaction, EntryLookupInfo(CatalogType::TABLE_ENTRY, info.Base().table));
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateFunction(CatalogTransaction, CreateFunctionInfo &) {
	ThrowUseExecute("Functions and macros");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateIndex(CatalogTransaction, CreateIndexInfo &,
                                                              TableCatalogEntry &) {
	ThrowUseExecute("Indexes");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateView(CatalogTransaction, CreateViewInfo &) {
	ThrowUseExecute("CREATE VIEW");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateSequence(CatalogTransaction, CreateSequenceInfo &) {
	ThrowUseExecute("Sequences");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateTableFunction(CatalogTransaction, CreateTableFunctionInfo &) {
	ThrowUseExecute("Table functions");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateCopyFunction(CatalogTransaction, CreateCopyFunctionInfo &) {
	ThrowUseExecute("Copy functions");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreatePragmaFunction(CatalogTransaction, CreatePragmaFunctionInfo &) {
	ThrowUseExecute("Pragma functions");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateCollation(CatalogTransaction, CreateCollationInfo &) {
	ThrowUseExecute("Collations");
}
optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateType(CatalogTransaction, CreateTypeInfo &) {
	ThrowUseExecute("Types");
}
void DatabricksSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		ThrowUseExecute("ALTER");
	}
	DatabricksDdl::Alter(transaction.GetContext(), catalog.Cast<DatabricksCatalog>(), info);
	tables.ClearEntries();
}
void DatabricksSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type != CatalogType::TABLE_ENTRY) {
		ThrowUseExecute("DROP " + CatalogTypeToString(info.type));
	}
	DatabricksDdl::DropTable(context, catalog.Cast<DatabricksCatalog>(), info);
	tables.Forget(info.name);
}

void DatabricksSchemaEntry::InvalidateTables() {
	tables.ClearEntries();
}

void DatabricksSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	tables.Scan(context, callback);
}

void DatabricksSchemaEntry::Scan(CatalogType, const std::function<void(CatalogEntry &)> &) {
	throw NotImplementedException("Scanning a Databricks schema requires a client context");
}

optional_ptr<CatalogEntry> DatabricksSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	return tables.GetEntry(transaction.GetContext(), lookup_info.GetEntryName());
}

} // namespace duckdb

#include "storage/databricks_schema_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

[[noreturn]] static void ThrowUseExecute(const string &what) {
	throw NotImplementedException("%s is not supported for Databricks databases; use databricks_execute()", what);
}

DatabricksSchemaEntry::DatabricksSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this, catalog) {
}

optional_ptr<CatalogEntry> DatabricksSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	(void)transaction;
	(void)info;
	ThrowUseExecute("CREATE TABLE");
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
void DatabricksSchemaEntry::Alter(CatalogTransaction, AlterInfo &) {
	ThrowUseExecute("ALTER TABLE");
}
void DatabricksSchemaEntry::DropEntry(ClientContext &, DropInfo &info) {
	ThrowUseExecute("DROP " + CatalogTypeToString(info.type));
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

#include "storage/databricks_schema_set.hpp"

#include "databricks_statement.hpp"
#include "databricks_utils.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_schema_entry.hpp"

namespace duckdb {

DatabricksSchemaSet::DatabricksSchemaSet(Catalog &catalog) : DatabricksCatalogSet(catalog) {
}

static bool IsHiddenSchema(const string &name) {
	return StringUtil::CIEquals(name, "information_schema");
}

void DatabricksSchemaSet::LoadEntries(ClientContext &context) {
	auto &dbx_catalog = catalog.Cast<DatabricksCatalog>();
	auto sql = "SELECT schema_name, comment FROM " + DatabricksQuoteIdentifier(dbx_catalog.GetConfig().catalog) +
	           ".information_schema.schemata";
	auto result =
	    dbx_catalog.GetSession()->Execute(context, DatabricksStatementMode::SMALL, sql, dbx_catalog.GetConfig().catalog,
	                                      dbx_catalog.GetDefaultSchema(), {});
	auto only_schema = dbx_catalog.GetAttachOptions().schema;
	string matched;
	string matched_comment;
	auto add_schema = [&](const string &name, const string &comment) {
		CreateSchemaInfo info;
		info.schema = name;
		info.internal = false;
		if (!comment.empty()) {
			info.comment = Value(comment);
		}
		CreateEntry(make_uniq<DatabricksSchemaEntry>(catalog, info));
	};
	for (idx_t row = 0; row < result.rows.size(); row++) {
		auto name_cell = dbx_catalog.GetSession()->Cell(result, row, "schema_name");
		if (!name_cell) {
			continue;
		}
		auto name = *name_cell;
		auto comment_cell = dbx_catalog.GetSession()->Cell(result, row, "comment");
		auto comment = comment_cell ? *comment_cell : string();
		if (!only_schema.empty()) {
			if (IsHiddenSchema(name) || (name != only_schema && !StringUtil::CIEquals(name, only_schema))) {
				continue;
			}
			if (matched.empty() || name == only_schema) {
				matched = name;
				matched_comment = std::move(comment);
			}
			if (name == only_schema) {
				break;
			}
			continue;
		}
		if (IsHiddenSchema(name)) {
			continue;
		}
		add_schema(name, comment);
	}
	if (only_schema.empty() || matched.empty()) {
		return;
	}
	add_schema(matched, matched_comment);
	if (matched != only_schema) {
		dbx_catalog.NoteAttachedSchema(matched);
	}
}

} // namespace duckdb

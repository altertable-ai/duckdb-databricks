#include "storage/databricks_table_set.hpp"

#include "databricks_literal.hpp"
#include "databricks_statement.hpp"
#include "databricks_types.hpp"
#include "databricks_utils.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_table_entry.hpp"

namespace duckdb {

DatabricksTableSet::DatabricksTableSet(SchemaCatalogEntry &schema, Catalog &catalog)
    : DatabricksCatalogSet(catalog), schema(schema) {
}

struct TableDefinition {
	string name;
	string table_type;
	string comment;
	vector<DatabricksColumn> columns;
};

bool DatabricksTableSet::Remember(CreateTableInfo &info) {
	if (!IsLoaded()) {
		return false;
	}
	auto copied = unique_ptr_cast<CreateInfo, CreateTableInfo>(info.Copy());
	copied->schema = schema.name;
	vector<DatabricksColumn> columns;
	for (auto &column : copied->columns.Logical()) {
		DatabricksColumn stored;
		stored.name = column.Name();
		stored.type = column.Type();
		stored.type_text = DatabricksLiteral::TypeName(column.Type());
		stored.nullable = true;
		columns.push_back(std::move(stored));
	}
	for (auto &constraint : copied->constraints) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto index = constraint->Cast<NotNullConstraint>().index.index;
		if (index < columns.size()) {
			columns[index].nullable = false;
		}
	}
	Erase(copied->table);
	CreateEntry(make_uniq<DatabricksTableEntry>(catalog, schema, *copied, std::move(columns), "TABLE", ""));
	return true;
}

void DatabricksTableSet::Forget(const string &name) {
	if (!IsLoaded()) {
		return;
	}
	Erase(name);
}

void DatabricksTableSet::LoadEntries(ClientContext &context) {
	auto &dbx_catalog = catalog.Cast<DatabricksCatalog>();
	auto catalog_name = dbx_catalog.GetConfig().catalog;
	auto qualified = DatabricksQuoteIdentifier(catalog_name);
	vector<DatabricksParameter> parameters = {{"schema", schema.name}};
	auto sql = "SELECT t.table_name, t.table_type, t.comment AS table_comment, c.column_name, c.ordinal_position, "
	           "c.full_data_type, c.is_nullable, c.column_default, c.comment AS column_comment FROM " +
	           qualified + ".information_schema.tables t LEFT JOIN " + qualified +
	           ".information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
	           "WHERE t.table_schema = :schema ORDER BY t.table_name, c.ordinal_position";
	auto rows = dbx_catalog.GetSession()->Execute(context, DatabricksStatementMode::SMALL, sql, catalog_name,
	                                              schema.name, parameters);
	vector<TableDefinition> definitions;
	unordered_map<string, idx_t> by_name;
	for (idx_t row = 0; row < rows.rows.size(); row++) {
		auto name = dbx_catalog.GetSession()->Cell(rows, row, "table_name");
		if (!name) {
			continue;
		}
		auto found = by_name.find(*name);
		if (found == by_name.end()) {
			TableDefinition definition;
			definition.name = *name;
			auto table_type = dbx_catalog.GetSession()->Cell(rows, row, "table_type");
			definition.table_type = table_type ? *table_type : "TABLE";
			auto comment = dbx_catalog.GetSession()->Cell(rows, row, "table_comment");
			if (comment) {
				definition.comment = *comment;
			}
			by_name.emplace(definition.name, definitions.size());
			definitions.push_back(std::move(definition));
			found = by_name.find(*name);
		}
		auto column_name = dbx_catalog.GetSession()->Cell(rows, row, "column_name");
		auto type_text = dbx_catalog.GetSession()->Cell(rows, row, "full_data_type");
		if (!column_name || !type_text) {
			continue;
		}
		DatabricksColumn column;
		column.name = *column_name;
		column.type_text = *type_text;
		column.type = DatabricksTypes::Parse(column.type_text);
		auto nullable = dbx_catalog.GetSession()->Cell(rows, row, "is_nullable");
		column.nullable = !nullable || !StringUtil::CIEquals(*nullable, "NO");
		auto default_sql = dbx_catalog.GetSession()->Cell(rows, row, "column_default");
		if (default_sql) {
			column.default_sql = *default_sql;
		}
		auto comment = dbx_catalog.GetSession()->Cell(rows, row, "column_comment");
		if (comment) {
			column.comment = *comment;
		}
		definitions[found->second].columns.push_back(std::move(column));
	}
	for (auto &definition : definitions) {
		CreateTableInfo info(schema, definition.name);
		if (!definition.comment.empty()) {
			info.comment = Value(definition.comment);
		}
		case_insensitive_map_t<string> seen;
		string column_collision;
		vector<DatabricksColumn> kept;
		for (auto &column : definition.columns) {
			auto previous = seen.find(column.name);
			if (previous != seen.end()) {
				if (column_collision.empty()) {
					column_collision = "\"" + previous->second + "\" and \"" + column.name + "\"";
				}
				continue;
			}
			seen.emplace(column.name, column.name);
			auto index = info.columns.LogicalColumnCount();
			ColumnDefinition column_def(column.name, column.type);
			if (!column.comment.empty()) {
				column_def.SetComment(Value(column.comment));
			}
			info.columns.AddColumn(std::move(column_def));
			if (!column.nullable) {
				info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(index)));
			}
			kept.push_back(std::move(column));
		}
		CreateEntry(make_uniq<DatabricksTableEntry>(catalog, schema, info, std::move(kept), definition.table_type,
		                                            std::move(column_collision)));
	}
}

} // namespace duckdb

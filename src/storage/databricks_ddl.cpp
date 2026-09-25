#include "databricks_ddl.hpp"

#include "databricks_literal.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_transaction.hpp"

namespace duckdb {

static string RenderDefault(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::CONSTANT) {
		return DatabricksLiteral::Render(expr.Cast<ConstantExpression>().value);
	}
	if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast_expr = expr.Cast<CastExpression>();
		if (cast_expr.child->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto value = cast_expr.child->Cast<ConstantExpression>().value;
			if (!value.IsNull()) {
				value = value.DefaultCastAs(cast_expr.cast_type);
			} else {
				value = Value(cast_expr.cast_type);
			}
			return DatabricksLiteral::Render(value);
		}
	}
	throw NotImplementedException("This DEFAULT expression is not a constant; create it with databricks_execute()");
}

static string ColumnSql(const ColumnDefinition &column, bool not_null) {
	auto sql = DatabricksQuoteIdentifier(column.Name()) + " " + DatabricksLiteral::TypeName(column.Type());
	if (not_null) {
		sql += " NOT NULL";
	}
	if (column.HasDefaultValue()) {
		sql += " DEFAULT " + RenderDefault(column.DefaultValue());
	}
	return sql;
}

static string CreateTableSql(const DatabricksCatalog &catalog, const string &schema, CreateTableInfo &info) {
	if (info.temporary) {
		throw NotImplementedException("Temporary tables are not supported for Databricks; use databricks_execute()");
	}
	if (!info.partition_keys.empty() || !info.sort_keys.empty() || !info.options.empty()) {
		throw NotImplementedException("PARTITION BY, SORTED BY and WITH options are not supported for Databricks "
		                              "tables; use databricks_execute()");
	}
	unordered_set<idx_t> not_null;
	vector<string> primary_key;
	for (auto &constraint : info.constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
			not_null.insert(constraint->Cast<NotNullConstraint>().index.index);
			break;
		case ConstraintType::UNIQUE: {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (!unique.IsPrimaryKey()) {
				throw NotImplementedException(
				    "UNIQUE constraints are not supported for Databricks tables; use databricks_execute()");
			}
			if (unique.HasIndex()) {
				primary_key.push_back(info.columns.GetColumn(unique.GetIndex()).Name());
			} else {
				primary_key = unique.GetColumnNames();
			}
			break;
		}
		case ConstraintType::CHECK:
			throw NotImplementedException(
			    "CHECK constraints are not supported for Databricks tables; use databricks_execute()");
		case ConstraintType::FOREIGN_KEY:
			throw NotImplementedException(
			    "FOREIGN KEY constraints are not supported for Databricks tables; use databricks_execute()");
		default:
			throw NotImplementedException(
			    "This constraint is not supported for Databricks tables; use databricks_execute()");
		}
	}
	for (auto &key : primary_key) {
		not_null.insert(info.columns.GetColumn(key).Logical().index);
	}
	bool any_default = false;
	vector<string> columns;
	for (auto &column : info.columns.Logical()) {
		any_default = any_default || column.HasDefaultValue();
		auto required = not_null.find(column.Logical().index) != not_null.end();
		columns.push_back(ColumnSql(column, required));
	}
	if (!primary_key.empty()) {
		vector<string> keys;
		for (auto &key : primary_key) {
			keys.push_back(DatabricksQuoteIdentifier(info.columns.GetColumn(key).Name()));
		}
		columns.push_back("CONSTRAINT " + DatabricksQuoteIdentifier(info.table + "_pk") + " PRIMARY KEY (" +
		                  StringUtil::Join(keys, ", ") + ")");
	}
	string sql = "CREATE ";
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		sql += "OR REPLACE ";
	}
	sql += "TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	}
	sql += DatabricksQualifiedName(catalog.GetConfig().catalog, schema, info.table) + " (" +
	       StringUtil::Join(columns, ", ") + ") TBLPROPERTIES ('delta.columnMapping.mode' = 'name'";
	if (any_default) {
		sql += ", 'delta.feature.allowColumnDefaults' = 'supported'";
	}
	sql += ")";
	return sql;
}

void DatabricksDdl::Execute(ClientContext &context, DatabricksCatalog &catalog, const string &sql) {
	catalog.ThrowIfReadOnly();
	catalog.GetSession()->Execute(context, DatabricksStatementMode::SMALL, sql, catalog.GetConfig().catalog,
	                              catalog.GetDefaultSchema(), {});
	catalog.ClearCache();
	DatabricksTransaction::Get(context, catalog).MarkWritten();
}

void DatabricksDdl::CreateSchema(ClientContext &context, DatabricksCatalog &catalog, CreateSchemaInfo &info) {
	string sql = "CREATE SCHEMA ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	} else if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("CREATE OR REPLACE SCHEMA is not supported; use databricks_execute()");
	}
	sql += DatabricksQuoteIdentifier(catalog.GetConfig().catalog) + "." + DatabricksQuoteIdentifier(info.schema);
	Execute(context, catalog, sql);
}

void DatabricksDdl::DropSchema(ClientContext &context, DatabricksCatalog &catalog, DropInfo &info) {
	string sql = "DROP SCHEMA ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		sql += "IF EXISTS ";
	}
	sql += DatabricksQuoteIdentifier(catalog.GetConfig().catalog) + "." + DatabricksQuoteIdentifier(info.name);
	if (info.cascade) {
		sql += " CASCADE";
	}
	Execute(context, catalog, sql);
}

void DatabricksDdl::CreateTable(ClientContext &context, DatabricksCatalog &catalog, const string &schema,
                                CreateTableInfo &info) {
	Execute(context, catalog, CreateTableSql(catalog, schema, info));
}

void DatabricksDdl::DropTable(ClientContext &context, DatabricksCatalog &catalog, DropInfo &info) {
	string sql = "DROP TABLE ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		sql += "IF EXISTS ";
	}
	sql += DatabricksQualifiedName(catalog.GetConfig().catalog, info.schema, info.name);
	Execute(context, catalog, sql);
}

static string Qualified(const DatabricksCatalog &catalog, const AlterInfo &info) {
	return DatabricksQualifiedName(catalog.GetConfig().catalog, info.schema, info.name);
}

void DatabricksDdl::Alter(ClientContext &context, DatabricksCatalog &catalog, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("ALTER %s is not supported for Databricks; use databricks_execute()",
		                              EnumUtil::ToString(info.type));
	}
	auto &table = info.Cast<AlterTableInfo>();
	auto qualified = Qualified(catalog, info);
	string sql;
	switch (table.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add = table.Cast<AddColumnInfo>();
		sql = "ALTER TABLE " + qualified + " ADD COLUMN " + ColumnSql(add.new_column, false);
		if (add.if_column_not_exists) {
			throw NotImplementedException("ADD COLUMN IF NOT EXISTS is not supported; use databricks_execute()");
		}
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &drop = table.Cast<RemoveColumnInfo>();
		sql = "ALTER TABLE " + qualified + " DROP COLUMN ";
		if (drop.if_column_exists) {
			sql += "IF EXISTS ";
		}
		sql += DatabricksQuoteIdentifier(drop.removed_column);
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename = table.Cast<RenameColumnInfo>();
		sql = "ALTER TABLE " + qualified + " RENAME COLUMN " + DatabricksQuoteIdentifier(rename.old_name) + " TO " +
		      DatabricksQuoteIdentifier(rename.new_name);
		break;
	}
	case AlterTableType::RENAME_TABLE: {
		auto &rename = table.Cast<RenameTableInfo>();
		sql = "ALTER TABLE " + qualified + " RENAME TO " + DatabricksQuoteIdentifier(rename.new_table_name);
		break;
	}
	case AlterTableType::SET_NOT_NULL: {
		auto &set = table.Cast<SetNotNullInfo>();
		sql = "ALTER TABLE " + qualified + " ALTER COLUMN " + DatabricksQuoteIdentifier(set.column_name) +
		      " SET NOT NULL";
		break;
	}
	case AlterTableType::DROP_NOT_NULL: {
		auto &drop = table.Cast<DropNotNullInfo>();
		sql = "ALTER TABLE " + qualified + " ALTER COLUMN " + DatabricksQuoteIdentifier(drop.column_name) +
		      " DROP NOT NULL";
		break;
	}
	case AlterTableType::SET_DEFAULT: {
		auto &set = table.Cast<SetDefaultInfo>();
		sql = "ALTER TABLE " + qualified + " ALTER COLUMN " + DatabricksQuoteIdentifier(set.column_name);
		if (!set.expression) {
			sql += " DROP DEFAULT";
		} else {
			sql += " SET DEFAULT " + RenderDefault(*set.expression);
		}
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change = table.Cast<ChangeColumnTypeInfo>();
		sql = "ALTER TABLE " + qualified + " ALTER COLUMN " + DatabricksQuoteIdentifier(change.column_name) + " TYPE " +
		      DatabricksLiteral::TypeName(change.target_type);
		break;
	}
	default:
		throw NotImplementedException(
		    "This ALTER TABLE is not supported for Databricks tables; use databricks_execute()");
	}
	Execute(context, catalog, sql);
}

} // namespace duckdb

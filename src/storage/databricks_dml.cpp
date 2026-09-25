#include "storage/databricks_dml.hpp"

#include "databricks_expression.hpp"
#include "databricks_scanner.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_table_entry.hpp"
#include "storage/databricks_transaction.hpp"

namespace duckdb {

DatabricksDml::DatabricksDml(PhysicalPlan &physical_plan, LogicalOperator &op, string catalog_name_p, string schema_p,
                             string sql_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1),
      catalog_name(std::move(catalog_name_p)), schema_name(std::move(schema_p)), sql(std::move(sql_p)) {
}

[[noreturn]] static void RejectDml(const string &what) {
	throw NotImplementedException("%s is not supported for Databricks tables; use databricks_execute() with MERGE",
	                              what);
}

struct DmlTarget {
	LogicalGet *get = nullptr;
	vector<reference<Expression>> filters;
};

static void CollectTarget(LogicalOperator &op, DmlTarget &target) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		if (target.get) {
			RejectDml("Joins");
		}
		target.get = &op.Cast<LogicalGet>();
		return;
	case LogicalOperatorType::LOGICAL_FILTER:
		for (auto &expr : op.expressions) {
			target.filters.push_back(*expr);
		}
		if (op.children.size() != 1) {
			RejectDml("Joins");
		}
		CollectTarget(*op.children[0], target);
		return;
	default:
		RejectDml("Joins");
	}
}

static string WhereSql(LogicalGet &get, const vector<DatabricksColumn> &columns,
                       const vector<reference<Expression>> &filters) {
	vector<string> predicates;
	if (!get.table_filters.filters.empty()) {
		vector<column_t> column_ids;
		for (auto &column_index : get.GetColumnIds()) {
			column_ids.push_back(column_index.GetPrimaryIndex());
		}
		try {
			auto pushed = DatabricksExpressions::TransformFilters(column_ids, &get.table_filters, columns);
			if (!pushed.empty()) {
				predicates.push_back(pushed);
			}
		} catch (const NotImplementedException &) {
			RejectDml("This WHERE clause");
		}
	}
	for (auto &filter : filters) {
		predicates.push_back(DatabricksExpressions::TranslateDml(get, filter.get()));
	}
	if (get.bind_data) {
		auto &bind_data = get.bind_data->Cast<DatabricksScanBindData>();
		if (!bind_data.extra_filter.empty()) {
			predicates.push_back(bind_data.extra_filter);
		}
	}
	if (predicates.empty()) {
		return string();
	}
	return " WHERE " + StringUtil::Join(predicates, " AND ");
}

static bool QueryStartsWith(ClientContext &context, const string &keyword) {
	auto query = StringUtil::Lower(context.GetCurrentQuery());
	StringUtil::Trim(query);
	return StringUtil::StartsWith(query, keyword);
}

static string QualifiedTable(const DatabricksTableEntry &table) {
	auto &catalog = table.catalog.Cast<DatabricksCatalog>();
	return DatabricksQualifiedName(catalog.GetConfig().catalog, table.schema.name, table.name);
}

string DatabricksDml::DeleteSql(ClientContext &context, LogicalDelete &op) {
	if (op.return_chunk) {
		RejectDml("RETURNING");
	}
	if (op.children.size() != 1) {
		RejectDml("DELETE");
	}
	DmlTarget target;
	try {
		CollectTarget(*op.children[0], target);
	} catch (const NotImplementedException &) {
		if (QueryStartsWith(context, "delete") &&
		    StringUtil::Contains(StringUtil::Lower(context.GetCurrentQuery()), " using ")) {
			RejectDml("USING");
		}
		throw;
	}
	if (!target.get || !target.get->GetTable()) {
		RejectDml("DELETE");
	}
	auto &table = target.get->GetTable()->Cast<DatabricksTableEntry>();
	if (QueryStartsWith(context, "truncate") && target.filters.empty() && target.get->table_filters.filters.empty()) {
		return "TRUNCATE TABLE " + QualifiedTable(table);
	}
	return "DELETE FROM " + QualifiedTable(table) + WhereSql(*target.get, table.GetColumns(), target.filters);
}

string DatabricksDml::UpdateSql(ClientContext &context, LogicalUpdate &op) {
	if (op.return_chunk) {
		RejectDml("RETURNING");
	}
	if (op.children.size() != 1 || op.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		RejectDml("UPDATE");
	}
	auto &projection = op.children[0]->Cast<LogicalProjection>();
	if (projection.children.size() != 1) {
		RejectDml("UPDATE");
	}
	DmlTarget target;
	try {
		CollectTarget(*projection.children[0], target);
	} catch (const NotImplementedException &) {
		if (QueryStartsWith(context, "update") &&
		    StringUtil::Contains(StringUtil::Lower(context.GetCurrentQuery()), " from ")) {
			RejectDml("UPDATE FROM");
		}
		throw;
	}
	if (!target.get || !target.get->GetTable()) {
		RejectDml("UPDATE");
	}
	if (op.columns.size() != op.expressions.size() || op.columns.empty()) {
		RejectDml("UPDATE");
	}
	auto &table = target.get->GetTable()->Cast<DatabricksTableEntry>();
	vector<string> assignments;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		auto &expr = *op.expressions[i];
		const Expression *value = &expr;
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &column_ref = expr.Cast<BoundColumnRefExpression>();
			if (column_ref.binding.table_index != projection.table_index ||
			    column_ref.binding.column_index >= projection.expressions.size()) {
				RejectDml("UPDATE");
			}
			value = projection.expressions[column_ref.binding.column_index].get();
		} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &reference = expr.Cast<BoundReferenceExpression>();
			if (reference.index >= projection.expressions.size()) {
				RejectDml("UPDATE");
			}
			value = projection.expressions[reference.index].get();
		}
		string rendered = "DEFAULT";
		if (value->GetExpressionClass() != ExpressionClass::BOUND_DEFAULT) {
			rendered = DatabricksExpressions::TranslateDml(*target.get, *value);
		}
		auto &column = table.TableCatalogEntry::GetColumns().GetColumn(op.columns[i]);
		assignments.push_back(DatabricksQuoteIdentifier(column.Name()) + " = " + rendered);
	}
	return "UPDATE " + QualifiedTable(table) + " SET " + StringUtil::Join(assignments, ", ") +
	       WhereSql(*target.get, table.GetColumns(), target.filters);
}

SourceResultType DatabricksDml::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	(void)input;
	auto &catalog = DatabricksCatalog::GetAttachedDatabase(context.client, catalog_name, "DML");
	auto result = catalog.GetSession()->Execute(context.client, DatabricksStatementMode::SMALL, sql,
	                                            catalog.GetConfig().catalog, schema_name, {});
	DatabricksTransaction::Get(context.client, catalog).MarkWritten();
	idx_t affected = 0;
	if (result.num_affected_rows.IsValid()) {
		affected = result.num_affected_rows.GetIndex();
	}
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(affected)));
	return SourceResultType::FINISHED;
}

string DatabricksDml::GetName() const {
	return "DATABRICKS_DML";
}

InsertionOrderPreservingMap<string> DatabricksDml::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["SQL"] = sql;
	return result;
}

} // namespace duckdb

#include "storage/databricks_optimizer.hpp"

#include "databricks_expression.hpp"
#include "databricks_scanner.hpp"
#include "databricks_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

static optional_ptr<LogicalGet> FindDatabricksScan(LogicalOperator &op) {
	reference<LogicalOperator> current = op;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		current = *current.get().children[0];
	}
	if (current.get().type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = current.get().Cast<LogicalGet>();
	if (get.function.name != "databricks_scan" || !get.bind_data) {
		return nullptr;
	}
	return &get;
}

static bool AllFiltersPushed(LogicalGet &get, const DatabricksScanBindData &bind_data) {
	if (!bind_data.filter_pushdown) {
		return get.table_filters.filters.empty();
	}
	for (auto &entry : get.table_filters.filters) {
		auto column_id = entry.first;
		if (IsVirtualColumn(column_id) || column_id >= bind_data.columns.size() ||
		    !DatabricksExpressions::SupportsFilterPushdown(bind_data.columns[column_id])) {
			return false;
		}
		try {
			DatabricksExpressions::TransformFilter(DatabricksQuoteIdentifier(bind_data.columns[column_id].name),
			                                       *entry.second);
		} catch (const NotImplementedException &) {
			return false;
		}
	}
	return true;
}

static string TraceColumn(Expression &expr, LogicalOperator &child, LogicalGet &get,
                          const DatabricksScanBindData &bind_data) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return string();
	}
	auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	if (column_ref.depth > 0) {
		return string();
	}
	auto binding = column_ref.binding;
	reference<LogicalOperator> current = child;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = current.get().Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index || binding.column_index >= projection.expressions.size()) {
			return string();
		}
		auto &projected = *projection.expressions[binding.column_index];
		if (projected.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return string();
		}
		auto &inner = projected.Cast<BoundColumnRefExpression>();
		if (inner.depth > 0) {
			return string();
		}
		binding = inner.binding;
		current = *current.get().children[0];
	}
	if (binding.table_index != get.table_index) {
		return string();
	}
	auto &column_ids = get.GetColumnIds();
	if (binding.column_index >= column_ids.size() || column_ids[binding.column_index].IsVirtualColumn()) {
		return string();
	}
	auto column_id = column_ids[binding.column_index].GetPrimaryIndex();
	if (column_id >= bind_data.columns.size()) {
		return string();
	}
	auto &column = bind_data.columns[column_id];
	if (!DatabricksExpressions::SupportsFilterPushdown(column)) {
		return string();
	}
	return DatabricksQuoteIdentifier(column.name);
}

static string BuildOrderByClause(vector<BoundOrderByNode> &orders, LogicalOperator &child, LogicalGet &get,
                                 const DatabricksScanBindData &bind_data) {
	vector<string> keys;
	for (auto &order : orders) {
		auto column = TraceColumn(*order.expression, child, get, bind_data);
		if (column.empty()) {
			return string();
		}
		if (order.type != OrderType::ASCENDING && order.type != OrderType::DESCENDING) {
			return string();
		}
		if (order.null_order != OrderByNullType::NULLS_FIRST && order.null_order != OrderByNullType::NULLS_LAST) {
			return string();
		}
		auto descending = order.type == OrderType::DESCENDING;
		auto nulls_first = order.null_order == OrderByNullType::NULLS_FIRST;
		keys.push_back(column + (descending ? " DESC" : " ASC") + (nulls_first ? " NULLS FIRST" : " NULLS LAST"));
	}
	return " ORDER BY " + StringUtil::Join(keys, ", ");
}

static void OptimizeRecursive(unique_ptr<LogicalOperator> &op, bool order_pushdown) {
	if (order_pushdown && op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op->Cast<LogicalTopN>();
		auto get = FindDatabricksScan(*op->children[0]);
		if (get) {
			auto &bind_data = get->bind_data->CastNoConst<DatabricksScanBindData>();
			if (bind_data.limit_clause.empty() && AllFiltersPushed(*get, bind_data)) {
				auto order_by = BuildOrderByClause(top_n.orders, *op->children[0], *get, bind_data);
				if (!order_by.empty()) {
					bind_data.order_by_clause = order_by;
					bind_data.limit_clause = " LIMIT " + to_string(top_n.limit);
					if (top_n.offset > 0) {
						bind_data.limit_clause += " OFFSET " + to_string(top_n.offset);
					}
					get->function.order_preservation_type = OrderPreservationType::FIXED_ORDER;
					op = std::move(op->children[0]);
					return;
				}
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		auto get = FindDatabricksScan(*op->children[0]);
		auto constant_limit = limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE;
		auto offset_type = limit.offset_val.Type();
		auto constant_offset = offset_type == LimitNodeType::CONSTANT_VALUE || offset_type == LimitNodeType::UNSET;
		if (get && constant_limit && constant_offset) {
			auto &bind_data = get->bind_data->CastNoConst<DatabricksScanBindData>();
			if (bind_data.limit_clause.empty() && AllFiltersPushed(*get, bind_data)) {
				bind_data.limit_clause = " LIMIT " + to_string(limit.limit_val.GetConstantValue());
				if (offset_type == LimitNodeType::CONSTANT_VALUE && limit.offset_val.GetConstantValue() > 0) {
					bind_data.limit_clause += " OFFSET " + to_string(limit.offset_val.GetConstantValue());
				}
				op = std::move(op->children[0]);
				return;
			}
		}
	}
	for (auto &child : op->children) {
		OptimizeRecursive(child, order_pushdown);
	}
}

void DatabricksOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto order_pushdown = DatabricksSettingBool(input.context, "dbx_order_pushdown", true);
	OptimizeRecursive(plan, order_pushdown);
}

} // namespace duckdb

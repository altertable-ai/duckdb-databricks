#include "databricks_expression.hpp"

#include "databricks_literal.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

namespace duckdb {

bool DatabricksExpressions::SupportsFilterPushdown(const DatabricksColumn &column) {
	if (column.type.IsJSONType()) {
		return false;
	}
	if (StringUtil::Contains(StringUtil::Upper(column.type_text), "COLLATE")) {
		return false;
	}
	switch (column.type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		// FLOAT/DOUBLE stay in DuckDB so NaN is not compared in the warehouse.
		return false;
	}
}

static string TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Databricks filter pushdown: unsupported comparison %s",
		                              EnumUtil::ToString(type));
	}
}

string DatabricksExpressions::TransformConstant(const Value &value) {
	if (value.IsNull()) {
		throw NotImplementedException("Databricks filter pushdown: NULL constant");
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return BooleanValue::Get(value) ? "true" : "false";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
		return value.ToString();
	case LogicalTypeId::DECIMAL:
		return value.ToString() + "BD";
	case LogicalTypeId::VARCHAR:
		return DatabricksQuoteString(StringValue::Get(value));
	case LogicalTypeId::DATE: {
		auto date = DateValue::Get(value);
		if (!Date::IsFinite(date)) {
			throw InvalidInputException("Databricks date is out of range");
		}
		int32_t year, month, day;
		Date::Convert(date, year, month, day);
		if (year < 1 || year > 9999) {
			throw InvalidInputException("Databricks date is out of range");
		}
		return "DATE" + DatabricksQuoteString(Date::ToString(date));
	}
	case LogicalTypeId::TIMESTAMP: {
		auto timestamp = TimestampValue::Get(value);
		if (!Timestamp::IsFinite(timestamp)) {
			throw InvalidInputException("Databricks timestamp is out of range");
		}
		return "TIMESTAMP_NTZ" + DatabricksQuoteString(Timestamp::ToString(timestamp));
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto timestamp = TimestampTZValue::Get(value);
		if (!Timestamp::IsFinite(timestamp)) {
			throw InvalidInputException("Databricks timestamp is out of range");
		}
		return "TIMESTAMP" + DatabricksQuoteString(Timestamp::ToString(timestamp) + "+00:00");
	}
	default:
		throw NotImplementedException("Databricks filter pushdown: unsupported constant type %s",
		                              value.type().ToString());
	}
}

//! prefix% with no other wildcard. The prefix itself may be empty only when the pattern is a single %.
static bool IsPrefixLike(const string &pattern, string &prefix) {
	if (pattern.empty() || pattern.back() != '%') {
		return false;
	}
	prefix = pattern.substr(0, pattern.size() - 1);
	return prefix.find('%') == string::npos && prefix.find('_') == string::npos && !prefix.empty();
}

static bool IsColumnRef(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_REF;
}

static const Value *ConstantValue(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return nullptr;
	}
	return &expr.Cast<BoundConstantExpression>().value;
}

string DatabricksExpressions::TransformFilter(const string &column, const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::IS_NULL:
		return column + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column + " IS NOT NULL";
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		if (constant_filter.constant.IsNull()) {
			return "1 = 0";
		}
		return column + " " + TransformComparison(constant_filter.comparison_type) + " " +
		       TransformConstant(constant_filter.constant);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		vector<string> values;
		for (auto &value : in_filter.values) {
			if (value.IsNull()) {
				continue;
			}
			values.push_back(TransformConstant(value));
		}
		if (values.empty()) {
			return "1 = 0";
		}
		return column + " IN (" + StringUtil::Join(values, ", ") + ")";
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<ConjunctionAndFilter>();
		vector<string> parts;
		for (auto &child : conjunction.child_filters) {
			auto part = TransformFilter(column, *child);
			if (!part.empty()) {
				parts.push_back(part);
			}
		}
		if (parts.empty()) {
			return string();
		}
		return "(" + StringUtil::Join(parts, " AND ") + ")";
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<ConjunctionOrFilter>();
		vector<string> parts;
		for (auto &child : conjunction.child_filters) {
			auto part = TransformFilter(column, *child);
			if (part.empty()) {
				return string();
			}
			parts.push_back(part);
		}
		return "(" + StringUtil::Join(parts, " OR ") + ")";
	}
	case TableFilterType::OPTIONAL_FILTER:
	case TableFilterType::DYNAMIC_FILTER:
	case TableFilterType::BLOOM_FILTER:
		return string();
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		auto &expr = *expression_filter.expr;
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = expr.Cast<BoundBetweenExpression>();
			auto lower = ConstantValue(*between.lower);
			auto upper = ConstantValue(*between.upper);
			if (!IsColumnRef(*between.input) || !lower || !upper) {
				throw NotImplementedException("Databricks filter pushdown: unsupported BETWEEN");
			}
			if (between.lower_inclusive && between.upper_inclusive) {
				return column + " BETWEEN " + TransformConstant(*lower) + " AND " + TransformConstant(*upper);
			}
			auto left = column + " " + (between.lower_inclusive ? ">=" : ">") + " " + TransformConstant(*lower);
			auto right = column + " " + (between.upper_inclusive ? "<=" : "<") + " " + TransformConstant(*upper);
			return "(" + left + " AND " + right + ")";
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = expr.Cast<BoundFunctionExpression>();
			if (func.function.name == "~~" && func.children.size() >= 2 && IsColumnRef(*func.children[0])) {
				auto pattern = ConstantValue(*func.children[1]);
				string prefix;
				if (pattern && !pattern->IsNull() && pattern->type().id() == LogicalTypeId::VARCHAR &&
				    IsPrefixLike(StringValue::Get(*pattern), prefix)) {
					return column + " LIKE " + DatabricksQuoteString(StringValue::Get(*pattern)) + " ESCAPE " +
					       DatabricksQuoteString("\\");
				}
			}
		}
		throw NotImplementedException("Databricks filter pushdown: unsupported expression filter %s", expr.ToString());
	}
	default:
		throw NotImplementedException("Databricks filter pushdown: unsupported filter type %s",
		                              EnumUtil::ToString(filter.filter_type));
	}
}

static string ColumnSql(const vector<DatabricksColumn> &columns, const LogicalGet &get, const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return string();
	}
	auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	if (column_ref.depth > 0 || column_ref.binding.table_index != get.table_index) {
		return string();
	}
	auto &column_ids = get.GetColumnIds();
	if (column_ref.binding.column_index >= column_ids.size() ||
	    column_ids[column_ref.binding.column_index].IsVirtualColumn()) {
		return string();
	}
	auto column_id = column_ids[column_ref.binding.column_index].GetPrimaryIndex();
	if (column_id >= columns.size()) {
		return string();
	}
	auto &alias = column_ref.GetAlias();
	if (!alias.empty() && alias != columns[column_id].name) {
		for (auto &candidate : column_ids) {
			if (candidate.IsVirtualColumn()) {
				continue;
			}
			auto candidate_id = candidate.GetPrimaryIndex();
			if (candidate_id < columns.size() && columns[candidate_id].name == alias) {
				column_id = candidate_id;
				break;
			}
		}
	}
	if (!DatabricksExpressions::SupportsFilterPushdown(columns[column_id])) {
		return string();
	}
	return DatabricksQuoteIdentifier(columns[column_id].name);
}

static bool ComparisonSql(const vector<DatabricksColumn> &columns, const LogicalGet &get, Expression &expr,
                          string &sql) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
		return false;
	}
	auto &comparison = expr.Cast<BoundComparisonExpression>();
	const Expression *column_expr = nullptr;
	const Expression *constant_expr = nullptr;
	auto type = comparison.GetExpressionType();
	if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = comparison.left.get();
		constant_expr = comparison.right.get();
	} else if (comparison.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	           comparison.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = comparison.right.get();
		constant_expr = comparison.left.get();
		type = FlipComparisonExpression(type);
	} else {
		return false;
	}
	auto column = ColumnSql(columns, get, *column_expr);
	if (column.empty()) {
		return false;
	}
	auto &constant = constant_expr->Cast<BoundConstantExpression>().value;
	if (constant.IsNull()) {
		sql = "1 = 0";
		return true;
	}
	try {
		sql = column + " " + TransformComparison(type) + " " + DatabricksExpressions::TransformConstant(constant);
	} catch (const NotImplementedException &) {
		return false;
	}
	return true;
}

bool DatabricksExpressions::TryTranslateExpression(const vector<DatabricksColumn> &columns, const LogicalGet &get,
                                                   Expression &expr, string &sql) {
	if (ComparisonSql(columns, get, expr, sql)) {
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto joiner = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
		if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		    expr.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			return false;
		}
		vector<string> parts;
		for (auto &child : conjunction.children) {
			string part;
			if (!TryTranslateExpression(columns, get, *child, part)) {
				return false;
			}
			parts.push_back(part);
		}
		if (parts.empty()) {
			return false;
		}
		sql = "(" + StringUtil::Join(parts, joiner) + ")";
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		if (expr.GetExpressionType() == ExpressionType::OPERATOR_NOT && op_expr.children.size() == 1) {
			string part;
			if (!TryTranslateExpression(columns, get, *op_expr.children[0], part)) {
				return false;
			}
			sql = "NOT (" + part + ")";
			return true;
		}
		if ((expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
		     expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) &&
		    op_expr.children.size() == 1) {
			auto column = ColumnSql(columns, get, *op_expr.children[0]);
			if (column.empty()) {
				return false;
			}
			sql = column + (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ? " IS NULL" : " IS NOT NULL");
			return true;
		}
		if ((expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
		     expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) &&
		    op_expr.children.size() >= 2) {
			auto column = ColumnSql(columns, get, *op_expr.children[0]);
			if (column.empty()) {
				return false;
			}
			vector<string> values;
			for (idx_t i = 1; i < op_expr.children.size(); i++) {
				auto constant = ConstantValue(*op_expr.children[i]);
				if (!constant || constant->IsNull()) {
					return false;
				}
				try {
					values.push_back(TransformConstant(*constant));
				} catch (const NotImplementedException &) {
					return false;
				}
			}
			auto keyword = expr.GetExpressionType() == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (";
			sql = column + keyword + StringUtil::Join(values, ", ") + ")";
			return true;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &between = expr.Cast<BoundBetweenExpression>();
		auto column = ColumnSql(columns, get, *between.input);
		auto lower = ConstantValue(*between.lower);
		auto upper = ConstantValue(*between.upper);
		if (column.empty() || !lower || !upper || lower->IsNull() || upper->IsNull()) {
			return false;
		}
		try {
			if (between.lower_inclusive && between.upper_inclusive) {
				sql = column + " BETWEEN " + TransformConstant(*lower) + " AND " + TransformConstant(*upper);
			} else {
				auto left_op = between.lower_inclusive ? " >= " : " > ";
				auto right_op = between.upper_inclusive ? " <= " : " < ";
				sql = "(" + column + left_op + TransformConstant(*lower) + " AND " + column + right_op +
				      TransformConstant(*upper) + ")";
			}
		} catch (const NotImplementedException &) {
			return false;
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name == "~~" && func.children.size() >= 2) {
			auto column = ColumnSql(columns, get, *func.children[0]);
			auto pattern = ConstantValue(*func.children[1]);
			string prefix;
			if (column.empty() || !pattern || pattern->IsNull() || pattern->type().id() != LogicalTypeId::VARCHAR ||
			    !IsPrefixLike(StringValue::Get(*pattern), prefix)) {
				return false;
			}
			sql = column + " LIKE " + DatabricksQuoteString(StringValue::Get(*pattern)) + " ESCAPE " +
			      DatabricksQuoteString("\\");
			return true;
		}
	}
	return false;
}

string DatabricksExpressions::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                               const vector<DatabricksColumn> &columns) {
	if (!filters || filters->filters.empty()) {
		return string();
	}
	(void)column_ids;
	vector<string> conditions;
	for (auto &entry : filters->filters) {
		auto column_id = entry.first;
		if (IsVirtualColumn(column_id)) {
			throw InternalException("Databricks filter pushdown: unexpected filter on a virtual column");
		}
		if (column_id >= columns.size()) {
			throw InternalException("Databricks filter pushdown: column id %llu is out of range", column_id);
		}
		auto condition = TransformFilter(DatabricksQuoteIdentifier(columns[column_id].name), *entry.second);
		if (!condition.empty()) {
			conditions.push_back(condition);
		}
	}
	return StringUtil::Join(conditions, " AND ");
}

[[noreturn]] static void RejectDml(const string &what) {
	throw NotImplementedException("%s is not supported for Databricks tables; use databricks_execute() with MERGE",
	                              what);
}

static string DmlColumn(const LogicalGet &get, const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return string();
	}
	auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	if (column_ref.depth > 0) {
		RejectDml("Subqueries");
	}
	if (column_ref.binding.table_index != get.table_index) {
		RejectDml("Joins");
	}
	auto &column_ids = get.GetColumnIds();
	if (column_ref.binding.column_index >= column_ids.size()) {
		throw InternalException("Databricks DML column reference is out of range");
	}
	auto &column_index = column_ids[column_ref.binding.column_index];
	if (column_index.IsVirtualColumn()) {
		RejectDml("rowid");
	}
	return DatabricksQuoteIdentifier(get.GetColumnName(column_index));
}

static string JoinArgs(const LogicalGet &get, const vector<unique_ptr<Expression>> &children) {
	vector<string> args;
	for (auto &child : children) {
		args.push_back(DatabricksExpressions::TranslateDml(get, *child));
	}
	return StringUtil::Join(args, ", ");
}

static string ReescapeLikePattern(const string &pattern, char escape) {
	string result;
	for (idx_t i = 0; i < pattern.size(); i++) {
		auto ch = pattern[i];
		if (escape != '\0' && ch == escape) {
			if (i + 1 >= pattern.size()) {
				throw InvalidInputException("LIKE pattern ends with an escape character");
			}
			ch = pattern[++i];
			if (ch == '\\' || ch == '%' || ch == '_') {
				result.push_back('\\');
			}
			result.push_back(ch);
			continue;
		}
		if (ch == '\\') {
			result += "\\\\";
		} else {
			result.push_back(ch);
		}
	}
	return result;
}

static string LikePatternSql(const LogicalGet &get, const Expression &pattern, char escape) {
	auto constant = ConstantValue(pattern);
	if (constant && !constant->IsNull() && constant->type().id() == LogicalTypeId::VARCHAR) {
		return DatabricksQuoteString(ReescapeLikePattern(StringValue::Get(*constant), escape));
	}
	auto sql = DatabricksExpressions::TranslateDml(get, pattern);
	if (escape == '\\') {
		return sql;
	}
	if (escape != '\0') {
		RejectDml("LIKE ESCAPE");
	}
	return "replace(" + sql + ", " + DatabricksQuoteString("\\") + ", " + DatabricksQuoteString("\\\\") + ")";
}

static char LikeEscape(const LogicalGet &get, const Expression &expr) {
	(void)get;
	auto constant = ConstantValue(expr);
	if (!constant || constant->IsNull() || constant->type().id() != LogicalTypeId::VARCHAR) {
		RejectDml("LIKE ESCAPE");
	}
	auto text = StringValue::Get(*constant);
	if (text.size() > 1) {
		RejectDml("LIKE ESCAPE");
	}
	return text.empty() ? '\0' : text[0];
}

static string LikeSql(const LogicalGet &get, const vector<unique_ptr<Expression>> &children, const string &keyword) {
	if (children.size() < 2 || children.size() > 3) {
		RejectDml("LIKE");
	}
	auto escape = children.size() == 3 ? LikeEscape(get, *children[2]) : '\0';
	return "(" + DatabricksExpressions::TranslateDml(get, *children[0]) + " " + keyword + " " +
	       LikePatternSql(get, *children[1], escape) + " ESCAPE " + DatabricksQuoteString("\\") + ")";
}

static string FunctionSql(const LogicalGet &get, const BoundFunctionExpression &func) {
	auto &name = func.function.name;
	if (name == "+" || name == "-" || name == "*" || name == "/" || name == "%") {
		if (func.children.size() == 1 && (name == "+" || name == "-")) {
			return "(" + name + DatabricksExpressions::TranslateDml(get, *func.children[0]) + ")";
		}
		if (func.children.size() == 2) {
			return "(" + DatabricksExpressions::TranslateDml(get, *func.children[0]) + " " + name + " " +
			       DatabricksExpressions::TranslateDml(get, *func.children[1]) + ")";
		}
	}
	if (name == "//" || name == "divide") {
		if (func.children.size() != 2) {
			RejectDml("Function " + name);
		}
		return "div(" + JoinArgs(get, func.children) + ")";
	}
	if (name == "||") {
		return "concat(" + JoinArgs(get, func.children) + ")";
	}
	string databricks_name;
	if (name == "lower" || name == "lcase") {
		databricks_name = "lower";
	} else if (name == "upper" || name == "ucase") {
		databricks_name = "upper";
	} else if (name == "length" || name == "len" || name == "char_length" || name == "character_length" ||
	           name == "strlen") {
		databricks_name = "length";
	} else if (name == "substring" || name == "substr") {
		databricks_name = "substring";
	} else if (name == "prefix" || name == "starts_with") {
		databricks_name = "startswith";
	} else if (name == "suffix" || name == "ends_with") {
		databricks_name = "endswith";
	} else if (name == "ceiling") {
		databricks_name = "ceil";
	} else if (name == "trim" || name == "ltrim" || name == "rtrim" || name == "concat" || name == "replace" ||
	           name == "contains" || name == "abs" || name == "round" || name == "floor" || name == "ceil") {
		databricks_name = name;
	} else if (name == "~~" || name == "like_escape") {
		return LikeSql(get, func.children, "LIKE");
	} else if (name == "!~~" || name == "not_like_escape") {
		return LikeSql(get, func.children, "NOT LIKE");
	} else if (name == "~~*" || name == "ilike_escape") {
		return LikeSql(get, func.children, "ILIKE");
	} else if (name == "!~~*" || name == "not_ilike_escape") {
		return LikeSql(get, func.children, "NOT ILIKE");
	} else {
		RejectDml("Function " + name);
	}
	if ((name == "trim" || name == "ltrim" || name == "rtrim") && func.children.size() == 2) {
		auto lead = name == "ltrim" ? "LEADING " : name == "rtrim" ? "TRAILING " : "";
		return string("trim(") + lead + DatabricksExpressions::TranslateDml(get, *func.children[1]) + " FROM " +
		       DatabricksExpressions::TranslateDml(get, *func.children[0]) + ")";
	}
	return databricks_name + "(" + JoinArgs(get, func.children) + ")";
}

string DatabricksExpressions::TranslateDml(const LogicalGet &get, const Expression &expr) {
	auto column = DmlColumn(get, expr);
	if (!column.empty()) {
		return column;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		return DatabricksLiteral::Render(expr.Cast<BoundConstantExpression>().value);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		auto function = string(cast_expr.try_cast ? "TRY_CAST" : "CAST");
		return function + "(" + TranslateDml(get, *cast_expr.child) + " AS " +
		       DatabricksLiteral::TypeName(cast_expr.return_type) + ")";
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = expr.Cast<BoundComparisonExpression>();
		return "(" + TranslateDml(get, *comparison.left) + " " + TransformComparison(comparison.GetExpressionType()) +
		       " " + TranslateDml(get, *comparison.right) + ")";
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto joiner = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
		if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND &&
		    expr.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
			RejectDml("This boolean expression");
		}
		vector<string> parts;
		for (auto &child : conjunction.children) {
			parts.push_back(TranslateDml(get, *child));
		}
		return "(" + StringUtil::Join(parts, joiner) + ")";
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &between = expr.Cast<BoundBetweenExpression>();
		auto input = TranslateDml(get, *between.input);
		auto lower = TranslateDml(get, *between.lower);
		auto upper = TranslateDml(get, *between.upper);
		if (between.lower_inclusive && between.upper_inclusive) {
			return "(" + input + " BETWEEN " + lower + " AND " + upper + ")";
		}
		auto left_op = between.lower_inclusive ? " >= " : " > ";
		auto right_op = between.upper_inclusive ? " <= " : " < ";
		return "((" + input + left_op + lower + ") AND (" + input + right_op + upper + "))";
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CASE) {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		string sql = "CASE";
		for (auto &check : case_expr.case_checks) {
			sql += " WHEN " + TranslateDml(get, *check.when_expr) + " THEN " + TranslateDml(get, *check.then_expr);
		}
		if (case_expr.else_expr) {
			sql += " ELSE " + TranslateDml(get, *case_expr.else_expr);
		}
		sql += " END";
		return sql;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		return FunctionSql(get, expr.Cast<BoundFunctionExpression>());
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		if (expr.GetExpressionType() == ExpressionType::OPERATOR_NOT && op_expr.children.size() == 1) {
			return "NOT (" + TranslateDml(get, *op_expr.children[0]) + ")";
		}
		if ((expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ||
		     expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) &&
		    op_expr.children.size() == 1) {
			auto keyword = expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL ? " IS NULL" : " IS NOT NULL";
			return "(" + TranslateDml(get, *op_expr.children[0]) + keyword + ")";
		}
		if ((expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
		     expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) &&
		    op_expr.children.size() >= 2) {
			vector<string> values;
			for (idx_t i = 1; i < op_expr.children.size(); i++) {
				values.push_back(TranslateDml(get, *op_expr.children[i]));
			}
			auto keyword = expr.GetExpressionType() == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (";
			return "(" + TranslateDml(get, *op_expr.children[0]) + keyword + StringUtil::Join(values, ", ") + "))";
		}
		if (expr.GetExpressionType() == ExpressionType::OPERATOR_COALESCE) {
			return "coalesce(" + JoinArgs(get, op_expr.children) + ")";
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &reference = expr.Cast<BoundReferenceExpression>();
		auto &column_ids = get.GetColumnIds();
		if (!reference.GetAlias().empty()) {
			for (auto &column_index : column_ids) {
				if (!column_index.IsVirtualColumn() && get.GetColumnName(column_index) == reference.GetAlias()) {
					return DatabricksQuoteIdentifier(reference.GetAlias());
				}
			}
		}
		if (reference.index >= column_ids.size() || column_ids[reference.index].IsVirtualColumn()) {
			RejectDml("rowid");
		}
		return DatabricksQuoteIdentifier(get.GetColumnName(column_ids[reference.index]));
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		RejectDml("Subqueries");
	}
	RejectDml("This expression");
}

} // namespace duckdb

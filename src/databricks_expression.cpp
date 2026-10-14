#include "databricks_expression.hpp"

#include "databricks_literal.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
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
#include "duckdb/function/scalar/tablefilter_functions.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"

#include <functional>

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
		return DatabricksLiteral::Render(value);
	default:
		throw NotImplementedException("Databricks filter pushdown: unsupported constant type %s",
		                              value.type().ToString());
	}
}

//! prefix% with no other wildcard. The prefix itself may not be empty.
static bool IsPrefixLike(const string &pattern, string &prefix) {
	if (pattern.empty() || pattern.back() != '%') {
		return false;
	}
	prefix = pattern.substr(0, pattern.size() - 1);
	return prefix.find('%') == string::npos && prefix.find('_') == string::npos && !prefix.empty();
}

static const Value *ConstantValue(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return nullptr;
	}
	return &expr.Cast<BoundConstantExpression>().GetValue();
}

static string BetweenSql(const string &input, const string &lower, const string &upper, bool lower_inclusive,
                         bool upper_inclusive) {
	if (lower_inclusive && upper_inclusive) {
		return "(" + input + " BETWEEN " + lower + " AND " + upper + ")";
	}
	auto left_op = lower_inclusive ? " >= " : " > ";
	auto right_op = upper_inclusive ? " <= " : " < ";
	return "((" + input + left_op + lower + ") AND (" + input + right_op + upper + "))";
}

//! DuckDB LIKE has no default escape. Databricks LIKE in this extension always uses backslash.
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

static string EscapedLikeLiteral(const string &pattern, char escape) {
	return DatabricksQuoteString(ReescapeLikePattern(pattern, escape));
}

enum class TranslatePolicy { FILTER, DML };

struct Translator {
	TranslatePolicy policy;
	std::function<string(const Expression &)> resolve;

	[[noreturn]] void Fail(const string &what) const {
		if (policy == TranslatePolicy::DML) {
			throw NotImplementedException(
			    "%s is not supported for Databricks tables; use databricks_execute() with MERGE", what);
		}
		throw NotImplementedException("Databricks filter pushdown: %s", what);
	}

	string Translate(const Expression &expr) const;
};

static string JoinTranslated(const Translator &translator, const vector<unique_ptr<Expression>> &children) {
	vector<string> args;
	for (auto &child : children) {
		args.push_back(translator.Translate(*child));
	}
	return StringUtil::Join(args, ", ");
}

static char LikeEscape(const Translator &translator, const Expression &expr) {
	auto constant = ConstantValue(expr);
	if (!constant || constant->IsNull() || constant->type().id() != LogicalTypeId::VARCHAR) {
		translator.Fail("LIKE ESCAPE");
	}
	auto text = StringValue::Get(*constant);
	if (text.size() > 1) {
		translator.Fail("LIKE ESCAPE");
	}
	return text.empty() ? '\0' : text[0];
}

static string LikePatternSql(const Translator &translator, const Expression &pattern, char escape) {
	auto constant = ConstantValue(pattern);
	if (constant && !constant->IsNull() && constant->type().id() == LogicalTypeId::VARCHAR) {
		return EscapedLikeLiteral(StringValue::Get(*constant), escape);
	}
	auto sql = translator.Translate(pattern);
	if (escape == '\\') {
		return sql;
	}
	if (escape != '\0') {
		translator.Fail("LIKE ESCAPE");
	}
	return "replace(" + sql + ", " + DatabricksQuoteString("\\") + ", " + DatabricksQuoteString("\\\\") + ")";
}

static string LikeSql(const Translator &translator, const vector<unique_ptr<Expression>> &children,
                      const string &keyword) {
	if (children.size() < 2 || children.size() > 3) {
		translator.Fail("LIKE");
	}
	auto escape = children.size() == 3 ? LikeEscape(translator, *children[2]) : '\0';
	return "(" + translator.Translate(*children[0]) + " " + keyword + " " +
	       LikePatternSql(translator, *children[1], escape) + " ESCAPE " + DatabricksQuoteString("\\") + ")";
}

static string FunctionSql(const Translator &translator, const BoundFunctionExpression &func) {
	auto &name = func.Function().GetName().GetIdentifierName();
	auto &children = func.GetChildren();
	if (name == "+" || name == "-" || name == "*" || name == "/" || name == "%") {
		if (children.size() == 1 && (name == "+" || name == "-")) {
			return "(" + name + translator.Translate(*children[0]) + ")";
		}
		if (children.size() == 2) {
			return "(" + translator.Translate(*children[0]) + " " + name + " " + translator.Translate(*children[1]) +
			       ")";
		}
	}
	if (name == "//" || name == "divide") {
		if (children.size() != 2) {
			translator.Fail("Function " + name);
		}
		return "div(" + JoinTranslated(translator, children) + ")";
	}
	if (name == "||") {
		return "concat(" + JoinTranslated(translator, children) + ")";
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
	           name == "contains" || name == "abs" || name == "round" || name == "floor" || name == "ceil" ||
	           name == "isnan") {
		databricks_name = name;
	} else if (name == "~~" || name == "like_escape") {
		return LikeSql(translator, children, "LIKE");
	} else if (name == "!~~" || name == "not_like_escape") {
		return LikeSql(translator, children, "NOT LIKE");
	} else if (name == "~~*" || name == "ilike_escape") {
		return LikeSql(translator, children, "ILIKE");
	} else if (name == "!~~*" || name == "not_ilike_escape") {
		return LikeSql(translator, children, "NOT ILIKE");
	} else {
		translator.Fail("Function " + name);
	}
	if ((name == "trim" || name == "ltrim" || name == "rtrim") && children.size() == 2) {
		auto lead = name == "ltrim" ? "LEADING " : name == "rtrim" ? "TRAILING " : "";
		return string("trim(") + lead + translator.Translate(*children[1]) + " FROM " +
		       translator.Translate(*children[0]) + ")";
	}
	return databricks_name + "(" + JoinTranslated(translator, children) + ")";
}

static string TranslatePrefixLike(const Translator &translator, const BoundFunctionExpression &func) {
	auto &children = func.GetChildren();
	if (func.Function().GetName().GetIdentifierName() != "~~" || children.size() < 2) {
		translator.Fail("unsupported expression filter");
	}
	auto column = translator.resolve(*children[0]);
	auto pattern = ConstantValue(*children[1]);
	string prefix;
	if (column.empty() || !pattern || pattern->IsNull() || pattern->type().id() != LogicalTypeId::VARCHAR ||
	    !IsPrefixLike(StringValue::Get(*pattern), prefix)) {
		translator.Fail("unsupported expression filter");
	}
	return column + " LIKE " + EscapedLikeLiteral(StringValue::Get(*pattern), '\0') + " ESCAPE " +
	       DatabricksQuoteString("\\");
}

static string TranslateBetween(const Translator &translator, const BoundFunctionExpression &between) {
	auto &input_expr = BoundBetweenExpression::Input(between);
	auto &lower_expr = BoundBetweenExpression::LowerBound(between);
	auto &upper_expr = BoundBetweenExpression::UpperBound(between);
	string input;
	string lower;
	string upper;
	if (translator.policy == TranslatePolicy::FILTER) {
		input = translator.resolve(input_expr);
		auto lower_value = ConstantValue(lower_expr);
		auto upper_value = ConstantValue(upper_expr);
		if (input.empty() || !lower_value || !upper_value || lower_value->IsNull() || upper_value->IsNull()) {
			translator.Fail("unsupported BETWEEN");
		}
		lower = DatabricksExpressions::TransformConstant(*lower_value);
		upper = DatabricksExpressions::TransformConstant(*upper_value);
	} else {
		input = translator.Translate(input_expr);
		lower = translator.Translate(lower_expr);
		upper = translator.Translate(upper_expr);
	}
	return BetweenSql(input, lower, upper, BoundBetweenExpression::LowerInclusive(between),
	                  BoundBetweenExpression::UpperInclusive(between));
}

static string TranslateComparison(const Translator &translator, const BoundFunctionExpression &comparison) {
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	if (translator.policy == TranslatePolicy::DML) {
		return "(" + translator.Translate(left) + " " + TransformComparison(comparison.GetExpressionType()) + " " +
		       translator.Translate(right) + ")";
	}
	const Expression *column_expr = nullptr;
	const Expression *constant_expr = nullptr;
	auto type = comparison.GetExpressionType();
	if (right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = &left;
		constant_expr = &right;
	} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column_expr = &right;
		constant_expr = &left;
		type = FlipComparisonExpression(type);
	} else {
		translator.Fail("unsupported comparison");
	}
	auto column = translator.resolve(*column_expr);
	if (column.empty()) {
		translator.Fail("unsupported comparison");
	}
	auto &constant = constant_expr->Cast<BoundConstantExpression>().GetValue();
	if (constant.IsNull()) {
		return "1 = 0";
	}
	return column + " " + TransformComparison(type) + " " + DatabricksExpressions::TransformConstant(constant);
}

static string TranslateCast(const Translator &translator, const BoundFunctionExpression &cast_expr) {
	if (translator.policy != TranslatePolicy::DML) {
		translator.Fail("unsupported cast");
	}
	auto function = string(BoundCastExpression::IsTryCast(cast_expr) ? "TRY_CAST" : "CAST");
	return function + "(" + translator.Translate(BoundCastExpression::Child(cast_expr)) + " AS " +
	       DatabricksLiteral::TypeName(cast_expr.GetReturnType()) + ")";
}

static string TranslateOperator(const Translator &translator, const Expression &expr) {
	auto &children = expr.Cast<BoundOperatorExpression>().GetChildren();
	auto type = expr.GetExpressionType();
	if (type == ExpressionType::OPERATOR_NOT && children.size() == 1) {
		return "NOT (" + translator.Translate(*children[0]) + ")";
	}
	if ((type == ExpressionType::OPERATOR_IS_NULL || type == ExpressionType::OPERATOR_IS_NOT_NULL) &&
	    children.size() == 1) {
		auto keyword = type == ExpressionType::OPERATOR_IS_NULL ? " IS NULL" : " IS NOT NULL";
		if (translator.policy == TranslatePolicy::FILTER) {
			auto column = translator.resolve(*children[0]);
			if (column.empty()) {
				translator.Fail("unsupported expression");
			}
			return column + keyword;
		}
		return "(" + translator.Translate(*children[0]) + keyword + ")";
	}
	if ((type == ExpressionType::COMPARE_IN || type == ExpressionType::COMPARE_NOT_IN) && children.size() >= 2) {
		auto keyword = type == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (";
		if (translator.policy == TranslatePolicy::FILTER) {
			auto column = translator.resolve(*children[0]);
			if (column.empty()) {
				translator.Fail("unsupported expression");
			}
			vector<string> values;
			for (idx_t i = 1; i < children.size(); i++) {
				auto constant = ConstantValue(*children[i]);
				if (!constant || constant->IsNull()) {
					translator.Fail("unsupported IN");
				}
				values.push_back(DatabricksExpressions::TransformConstant(*constant));
			}
			return column + keyword + StringUtil::Join(values, ", ") + ")";
		}
		vector<string> values;
		for (idx_t i = 1; i < children.size(); i++) {
			values.push_back(translator.Translate(*children[i]));
		}
		return "(" + translator.Translate(*children[0]) + keyword + StringUtil::Join(values, ", ") + "))";
	}
	if (translator.policy == TranslatePolicy::DML && type == ExpressionType::OPERATOR_COALESCE) {
		return "coalesce(" + JoinTranslated(translator, children) + ")";
	}
	translator.Fail(translator.policy == TranslatePolicy::DML ? "This expression" : "unsupported expression");
}

string Translator::Translate(const Expression &expr) const {
	auto expr_class = expr.GetExpressionClass();
	if (policy == TranslatePolicy::DML &&
	    (expr_class == ExpressionClass::BOUND_COLUMN_REF || expr_class == ExpressionClass::BOUND_REF)) {
		auto column = resolve(expr);
		if (!column.empty()) {
			return column;
		}
		Fail("This expression");
	}
	switch (expr_class) {
	case ExpressionClass::BOUND_CONSTANT:
		if (policy != TranslatePolicy::DML) {
			Fail("unsupported constant");
		}
		return DatabricksLiteral::Render(expr.Cast<BoundConstantExpression>().GetValue());
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		auto type = expr.GetExpressionType();
		if (type != ExpressionType::CONJUNCTION_AND && type != ExpressionType::CONJUNCTION_OR) {
			Fail(policy == TranslatePolicy::DML ? "This boolean expression" : "unsupported conjunction");
		}
		auto joiner = type == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
		vector<string> parts;
		for (auto &child : conjunction.GetChildren()) {
			parts.push_back(Translate(*child));
		}
		if (parts.empty()) {
			Fail("unsupported conjunction");
		}
		return "(" + StringUtil::Join(parts, joiner) + ")";
	}
	case ExpressionClass::BOUND_CASE: {
		if (policy != TranslatePolicy::DML) {
			Fail("unsupported case");
		}
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		string sql = "CASE";
		for (auto &check : case_expr.CaseChecks()) {
			sql += " WHEN " + Translate(*check.when_expr) + " THEN " + Translate(*check.then_expr);
		}
		if (case_expr.ElseExpression()) {
			sql += " ELSE " + Translate(*case_expr.ElseExpression());
		}
		sql += " END";
		return sql;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (BoundComparisonExpression::IsComparison(func)) {
			return TranslateComparison(*this, func);
		}
		if (func.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
			return TranslateBetween(*this, func);
		}
		if (BoundCastExpression::IsCast(func)) {
			return TranslateCast(*this, func);
		}
		if (policy == TranslatePolicy::FILTER) {
			return TranslatePrefixLike(*this, func);
		}
		return FunctionSql(*this, func);
	}
	case ExpressionClass::BOUND_OPERATOR:
		return TranslateOperator(*this, expr);
	case ExpressionClass::BOUND_SUBQUERY:
		Fail(policy == TranslatePolicy::DML ? "Subqueries" : "unsupported expression");
	default:
		Fail(policy == TranslatePolicy::DML ? "This expression" : "unsupported expression filter " + expr.ToString());
	}
}

static string ScanColumnSql(const vector<DatabricksColumn> &columns, const LogicalGet &get, const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return string();
	}
	auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	if (column_ref.Depth() > 0 || column_ref.Binding().table_index != get.table_index) {
		return string();
	}
	auto &column_ids = get.GetColumnIds();
	if (column_ref.Binding().column_index >= column_ids.size() ||
	    column_ids[column_ref.Binding().column_index].IsVirtualColumn()) {
		return string();
	}
	auto column_id = column_ids[column_ref.Binding().column_index].GetPrimaryIndex();
	if (column_id >= columns.size()) {
		return string();
	}
	auto &alias = column_ref.GetAlias().GetIdentifierName();
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

static string DmlColumnSql(const LogicalGet &get, const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column_ref = expr.Cast<BoundColumnRefExpression>();
		if (column_ref.Depth() > 0) {
			throw NotImplementedException(
			    "Subqueries is not supported for Databricks tables; use databricks_execute() with MERGE");
		}
		if (column_ref.Binding().table_index != get.table_index) {
			throw NotImplementedException(
			    "Joins is not supported for Databricks tables; use databricks_execute() with MERGE");
		}
		auto &column_ids = get.GetColumnIds();
		if (column_ref.Binding().column_index >= column_ids.size()) {
			throw InternalException("Databricks DML column reference is out of range");
		}
		auto &column_index = column_ids[column_ref.Binding().column_index];
		if (column_index.IsVirtualColumn()) {
			throw NotImplementedException(
			    "rowid is not supported for Databricks tables; use databricks_execute() with MERGE");
		}
		return DatabricksQuoteIdentifier(get.GetColumnName(column_index));
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return string();
	}
	auto &reference = expr.Cast<BoundReferenceExpression>();
	auto &column_ids = get.GetColumnIds();
	auto &alias = reference.GetAlias().GetIdentifierName();
	if (!alias.empty()) {
		for (auto &column_index : column_ids) {
			if (!column_index.IsVirtualColumn() && get.GetColumnName(column_index).GetIdentifierName() == alias) {
				return DatabricksQuoteIdentifier(alias);
			}
		}
	}
	if (reference.Index() >= column_ids.size() || column_ids[reference.Index()].IsVirtualColumn()) {
		throw NotImplementedException(
		    "rowid is not supported for Databricks tables; use databricks_execute() with MERGE");
	}
	return DatabricksQuoteIdentifier(get.GetColumnName(column_ids[reference.Index()]));
}

static bool IsRuntimeHint(const Expression &expr) {
	return ExpressionFilter::ContainsInternalFunction(expr, TableFilterDynamicFun::Name) ||
	       ExpressionFilter::ContainsInternalFunction(expr, TableFilterBloomFilterFun::Name) ||
	       ExpressionFilter::ContainsInternalFunction(expr, TableFilterPrefixRangeFun::Name);
}

static string TransformFilterExpression(const string &column, const Expression &expr) {
	if (ExpressionFilter::IsOptionalExpression(expr) || ExpressionFilter::IsRootOptionalExpression(expr)) {
		return string();
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
		vector<string> parts;
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			auto part = TransformFilterExpression(column, *child);
			if (part.empty()) {
				if (!is_and) {
					return string();
				}
				continue;
			}
			parts.push_back(part);
		}
		if (parts.empty()) {
			return string();
		}
		return "(" + StringUtil::Join(parts, is_and ? " AND " : " OR ") + ")";
	}
	if (IsRuntimeHint(expr)) {
		return string();
	}
	Translator translator {TranslatePolicy::FILTER, [&](const Expression &child) {
		                       if (ExpressionFilter::IsSimpleFilterColumnRef(child)) {
			                       return column;
		                       }
		                       return string();
	                       }};
	return translator.Translate(expr);
}

string DatabricksExpressions::TransformFilter(const string &column, const TableFilter &filter,
                                              const LogicalType &column_type) {
	switch (filter.filter_type) {
	case TableFilterType::EXPRESSION_FILTER:
		if (ExpressionFilter::IsOptionalFilter(filter) || ExpressionFilter::IsRootOptionalFilter(filter) ||
		    ExpressionFilter::GetRootOptionalDynamicFilterData(filter)) {
			return string();
		}
		return TransformFilterExpression(column, *ExpressionFilter::GetExpressionFilter(filter, "databricks").expr);
	case TableFilterType::LEGACY_OPTIONAL_FILTER:
	case TableFilterType::LEGACY_DYNAMIC_FILTER:
	case TableFilterType::LEGACY_BLOOM_FILTER:
	case TableFilterType::LEGACY_PREFIX_RANGE_FILTER:
		return string();
	default:
		return TransformFilterExpression(column, *ExpressionFilter::FromTableFilter(filter, column_type)->expr);
	}
}

bool DatabricksExpressions::TryTranslateExpression(const vector<DatabricksColumn> &columns, const LogicalGet &get,
                                                   Expression &expr, string &sql) {
	Translator translator {TranslatePolicy::FILTER, [&](const Expression &child) {
		                       return ScanColumnSql(columns, get, child);
	                       }};
	try {
		sql = translator.Translate(expr);
		return true;
	} catch (const NotImplementedException &) {
		return false;
	}
}

string DatabricksExpressions::TranslateDml(const LogicalGet &get, const Expression &expr) {
	Translator translator {TranslatePolicy::DML, [&](const Expression &child) {
		                       return DmlColumnSql(get, child);
	                       }};
	return translator.Translate(expr);
}

string DatabricksExpressions::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                               const vector<DatabricksColumn> &columns) {
	if (!filters || !filters->HasFilters()) {
		return string();
	}
	vector<string> conditions;
	for (auto &entry : *filters) {
		auto projection_index = entry.GetIndex().GetIndex();
		if (projection_index >= column_ids.size()) {
			throw InternalException("Databricks filter pushdown: filter on unknown projection %llu", projection_index);
		}
		auto column_id = column_ids[projection_index];
		if (IsVirtualColumn(column_id)) {
			throw InternalException("Databricks filter pushdown: unexpected filter on a virtual column");
		}
		if (column_id >= columns.size()) {
			throw InternalException("Databricks filter pushdown: column id %llu is out of range", column_id);
		}
		auto &column = columns[column_id];
		auto condition = TransformFilter(DatabricksQuoteIdentifier(column.name), entry.Filter(), column.type);
		if (!condition.empty()) {
			conditions.push_back(condition);
		}
	}
	return StringUtil::Join(conditions, " AND ");
}

} // namespace duckdb

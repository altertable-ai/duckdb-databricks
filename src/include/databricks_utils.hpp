#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

inline string DatabricksQuoteIdentifier(const string &name) {
	string result = "`";
	for (auto ch : name) {
		if (ch == '`') {
			result += "``";
		} else {
			result.push_back(ch);
		}
	}
	result.push_back('`');
	return result;
}

inline string DatabricksQualifiedName(const string &catalog, const string &schema, const string &table) {
	return DatabricksQuoteIdentifier(catalog) + "." + DatabricksQuoteIdentifier(schema) + "." +
	       DatabricksQuoteIdentifier(table);
}

inline string DatabricksRedactUrl(const string &url) {
	auto query = url.find('?');
	if (query == string::npos) {
		return url;
	}
	return url.substr(0, query) + "?redacted";
}

inline idx_t DatabricksSettingIndex(ClientContext &context, const string &key, idx_t fallback) {
	Value value;
	if (!context.TryGetCurrentSetting(key, value) || value.IsNull()) {
		return fallback;
	}
	return UBigIntValue::Get(value.DefaultCastAs(LogicalType::UBIGINT));
}

inline bool DatabricksSettingBool(ClientContext &context, const string &key, bool fallback) {
	Value value;
	if (!context.TryGetCurrentSetting(key, value) || value.IsNull()) {
		return fallback;
	}
	return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
}

inline string DatabricksSettingString(ClientContext &context, const string &key, const string &fallback) {
	Value value;
	if (!context.TryGetCurrentSetting(key, value) || value.IsNull()) {
		return fallback;
	}
	return StringValue::Get(value.DefaultCastAs(LogicalType::VARCHAR));
}

} // namespace duckdb

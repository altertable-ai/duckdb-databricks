#include "databricks_literal.hpp"

#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"

#include <cmath>
#include <limits>
#include <sstream>

namespace duckdb {

static string FloatText(double value, int precision) {
	std::ostringstream out;
	out.precision(precision);
	out << value;
	return out.str();
}

string DatabricksLiteral::TypeName(const LogicalType &type) {
	if (type.IsJSONType()) {
		return "VARIANT";
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
		return "TINYINT";
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::UTINYINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::USMALLINT:
		return "INT";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::UBIGINT:
		return "DECIMAL(20,0)";
	case LogicalTypeId::HUGEINT:
		return "DECIMAL(38,0)";
	case LogicalTypeId::FLOAT:
		return "FLOAT";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE";
	case LogicalTypeId::DECIMAL:
		return StringUtil::Format("DECIMAL(%d,%d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::UUID:
		return "STRING";
	case LogicalTypeId::BLOB:
		return "BINARY";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIMESTAMP:
		return "TIMESTAMP_NTZ";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMP";
	case LogicalTypeId::INTERVAL:
		return "INTERVAL DAY TO SECOND";
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
		return "ARRAY<" + TypeName(ListType::GetChildType(type)) + ">";
	case LogicalTypeId::MAP:
		return "MAP<" + TypeName(MapType::KeyType(type)) + ", " + TypeName(MapType::ValueType(type)) + ">";
	case LogicalTypeId::STRUCT: {
		auto count = StructType::GetChildCount(type);
		vector<string> fields;
		for (idx_t i = 0; i < count; i++) {
			fields.push_back(DatabricksQuoteIdentifier(StructType::GetChildName(type, i)) + ": " +
			                 TypeName(StructType::GetChildType(type, i)));
		}
		return "STRUCT<" + StringUtil::Join(fields, ", ") + ">";
	}
	default:
		throw InvalidInputException("Value of type %s is not representable in Databricks", type.ToString());
	}
}

static string RenderFloat(const Value &value, bool as_float) {
	auto number = as_float ? static_cast<double>(FloatValue::Get(value)) : DoubleValue::Get(value);
	auto function = as_float ? "float" : "double";
	if (std::isnan(number)) {
		return string(function) + "('NaN')";
	}
	if (std::isinf(number)) {
		return string(function) + (number < 0 ? "('-Infinity')" : "('Infinity')");
	}
	auto precision = as_float ? std::numeric_limits<float>::max_digits10 : std::numeric_limits<double>::max_digits10;
	return FloatText(number, precision) + (as_float ? "F" : "D");
}

static string RenderInterval(interval_t interval) {
	interval = interval.Normalize();
	if (interval.months != 0 && (interval.days != 0 || interval.micros != 0)) {
		throw InvalidInputException("A Databricks interval cannot mix a month part with a day or time part");
	}
	if (interval.months != 0) {
		auto months = interval.months;
		auto negative = months < 0;
		auto absolute = negative ? -months : months;
		auto years = absolute / 12;
		auto remainder = absolute % 12;
		return StringUtil::Format("INTERVAL '%s%d-%d' YEAR TO MONTH", negative ? "-" : "", years, remainder);
	}
	auto negative = interval.days < 0 || interval.micros < 0;
	auto days = interval.days < 0 ? -interval.days : interval.days;
	auto micros = interval.micros < 0 ? -interval.micros : interval.micros;
	auto seconds = micros / Interval::MICROS_PER_SEC;
	auto fraction = micros % Interval::MICROS_PER_SEC;
	auto hours = seconds / Interval::SECS_PER_HOUR;
	seconds %= Interval::SECS_PER_HOUR;
	auto minutes = seconds / Interval::SECS_PER_MINUTE;
	seconds %= Interval::SECS_PER_MINUTE;
	return StringUtil::Format("INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY TO SECOND", negative ? "-" : "", days,
	                          static_cast<int>(hours), static_cast<int>(minutes), static_cast<int>(seconds),
	                          static_cast<int>(fraction));
}

static string RenderBlob(const string &bytes) {
	static const char HEX[] = "0123456789ABCDEF";
	string hex;
	hex.resize(bytes.size() * 2);
	for (idx_t i = 0; i < bytes.size(); i++) {
		auto byte = static_cast<unsigned char>(bytes[i]);
		hex[i * 2] = HEX[byte >> 4];
		hex[i * 2 + 1] = HEX[byte & 0x0F];
	}
	return "X'" + hex + "'";
}

string DatabricksLiteral::Render(const Value &value) {
	if (value.IsNull()) {
		if (value.type().id() == LogicalTypeId::SQLNULL) {
			return "NULL";
		}
		return "CAST(NULL AS " + TypeName(value.type()) + ")";
	}
	if (value.type().IsJSONType()) {
		return "parse_json(" + DatabricksQuoteString(StringValue::Get(value)) + ")";
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return BooleanValue::Get(value) ? "true" : "false";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return value.ToString();
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
		return value.ToString() + "BD";
	case LogicalTypeId::FLOAT:
		return RenderFloat(value, true);
	case LogicalTypeId::DOUBLE:
		return RenderFloat(value, false);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::UUID:
		return DatabricksQuoteString(value.ToString());
	case LogicalTypeId::BLOB:
		return RenderBlob(StringValue::Get(value));
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
	case LogicalTypeId::INTERVAL:
		return RenderInterval(IntervalValue::Get(value));
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		vector<string> items;
		auto &children =
		    value.type().id() == LogicalTypeId::ARRAY ? ArrayValue::GetChildren(value) : ListValue::GetChildren(value);
		for (auto &child : children) {
			items.push_back(Render(child));
		}
		return "array(" + StringUtil::Join(items, ", ") + ")";
	}
	case LogicalTypeId::MAP: {
		vector<string> items;
		for (auto &entry : MapValue::GetChildren(value)) {
			auto &pair = StructValue::GetChildren(entry);
			if (pair.size() != 2) {
				throw InvalidInputException("Databricks map values must be key/value pairs");
			}
			items.push_back(Render(pair[0]));
			items.push_back(Render(pair[1]));
		}
		return "map(" + StringUtil::Join(items, ", ") + ")";
	}
	case LogicalTypeId::STRUCT: {
		auto &children = StructValue::GetChildren(value);
		vector<string> items;
		for (idx_t i = 0; i < children.size(); i++) {
			items.push_back(DatabricksQuoteString(StructType::GetChildName(value.type(), i)));
			items.push_back(Render(children[i]));
		}
		return "named_struct(" + StringUtil::Join(items, ", ") + ")";
	}
	default:
		throw InvalidInputException("Value of type %s is not representable in Databricks", value.type().ToString());
	}
}

} // namespace duckdb

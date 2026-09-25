#include "databricks_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cctype>

namespace duckdb {

namespace {

class TypeParser {
public:
	explicit TypeParser(const string &input) : input(input) {
	}

	LogicalType Parse() {
		auto type = ParseType();
		Skip();
		if (i != input.size()) {
			throw InvalidInputException("Unrecognized trailing input in Databricks type \"%s\"", input);
		}
		return type;
	}

private:
	const string &input;
	idx_t i = 0;

	void Skip() {
		while (i < input.size() && std::isspace(static_cast<unsigned char>(input[i]))) {
			i++;
		}
	}

	bool IsWordChar(char ch) const {
		return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
	}

	string ReadWord() {
		Skip();
		if (i >= input.size() || !IsWordChar(input[i]) || std::isdigit(static_cast<unsigned char>(input[i]))) {
			throw InvalidInputException("Expected a type name in Databricks type \"%s\"", input);
		}
		auto start = i;
		while (i < input.size() && IsWordChar(input[i])) {
			i++;
		}
		return input.substr(start, i - start);
	}

	string PeekWord() {
		auto saved = i;
		Skip();
		if (i >= input.size() || !IsWordChar(input[i]) || std::isdigit(static_cast<unsigned char>(input[i]))) {
			i = saved;
			return "";
		}
		auto start = i;
		while (i < input.size() && IsWordChar(input[i])) {
			i++;
		}
		auto word = input.substr(start, i - start);
		i = saved;
		return word;
	}

	bool Consume(char expected) {
		Skip();
		if (i < input.size() && input[i] == expected) {
			i++;
			return true;
		}
		return false;
	}

	void Expect(char expected) {
		if (!Consume(expected)) {
			throw InvalidInputException("Expected '%c' in Databricks type \"%s\"", expected, input);
		}
	}

	int64_t ReadInt() {
		Skip();
		if (i >= input.size() || !std::isdigit(static_cast<unsigned char>(input[i]))) {
			throw InvalidInputException("Expected a number in Databricks type \"%s\"", input);
		}
		int64_t value = 0;
		while (i < input.size() && std::isdigit(static_cast<unsigned char>(input[i]))) {
			value = value * 10 + (input[i] - '0');
			i++;
		}
		return value;
	}

	string ReadFieldName() {
		Skip();
		if (i < input.size() && input[i] == '`') {
			i++;
			string name;
			while (i < input.size()) {
				if (input[i] == '`') {
					if (i + 1 < input.size() && input[i + 1] == '`') {
						name.push_back('`');
						i += 2;
						continue;
					}
					i++;
					return name;
				}
				name.push_back(input[i]);
				i++;
			}
			throw InvalidInputException("Unterminated quoted identifier in Databricks type \"%s\"", input);
		}
		return ReadWord();
	}

	void SkipNotNull() {
		auto next = StringUtil::Upper(PeekWord());
		if (next != "NOT") {
			return;
		}
		ReadWord();
		auto null_word = StringUtil::Upper(ReadWord());
		if (null_word != "NULL") {
			throw InvalidInputException("Expected NULL after NOT in Databricks type \"%s\"", input);
		}
	}

	void SkipCollate() {
		if (StringUtil::Upper(PeekWord()) != "COLLATE") {
			return;
		}
		ReadWord();
		Skip();
		if (i < input.size() && input[i] == '`') {
			ReadFieldName();
			return;
		}
		ReadWord();
	}

	void ConsumeIntervalQualifier() {
		while (true) {
			auto next = StringUtil::Upper(PeekWord());
			if (next != "YEAR" && next != "MONTH" && next != "DAY" && next != "HOUR" && next != "MINUTE" &&
			    next != "SECOND" && next != "TO") {
				break;
			}
			ReadWord();
		}
	}

	LogicalType ParseType() {
		auto word = StringUtil::Upper(ReadWord());
		LogicalType result;
		if (word == "ARRAY") {
			Expect('<');
			auto child = ParseType();
			Expect('>');
			result = LogicalType::LIST(child);
		} else if (word == "MAP") {
			Expect('<');
			auto key = ParseType();
			Expect(',');
			auto value = ParseType();
			Expect('>');
			result = LogicalType::MAP(key, value);
		} else if (word == "STRUCT") {
			Expect('<');
			child_list_t<LogicalType> children;
			Skip();
			if (!Consume('>')) {
				while (true) {
					auto name = ReadFieldName();
					Expect(':');
					children.emplace_back(std::move(name), ParseType());
					if (!Consume(',')) {
						break;
					}
				}
				Expect('>');
			}
			result = LogicalType::STRUCT(std::move(children));
		} else if (word == "DECIMAL" || word == "DEC" || word == "NUMERIC") {
			uint8_t width = 38;
			uint8_t scale = 0;
			if (Consume('(')) {
				auto parsed_width = ReadInt();
				if (Consume(',')) {
					auto parsed_scale = ReadInt();
					if (parsed_scale < 0 || parsed_scale > 38) {
						throw InvalidInputException("Decimal scale %d is out of range in \"%s\"", parsed_scale, input);
					}
					scale = static_cast<uint8_t>(parsed_scale);
				}
				Expect(')');
				if (parsed_width < 1 || parsed_width > 38 || scale > parsed_width) {
					throw InvalidInputException("Decimal precision %d is out of range in \"%s\"", parsed_width, input);
				}
				width = static_cast<uint8_t>(parsed_width);
			}
			result = LogicalType::DECIMAL(width, scale);
		} else if (word == "STRING" || word == "CHAR" || word == "CHARACTER" || word == "VARCHAR") {
			if (Consume('(')) {
				ReadInt();
				Expect(')');
			}
			SkipCollate();
			result = LogicalType::VARCHAR;
		} else if (word == "INTERVAL") {
			ConsumeIntervalQualifier();
			result = LogicalType::INTERVAL;
		} else if (word == "BOOLEAN" || word == "BOOL") {
			result = LogicalType::BOOLEAN;
		} else if (word == "TINYINT" || word == "BYTE") {
			result = LogicalType::TINYINT;
		} else if (word == "SMALLINT" || word == "SHORT") {
			result = LogicalType::SMALLINT;
		} else if (word == "INT" || word == "INTEGER") {
			result = LogicalType::INTEGER;
		} else if (word == "BIGINT" || word == "LONG") {
			result = LogicalType::BIGINT;
		} else if (word == "FLOAT" || word == "REAL") {
			result = LogicalType::FLOAT;
		} else if (word == "DOUBLE") {
			result = LogicalType::DOUBLE;
		} else if (word == "DATE") {
			result = LogicalType::DATE;
		} else if (word == "TIMESTAMP_NTZ") {
			result = LogicalType::TIMESTAMP;
		} else if (word == "TIMESTAMP" || word == "TIMESTAMP_LTZ") {
			result = LogicalType::TIMESTAMP_TZ;
		} else if (word == "BINARY" || word == "VARBINARY") {
			result = LogicalType::BLOB;
		} else if (word == "VARIANT") {
			result = LogicalType::JSON();
		} else if (word == "VOID") {
			result = LogicalType::INTEGER;
		} else {
			// GEOMETRY, GEOGRAPHY, OBJECT, and anything else the warehouse adds
			result = LogicalType::VARCHAR;
		}
		SkipNotNull();
		return result;
	}
};

} // namespace

LogicalType DatabricksTypes::Parse(const string &type_text) {
	if (type_text.empty()) {
		throw InvalidInputException("Databricks type text is empty");
	}
	try {
		return TypeParser(type_text).Parse();
	} catch (const InvalidInputException &) {
		throw;
	} catch (const std::exception &ex) {
		throw InvalidInputException("Failed to parse Databricks type \"%s\": %s", type_text, ex.what());
	}
}

} // namespace duckdb

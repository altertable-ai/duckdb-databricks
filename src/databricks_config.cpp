#include "databricks_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void DatabricksConfig::SetHost(string raw) {
	StringUtil::Trim(raw);
	while (!raw.empty() && raw.back() == '/') {
		raw.pop_back();
	}
	auto lower = StringUtil::Lower(raw);
	if (StringUtil::StartsWith(lower, "https://")) {
		scheme = "https";
		raw = raw.substr(8);
	} else if (StringUtil::StartsWith(lower, "http://")) {
		scheme = "http";
		raw = raw.substr(7);
	} else {
		scheme = "https";
	}
	auto slash = raw.find('/');
	if (slash != string::npos) {
		raw = raw.substr(0, slash);
	}
	host = std::move(raw);
	if (host.empty()) {
		throw InvalidInputException("HOST is required");
	}
}

void DatabricksConfig::ValidateAuth() const {
	if (host.empty()) {
		throw InvalidInputException("HOST is required");
	}
	auto has_token = !token.empty();
	auto has_id = !client_id.empty();
	auto has_secret = !client_secret.empty();
	if (has_token && (has_id || has_secret)) {
		throw InvalidInputException("TOKEN cannot be combined with CLIENT_ID");
	}
	if (has_token) {
		return;
	}
	if (has_id && has_secret) {
		return;
	}
	if (has_id != has_secret) {
		throw InvalidInputException("CLIENT_ID and CLIENT_SECRET must both be set");
	}
	throw InvalidInputException("Specify TOKEN, or CLIENT_ID and CLIENT_SECRET");
}

void DatabricksConfig::ValidateAttach() const {
	ValidateAuth();
	if (warehouse_id.empty()) {
		throw BinderException("WAREHOUSE_ID is required on the secret or on ATTACH");
	}
	if (catalog.empty()) {
		throw BinderException("ATTACH requires a catalog name, or CATALOG on the secret");
	}
}

string DatabricksConfig::BaseUrl() const {
	return scheme + "://" + host;
}

string DatabricksConfig::Identity() const {
	return BaseUrl() + "/" + warehouse_id + "/" + catalog;
}

} // namespace duckdb

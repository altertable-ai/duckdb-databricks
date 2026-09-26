#include "databricks_auth.hpp"

#include "databricks_http.hpp"
#include "duckdb/common/exception.hpp"

#include "nlohmann/json.hpp"

namespace duckdb {

DatabricksAuth::DatabricksAuth(DatabricksConfig config_p) : config(std::move(config_p)) {
	config.oauth = config.token.empty();
}

bool DatabricksAuth::TokenIsFresh() const {
	if (!has_token) {
		return false;
	}
	return std::chrono::steady_clock::now() + std::chrono::seconds(60) < expires_at;
}

void DatabricksAuth::Invalidate() {
	lock_guard<mutex> guard(lock);
	has_token = false;
	access_token.clear();
}

void DatabricksAuth::Refresh(ClientContext &context) {
	auto url = config.BaseUrl() + "/oidc/v1/token";
	string body = "grant_type=client_credentials&scope=all-apis";
	string userpwd = config.client_id + ":" + config.client_secret;
	// Basic auth is sent as a header built here so the HTTP helper never logs it
	string basic = "Basic ";
	static const char *table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	string encoded;
	idx_t i = 0;
	while (i < userpwd.size()) {
		unsigned char bytes[3] = {0, 0, 0};
		int count = 0;
		for (; count < 3 && i < userpwd.size(); count++, i++) {
			bytes[count] = static_cast<unsigned char>(userpwd[i]);
		}
		encoded.push_back(table[bytes[0] >> 2]);
		encoded.push_back(table[((bytes[0] & 0x03) << 4) | (bytes[1] >> 4)]);
		if (count > 1) {
			encoded.push_back(table[((bytes[1] & 0x0F) << 2) | (bytes[2] >> 6)]);
		} else {
			encoded.push_back('=');
		}
		if (count > 2) {
			encoded.push_back(table[bytes[2] & 0x3F]);
		} else {
			encoded.push_back('=');
		}
	}
	basic += encoded;
	auto response = DatabricksHttp::Request(context, config, "auth", "POST", url, body, {}, basic,
	                                        "application/x-www-form-urlencoded", true);
	if (response.status < 200 || response.status >= 300) {
		string message = "token request failed";
		auto parsed = nlohmann::json::parse(response.body, nullptr, false);
		if (!parsed.is_discarded() && parsed.contains("message") && parsed["message"].is_string()) {
			message = parsed["message"].get<string>();
		}
		auto code =
		    parsed.is_discarded() || !parsed.contains("error_code") ? string("HTTP") : parsed["error_code"].dump();
		if (!parsed.is_discarded() && parsed.contains("error_code") && parsed["error_code"].is_string()) {
			code = parsed["error_code"].get<string>();
		}
		throw IOException("Databricks error %s: %s (HTTP %d)", code, message, response.status);
	}
	auto parsed = nlohmann::json::parse(response.body, nullptr, false);
	if (parsed.is_discarded() || !parsed.contains("access_token") || !parsed["access_token"].is_string()) {
		throw IOException("Databricks transport error during auth: token response has no access_token");
	}
	access_token = parsed["access_token"].get<string>();
	int64_t expires_in = 3600;
	if (parsed.contains("expires_in") && parsed["expires_in"].is_number()) {
		expires_in = parsed["expires_in"].get<int64_t>();
	}
	if (expires_in < 0) {
		expires_in = 0;
	}
	expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(expires_in);
	has_token = true;
}

string DatabricksAuth::AuthorizationHeader(ClientContext &context) {
	if (!config.oauth) {
		return "Bearer " + config.token;
	}
	lock_guard<mutex> guard(lock);
	if (!TokenIsFresh()) {
		Refresh(context);
	}
	return "Bearer " + access_token;
}

} // namespace duckdb

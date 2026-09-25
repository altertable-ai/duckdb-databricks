#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

struct DatabricksConfig {
	string scheme = "https";
	string host;
	string warehouse_id;
	string catalog;
	string token;
	string client_id;
	string client_secret;
	bool oauth = false;

	void SetHost(string raw);
	void ValidateAuth() const;
	void ValidateAttach() const;
	string BaseUrl() const;
	//! Thread-local curl handles are keyed by this, one per attached database
	string Identity() const;
};

} // namespace duckdb

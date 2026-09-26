#pragma once

#include "databricks_config.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>

namespace duckdb {

class DatabricksAuth {
public:
	explicit DatabricksAuth(DatabricksConfig config);

	//! Bearer token. PAT returns the configured token. M2M refreshes under the mutex when the cached token
	//! is within 60 seconds of expiry.
	string AuthorizationHeader(ClientContext &context);
	//! Drops the cached M2M token so the next AuthorizationHeader() fetches a new one
	void Invalidate();

	const DatabricksConfig &Config() const {
		return config;
	}

private:
	void Refresh(ClientContext &context);
	bool TokenIsFresh() const;

	DatabricksConfig config;
	mutex lock;
	string access_token;
	std::chrono::steady_clock::time_point expires_at {};
	bool has_token = false;
};

} // namespace duckdb

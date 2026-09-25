#pragma once

#include "databricks_config.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"

#include <string>

namespace duckdb {

struct DatabricksHttpResponse {
	long status = 0;
	string body;
	unordered_map<string, string> headers;
	string curl_error;
	bool transport_error = false;
	bool interrupted = false;
};

class DatabricksHttp {
public:
	//! `step` is auth, submit, poll, chunk download, or cancel. Retries 429, 503, and connection or timeout
	//! failures. Does not send Authorization when authorization_header is empty. Presigned downloads pass the
	//! link headers and no Authorization header.
	static DatabricksHttpResponse Request(ClientContext &context, const DatabricksConfig &config, const string &step,
	                                      const string &method, const string &url, const string &body,
	                                      const vector<std::pair<string, string>> &extra_headers,
	                                      const string &authorization_header, const string &content_type,
	                                      bool allow_interrupt = true);
};

} // namespace duckdb

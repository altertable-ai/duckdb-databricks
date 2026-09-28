#pragma once

#include "databricks_auth.hpp"
#include "databricks_config.hpp"
#include "databricks_http.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include <mutex>
#include <optional>

namespace duckdb {

enum class DatabricksStatementMode : uint8_t { SMALL, SCAN };

struct DatabricksParameter {
	string name;
	string value;
};

struct DatabricksExternalLink {
	string url;
	string expiration;
	vector<std::pair<string, string>> http_headers;
	idx_t row_count = 0;
};

struct DatabricksResultColumn {
	string name;
	string type_text;
	string type_name;
};

struct DatabricksStatementResult {
	string statement_id;
	string state;
	vector<DatabricksResultColumn> columns;
	idx_t total_chunk_count = 0;
	idx_t total_row_count = 0;
	//! Small-mode JSON_ARRAY rows. Null cells are nullopt
	vector<vector<std::optional<string>>> rows;
	//! external_links[chunk_index], filled for chunks the submit response already described
	vector<vector<DatabricksExternalLink>> chunk_links;
	optional_idx num_affected_rows;
};

class DatabricksSession {
public:
	explicit DatabricksSession(DatabricksConfig config);

	const DatabricksConfig &Config() const {
		return config;
	}
	DatabricksAuth &Auth() {
		return auth;
	}

	DatabricksStatementResult Execute(ClientContext &context, DatabricksStatementMode mode, const string &sql,
	                                  const string &catalog, const string &schema,
	                                  const vector<DatabricksParameter> &parameters);
	//! Presigned links for one result chunk. The caller caches them.
	vector<DatabricksExternalLink> FetchChunkLinks(ClientContext &context, const string &statement_id,
	                                               idx_t chunk_index);
	void Cancel(ClientContext &context, const string &statement_id);

	std::optional<string> Cell(const DatabricksStatementResult &result, idx_t row, const string &column) const;

private:
	DatabricksStatementResult ExecuteOnce(ClientContext &context, DatabricksStatementMode mode, const string &sql,
	                                      const string &catalog, const string &schema,
	                                      const vector<DatabricksParameter> &parameters, bool allow_auth_retry);
	DatabricksHttpResponse AuthedRequest(ClientContext &context, const string &step, const string &method,
	                                     const string &url, const string &body, bool allow_auth_retry);
	void Poll(ClientContext &context, DatabricksStatementResult &result, string &last_body);
	void ParseStatement(const string &body, DatabricksStatementResult &result) const;
	void ThrowApiError(const string &body, int64_t status, const string &fallback_state) const;

	DatabricksConfig config;
	DatabricksAuth auth;
};

//! Owns one statement result and its chunk-link cache. Safe for overlapping chunk downloads.
class DatabricksResultReader {
public:
	DatabricksResultReader(shared_ptr<DatabricksSession> session, DatabricksStatementResult result);

	const DatabricksStatementResult &Result() const {
		return result;
	}
	//! Presigned Arrow bytes for one result chunk. Refreshes an expired or 403 link once.
	vector<string> Download(ClientContext &context, idx_t chunk_index);

private:
	vector<DatabricksExternalLink> ChunkLinks(ClientContext &context, idx_t chunk_index, bool refresh);
	bool TryDownload(ClientContext &context, const vector<DatabricksExternalLink> &links, vector<string> &payloads);

	shared_ptr<DatabricksSession> session;
	DatabricksStatementResult result;
	std::mutex lock;
};

} // namespace duckdb

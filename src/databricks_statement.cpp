#include "databricks_statement.hpp"

#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

#include "nlohmann/json.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

namespace {

constexpr idx_t SMALL_RESULT_LIMIT = 25 * 1024 * 1024;

std::optional<string> JsonCell(const nlohmann::json &cell) {
	if (cell.is_null()) {
		return std::nullopt;
	}
	if (cell.is_string()) {
		return cell.get<string>();
	}
	return cell.dump();
}

vector<std::pair<string, string>> JsonHeaders(const nlohmann::json &value) {
	vector<std::pair<string, string>> headers;
	if (value.is_object()) {
		for (auto it = value.begin(); it != value.end(); ++it) {
			if (it.value().is_string()) {
				headers.emplace_back(it.key(), it.value().get<string>());
			} else if (!it.value().is_null()) {
				headers.emplace_back(it.key(), it.value().dump());
			}
		}
	} else if (value.is_array()) {
		for (auto &entry : value) {
			if (entry.is_object() && entry.contains("name") && entry.contains("value")) {
				headers.emplace_back(entry["name"].dump(),
				                     entry["value"].is_string() ? entry["value"].get<string>() : entry["value"].dump());
				if (entry["name"].is_string()) {
					headers.back().first = entry["name"].get<string>();
				}
			}
		}
	}
	return headers;
}

bool LinkExpired(const string &expiration) {
	if (expiration.empty()) {
		return false;
	}
	int year, month, day, hour, minute, second;
	if (sscanf(expiration.c_str(), "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second) != 6) {
		return false;
	}
	std::tm tm {};
	tm.tm_year = year - 1900;
	tm.tm_mon = month - 1;
	tm.tm_mday = day;
	tm.tm_hour = hour;
	tm.tm_min = minute;
	tm.tm_sec = second;
#if defined(_WIN32)
	auto when = _mkgmtime(&tm);
#else
	auto when = timegm(&tm);
#endif
	if (when == static_cast<time_t>(-1)) {
		return false;
	}
	return std::chrono::system_clock::now() >= std::chrono::system_clock::from_time_t(when);
}

bool LinksExpired(const vector<DatabricksExternalLink> &links) {
	for (auto &link : links) {
		if (LinkExpired(link.expiration)) {
			return true;
		}
	}
	return false;
}

} // namespace

DatabricksSession::DatabricksSession(DatabricksConfig config_p) : config(std::move(config_p)), auth(config) {
}

std::optional<string> DatabricksSession::Cell(const DatabricksStatementResult &result, idx_t row,
                                              const string &column) const {
	if (row >= result.rows.size()) {
		throw InternalException("Databricks result row %llu is out of range", row);
	}
	for (idx_t i = 0; i < result.columns.size(); i++) {
		if (StringUtil::CIEquals(result.columns[i].name, column)) {
			return result.rows[row][i];
		}
	}
	throw InternalException("Databricks result has no column \"%s\"", column);
}

void DatabricksSession::ThrowApiError(const string &body, int64_t status, const string &fallback_state) const {
	auto parsed = nlohmann::json::parse(body, nullptr, false);
	string code = "HTTP";
	string message = fallback_state.empty() ? "request failed" : fallback_state;
	if (!parsed.is_discarded()) {
		const nlohmann::json *error = nullptr;
		if (parsed.contains("status") && parsed["status"].is_object() && parsed["status"].contains("error")) {
			error = &parsed["status"]["error"];
		} else if (parsed.contains("error_code") || parsed.contains("message")) {
			error = &parsed;
		}
		if (error) {
			if (error->contains("error_code") && (*error)["error_code"].is_string()) {
				code = (*error)["error_code"].get<string>();
			}
			if (error->contains("message") && (*error)["message"].is_string()) {
				message = (*error)["message"].get<string>();
			}
		}
	}
	throw IOException("Databricks error %s: %s (HTTP %lld)", code, message, static_cast<long long>(status));
}

void DatabricksSession::ParseStatement(const string &body, DatabricksStatementResult &result) const {
	auto parsed = nlohmann::json::parse(body, nullptr, false);
	if (parsed.is_discarded()) {
		throw IOException("Databricks transport error during submit: response was not JSON");
	}
	if (parsed.contains("statement_id") && parsed["statement_id"].is_string()) {
		result.statement_id = parsed["statement_id"].get<string>();
	}
	if (parsed.contains("status") && parsed["status"].is_object()) {
		auto &status = parsed["status"];
		if (status.contains("state") && status["state"].is_string()) {
			result.state = status["state"].get<string>();
		}
	}
	if (parsed.contains("manifest") && parsed["manifest"].is_object()) {
		auto &manifest = parsed["manifest"];
		if (manifest.contains("total_chunk_count") && manifest["total_chunk_count"].is_number()) {
			result.total_chunk_count = manifest["total_chunk_count"].get<idx_t>();
		}
		if (manifest.contains("total_row_count") && manifest["total_row_count"].is_number()) {
			result.total_row_count = manifest["total_row_count"].get<idx_t>();
		}
		if (manifest.contains("schema") && manifest["schema"].is_object() && manifest["schema"].contains("columns") &&
		    manifest["schema"]["columns"].is_array()) {
			result.columns.clear();
			for (auto &column : manifest["schema"]["columns"]) {
				DatabricksResultColumn parsed_column;
				if (column.contains("name") && column["name"].is_string()) {
					parsed_column.name = column["name"].get<string>();
				}
				if (column.contains("type_text") && column["type_text"].is_string()) {
					parsed_column.type_text = column["type_text"].get<string>();
				}
				if (column.contains("type_name") && column["type_name"].is_string()) {
					parsed_column.type_name = column["type_name"].get<string>();
				}
				result.columns.push_back(std::move(parsed_column));
			}
		}
	}
	const nlohmann::json *payload_ptr = nullptr;
	if (parsed.contains("result") && parsed["result"].is_object()) {
		payload_ptr = &parsed["result"];
	} else if (parsed.contains("external_links") || parsed.contains("data_array")) {
		payload_ptr = &parsed;
	}
	if (!payload_ptr) {
		return;
	}
	auto &payload = *payload_ptr;
	if (payload.contains("external_links") && payload["external_links"].is_array()) {
		if (result.chunk_links.size() < result.total_chunk_count) {
			result.chunk_links.resize(result.total_chunk_count);
		}
		for (auto &link_json : payload["external_links"]) {
			DatabricksExternalLink link;
			if (link_json.contains("external_link") && link_json["external_link"].is_string()) {
				link.url = link_json["external_link"].get<string>();
			}
			if (link_json.contains("expiration") && link_json["expiration"].is_string()) {
				link.expiration = link_json["expiration"].get<string>();
			}
			if (link_json.contains("row_count") && link_json["row_count"].is_number()) {
				link.row_count = link_json["row_count"].get<idx_t>();
			}
			if (link_json.contains("http_headers")) {
				link.http_headers = JsonHeaders(link_json["http_headers"]);
			}
			idx_t chunk_index = 0;
			if (link_json.contains("chunk_index") && link_json["chunk_index"].is_number()) {
				chunk_index = link_json["chunk_index"].get<idx_t>();
			}
			if (result.chunk_links.size() <= chunk_index) {
				result.chunk_links.resize(chunk_index + 1);
			}
			result.chunk_links[chunk_index].push_back(std::move(link));
		}
	}
	if (payload.contains("data_array") && payload["data_array"].is_array()) {
		result.rows.clear();
		for (auto &row : payload["data_array"]) {
			vector<std::optional<string>> parsed_row;
			if (row.is_array()) {
				for (auto &cell : row) {
					parsed_row.push_back(JsonCell(cell));
				}
			}
			result.rows.push_back(std::move(parsed_row));
		}
	}
	for (auto &column : result.columns) {
		if (!StringUtil::CIEquals(column.name, "num_affected_rows") &&
		    !StringUtil::CIEquals(column.name, "num_inserted_rows") &&
		    !StringUtil::CIEquals(column.name, "num_updated_rows") &&
		    !StringUtil::CIEquals(column.name, "num_deleted_rows")) {
			continue;
		}
		if (!result.rows.empty()) {
			auto cell = Cell(result, 0, column.name);
			if (cell && !cell->empty()) {
				try {
					result.num_affected_rows = static_cast<idx_t>(std::stoull(*cell));
				} catch (const std::exception &) {
				}
			}
		}
	}
}

DatabricksHttpResponse DatabricksSession::AuthedRequest(ClientContext &context, const string &step,
                                                        const string &method, const string &url, const string &body,
                                                        bool allow_auth_retry) {
	auto response =
	    DatabricksHttp::Request(context, config, step, method, url, body, {}, auth.AuthorizationHeader(context),
	                            body.empty() && method == "GET" ? "" : "application/json", true);
	if (response.status == 401 && allow_auth_retry && config.token.empty()) {
		auth.Invalidate();
		response =
		    DatabricksHttp::Request(context, config, step, method, url, body, {}, auth.AuthorizationHeader(context),
		                            body.empty() && method == "GET" ? "" : "application/json", true);
	}
	return response;
}

void DatabricksSession::Poll(ClientContext &context, DatabricksStatementResult &result, string &last_body) {
	auto timeout_ms = DatabricksSettingIndex(context, "dbx_statement_timeout_ms", 0);
	auto started = std::chrono::steady_clock::now();
	idx_t delay = 100;
	auto url = config.BaseUrl() + "/api/2.0/sql/statements/" + result.statement_id;
	while (result.state == "PENDING" || result.state == "RUNNING") {
		if (context.IsInterrupted()) {
			Cancel(context, result.statement_id);
			throw InterruptException();
		}
		if (timeout_ms > 0) {
			auto elapsed =
			    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started)
			        .count();
			if (elapsed >= static_cast<int64_t>(timeout_ms)) {
				Cancel(context, result.statement_id);
				throw IOException("Databricks statement timed out after %llu ms and was canceled", timeout_ms);
			}
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(delay));
		if (delay < 2000) {
			delay = std::min<idx_t>(delay * 2, 2000);
		}
		auto response = AuthedRequest(context, "poll", "GET", url, "", true);
		if (response.status < 200 || response.status >= 300) {
			ThrowApiError(response.body, response.status, "poll failed");
		}
		last_body = response.body;
		ParseStatement(last_body, result);
	}
}

void DatabricksSession::Cancel(ClientContext &context, const string &statement_id) {
	if (statement_id.empty()) {
		return;
	}
	try {
		auto url = config.BaseUrl() + "/api/2.0/sql/statements/" + statement_id + "/cancel";
		DatabricksHttp::Request(context, config, "cancel", "POST", url, "{}", {}, auth.AuthorizationHeader(context),
		                        "application/json", false);
	} catch (const std::exception &) {
	}
}

DatabricksStatementResult DatabricksSession::Execute(ClientContext &context, DatabricksStatementMode mode,
                                                     const string &sql, const string &catalog, const string &schema,
                                                     const vector<DatabricksParameter> &parameters) {
	try {
		return ExecuteOnce(context, mode, sql, catalog, schema, parameters, true);
	} catch (const InterruptException &) {
		throw;
	}
}

DatabricksStatementResult DatabricksSession::ExecuteOnce(ClientContext &context, DatabricksStatementMode mode,
                                                         const string &sql, const string &catalog, const string &schema,
                                                         const vector<DatabricksParameter> &parameters,
                                                         bool allow_auth_retry) {
	nlohmann::json payload;
	payload["warehouse_id"] = config.warehouse_id;
	payload["catalog"] = catalog;
	payload["schema"] = schema;
	payload["statement"] = sql;
	payload["wait_timeout"] = "10s";
	payload["on_wait_timeout"] = "CONTINUE";
	payload["disposition"] = mode == DatabricksStatementMode::SMALL ? "INLINE" : "EXTERNAL_LINKS";
	payload["format"] = mode == DatabricksStatementMode::SMALL ? "JSON_ARRAY" : "ARROW_STREAM";
	if (!parameters.empty()) {
		auto params = nlohmann::json::array();
		for (auto &parameter : parameters) {
			params.push_back({{"name", parameter.name}, {"value", parameter.value}, {"type", "STRING"}});
		}
		payload["parameters"] = std::move(params);
	}
	auto body = payload.dump();
	auto url = config.BaseUrl() + "/api/2.0/sql/statements";
	auto response = AuthedRequest(context, "submit", "POST", url, body, allow_auth_retry);
	if (mode == DatabricksStatementMode::SMALL && response.body.size() > SMALL_RESULT_LIMIT) {
		throw IOException("Databricks statement response is larger than 25 MiB; use databricks_query() to read it");
	}
	if (response.status == 401) {
		ThrowApiError(response.body, response.status, "authentication failed");
	}
	if (response.status < 200 || response.status >= 300) {
		ThrowApiError(response.body, response.status, "submit failed");
	}
	DatabricksStatementResult result;
	auto last_body = response.body;
	ParseStatement(last_body, result);
	if (DatabricksSettingBool(context, "dbx_debug_show_queries", false)) {
		Printer::Print(StringUtil::Format("databricks statement %s: %s", result.statement_id, sql));
	}
	if (result.state == "PENDING" || result.state == "RUNNING") {
		Poll(context, result, last_body);
	}
	if (result.state == "FAILED" || result.state == "CANCELED" || result.state == "CLOSED") {
		ThrowApiError(last_body, response.status, result.state);
	}
	if (result.state != "SUCCEEDED") {
		ThrowApiError(response.body, response.status,
		              result.state.empty() ? "statement did not succeed" : result.state);
	}
	return result;
}

vector<DatabricksExternalLink> DatabricksSession::FetchChunkLinks(ClientContext &context, const string &statement_id,
                                                                  idx_t chunk_index) {
	auto url =
	    config.BaseUrl() + "/api/2.0/sql/statements/" + statement_id + "/result/chunks/" + to_string(chunk_index);
	auto response = AuthedRequest(context, "chunk download", "GET", url, "", true);
	if (response.status < 200 || response.status >= 300) {
		ThrowApiError(response.body, response.status, "chunk link refresh failed");
	}
	DatabricksStatementResult parsed;
	parsed.total_chunk_count = chunk_index + 1;
	ParseStatement(response.body, parsed);
	if (chunk_index < parsed.chunk_links.size()) {
		return parsed.chunk_links[chunk_index];
	}
	throw IOException("Databricks transport error during chunk download: refreshed links for chunk %llu were empty",
	                  chunk_index);
}

DatabricksResultReader::DatabricksResultReader(shared_ptr<DatabricksSession> session_p,
                                               DatabricksStatementResult result_p)
    : session(std::move(session_p)), result(std::move(result_p)) {
}

vector<DatabricksExternalLink> DatabricksResultReader::ChunkLinks(ClientContext &context, idx_t chunk_index,
                                                                  bool refresh) {
	if (!refresh) {
		lock_guard<mutex> guard(lock);
		if (chunk_index < result.chunk_links.size() && !result.chunk_links[chunk_index].empty()) {
			return result.chunk_links[chunk_index];
		}
	}
	auto fetched = session->FetchChunkLinks(context, result.statement_id, chunk_index);
	lock_guard<mutex> guard(lock);
	if (result.chunk_links.size() <= chunk_index) {
		result.chunk_links.resize(chunk_index + 1);
	}
	if (refresh || result.chunk_links[chunk_index].empty()) {
		result.chunk_links[chunk_index] = fetched;
	}
	return result.chunk_links[chunk_index];
}

bool DatabricksResultReader::TryDownload(ClientContext &context, const vector<DatabricksExternalLink> &links,
                                         vector<string> &payloads) {
	for (auto &link : links) {
		auto response = DatabricksHttp::Request(context, session->Config(), "chunk download", "GET", link.url, "",
		                                        link.http_headers, "", "", true);
		if (response.status == 403) {
			return false;
		}
		if (response.status < 200 || response.status >= 300) {
			throw IOException("Databricks transport error during chunk download: HTTP %lld for %s",
			                  static_cast<long long>(response.status), DatabricksRedactUrl(link.url));
		}
		payloads.push_back(std::move(response.body));
	}
	return true;
}

vector<string> DatabricksResultReader::Download(ClientContext &context, idx_t chunk_index) {
	auto links = ChunkLinks(context, chunk_index, false);
	if (LinksExpired(links)) {
		links = ChunkLinks(context, chunk_index, true);
	}
	vector<string> payloads;
	if (!TryDownload(context, links, payloads)) {
		links = ChunkLinks(context, chunk_index, true);
		payloads.clear();
		if (!TryDownload(context, links, payloads)) {
			throw IOException("Databricks transport error during chunk download: HTTP 403 for chunk %llu", chunk_index);
		}
	}
	return payloads;
}

} // namespace duckdb

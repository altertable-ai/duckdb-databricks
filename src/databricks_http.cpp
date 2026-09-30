#include "databricks_http.hpp"

#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <curl/curl.h>
#include <mutex>
#include <random>
#include <thread>

namespace duckdb {

namespace {

struct EasyHandle {
	CURL *easy = nullptr;
	~EasyHandle() {
		if (easy) {
			curl_easy_cleanup(easy);
		}
	}
	CURL *Get() {
		if (!easy) {
			easy = curl_easy_init();
			if (!easy) {
				throw IOException("Databricks transport error during init: curl_easy_init failed");
			}
		} else {
			curl_easy_reset(easy);
		}
		return easy;
	}
};

void EnsureCurl() {
	static std::once_flag once;
	std::call_once(once, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
}

CURL *HandleFor(const DatabricksConfig &config) {
	EnsureCurl();
	thread_local unordered_map<string, EasyHandle> handles;
	return handles[config.Identity()].Get();
}

size_t WriteBody(char *ptr, size_t size, size_t nmemb, void *userdata) {
	auto &body = *static_cast<string *>(userdata);
	body.append(ptr, size * nmemb);
	return size * nmemb;
}

size_t WriteHeader(char *ptr, size_t size, size_t nmemb, void *userdata) {
	auto &headers = *static_cast<unordered_map<string, string> *>(userdata);
	string line(ptr, size * nmemb);
	auto colon = line.find(':');
	if (colon != string::npos) {
		auto key = StringUtil::Lower(line.substr(0, colon));
		auto value = line.substr(colon + 1);
		StringUtil::Trim(value);
		while (!value.empty() && (value.back() == '\r' || value.back() == '\n')) {
			value.pop_back();
		}
		headers[key] = value;
	}
	return size * nmemb;
}

int Progress(void *clientp, curl_off_t, curl_off_t, curl_off_t, curl_off_t) {
	auto *context = static_cast<ClientContext *>(clientp);
	if (context->IsInterrupted()) {
		return 1;
	}
	return 0;
}

bool RetryableCurl(CURLcode code) {
	switch (code) {
	case CURLE_COULDNT_RESOLVE_HOST:
	case CURLE_COULDNT_CONNECT:
	case CURLE_OPERATION_TIMEDOUT:
	case CURLE_RECV_ERROR:
	case CURLE_SEND_ERROR:
	case CURLE_GOT_NOTHING:
	case CURLE_PARTIAL_FILE:
	case CURLE_WEIRD_SERVER_REPLY:
		return true;
	default:
		return false;
	}
}

idx_t BackoffMs(idx_t attempt, const unordered_map<string, string> &headers) {
	auto retry_after = headers.find("retry-after");
	if (retry_after != headers.end()) {
		try {
			auto seconds = std::stoll(retry_after->second);
			if (seconds < 0) {
				seconds = 0;
			}
			if (seconds > 30) {
				seconds = 30;
			}
			return static_cast<idx_t>(seconds) * 1000;
		} catch (const std::exception &) {
		}
	}
	idx_t delay = 100;
	for (idx_t i = 0; i < attempt && delay < 2000; i++) {
		delay *= 2;
	}
	if (delay > 2000) {
		delay = 2000;
	}
	thread_local std::mt19937 rng {std::random_device {}()};
	std::uniform_int_distribution<idx_t> jitter(0, delay / 2 + 1);
	return delay + jitter(rng);
}

void SleepOrInterrupt(ClientContext &context, idx_t delay_ms, bool allow_interrupt) {
	auto remaining = delay_ms;
	while (remaining > 0) {
		if (allow_interrupt && context.IsInterrupted()) {
			throw InterruptException();
		}
		auto slice = remaining > 50 ? 50 : remaining;
		std::this_thread::sleep_for(std::chrono::milliseconds(slice));
		remaining -= slice;
	}
}

} // namespace

DatabricksHttpResponse DatabricksHttp::Request(ClientContext &context, const DatabricksConfig &config,
                                               const string &step, const string &method, const string &url,
                                               const string &body,
                                               const vector<std::pair<string, string>> &extra_headers,
                                               const string &authorization_header, const string &content_type,
                                               bool allow_interrupt) {
	auto retries = DatabricksSettingIndex(context, "dbx_http_retries", 5);
	auto timeout_ms = DatabricksSettingIndex(context, "dbx_http_timeout_ms", 60000);
	auto ca_cert = DatabricksSettingString(context, "dbx_ca_cert", "system");
	DatabricksHttpResponse last;
	for (idx_t attempt = 0;; attempt++) {
		if (allow_interrupt && context.IsInterrupted()) {
			throw InterruptException();
		}
		auto *easy = HandleFor(config);
		DatabricksHttpResponse response;
		curl_easy_setopt(easy, CURLOPT_URL, url.c_str());
		curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, method.c_str());
		curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, WriteBody);
		curl_easy_setopt(easy, CURLOPT_WRITEDATA, &response.body);
		curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, WriteHeader);
		curl_easy_setopt(easy, CURLOPT_HEADERDATA, &response.headers);
		curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, static_cast<long>(timeout_ms));
		curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
		curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, 1L);
		curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1L);
		curl_easy_setopt(easy, CURLOPT_MAXREDIRS, 5L);
		curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
		curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, "");
		curl_easy_setopt(easy, CURLOPT_USERAGENT, "duckdb-databricks");
		if (allow_interrupt) {
			curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 0L);
			curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION, Progress);
			curl_easy_setopt(easy, CURLOPT_XFERINFODATA, &context);
		} else {
			curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 1L);
		}
		if (!ca_cert.empty() && ca_cert != "system") {
			curl_easy_setopt(easy, CURLOPT_CAINFO, ca_cert.c_str());
		}
		if (!body.empty() || method == "POST") {
			curl_easy_setopt(easy, CURLOPT_POSTFIELDS, body.c_str());
			curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
		}
		curl_slist *headers = nullptr;
		if (!content_type.empty()) {
			headers = curl_slist_append(headers, ("Content-Type: " + content_type).c_str());
		}
		if (!authorization_header.empty()) {
			headers = curl_slist_append(headers, ("Authorization: " + authorization_header).c_str());
		}
		for (auto &header : extra_headers) {
			headers = curl_slist_append(headers, (header.first + ": " + header.second).c_str());
		}
		if (headers) {
			curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers);
		}
		auto code = curl_easy_perform(easy);
		if (headers) {
			curl_slist_free_all(headers);
		}
		if (allow_interrupt && context.IsInterrupted()) {
			throw InterruptException();
		}
		if (code == CURLE_ABORTED_BY_CALLBACK) {
			throw InterruptException();
		}
		if (code != CURLE_OK) {
			response.transport_error = true;
			response.curl_error = curl_easy_strerror(code);
			last = std::move(response);
			if (RetryableCurl(code) && attempt < retries) {
				SleepOrInterrupt(context, BackoffMs(attempt, last.headers), allow_interrupt);
				continue;
			}
			throw IOException("Databricks transport error during %s: %s", step, last.curl_error);
		}
		long status_code = 0; // NOLINT(google-runtime-int) curl writes a long
		curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status_code);
		response.status = status_code;
		if ((response.status == 429 || response.status == 503) && attempt < retries) {
			auto delay = BackoffMs(attempt, response.headers);
			last = std::move(response);
			SleepOrInterrupt(context, delay, allow_interrupt);
			continue;
		}
		return response;
	}
}

} // namespace duckdb

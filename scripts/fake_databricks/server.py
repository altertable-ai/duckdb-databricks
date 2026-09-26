#!/usr/bin/env python3
"""Scriptable stand-in for the Databricks SQL Statement Execution API.

Records every statement. Tests read the log back with databricks_query(db, '__recorded__').
SQL text selects a behavior: __pending__, __fail__, __429__, __503__, __401__, __expire__,
__403__, __multi__, __slow__, __recorded__, __stats__.
"""

import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.ipc as ipc

import state
from engine import apply_query, apply_statement, json_table, metadata_result, qualified_table

HOST = "127.0.0.1"

def arrow_stream(table):
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def column_array(name, rows):
    if name == "const1":
        return pa.array([1] * len(rows), type=pa.int32()), "v"
    if name == "null_bigint":
        return pa.array([None] * len(rows), type=pa.int64()), "rowid"
    if name == "null_int":
        return pa.array([None] * len(rows), type=pa.int32()), "void"
    if name == "id":
        return pa.array([row["id"] for row in rows], type=pa.int32()), "id"
    if name == "region":
        return pa.array([row["region"] for row in rows], type=pa.string()), "region"
    if name == "amount":
        return pa.array([row["amount"] for row in rows], type=pa.decimal128(10, 2)), "amount"
    if name == "note":
        return pa.array([row["note"] for row in rows], type=pa.string()), "note"
    if name == "statement":
        return pa.array([row["statement"] for row in rows], type=pa.string()), "statement"
    if name == "token_requests":
        return pa.array([row["token_requests"] for row in rows], type=pa.int64()), "token_requests"
    if name == "http_429":
        return pa.array([row["http_429"] for row in rows], type=pa.int64()), "http_429"
    if name == "http_503":
        return pa.array([row["http_503"] for row in rows], type=pa.int64()), "http_503"
    if name == "cancels":
        return pa.array([row["cancels"] for row in rows], type=pa.int64()), "cancels"
    if name == "n":
        return pa.array([row["n"] for row in rows], type=pa.int32()), "n"
    return pa.array([row.get(name) for row in rows], type=pa.string()), name


def table_for(names, rows):
    arrays = []
    fields = []
    for name in names:
        array, field_name = column_array(name, rows)
        arrays.append(array)
        fields.append(field_name)
    return pa.table(arrays, names=fields)


def parse_select(sql):
    upper = sql.upper()
    from_orders = ".`ORDERS`" in upper or "`ORDERS`" in upper
    start = upper.find("SELECT")
    frm = upper.find(" FROM ")
    if start < 0 or frm < 0:
        return ["n"], False
    select = sql[start + 6 : frm]
    parts = []
    depth = 0
    current = []
    for ch in select:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    names = []
    for part in parts:
        compact = " ".join(part.split()).upper()
        if compact == "1":
            names.append("const1")
        elif "AS BIGINT" in compact:
            names.append("null_bigint")
        elif "AS INT" in compact:
            names.append("null_int")
        elif part.startswith("`") and "`" in part[1:]:
            end = part.find("`", 1)
            names.append(part[1:end].replace("``", "`"))
        else:
            names.append("n")
    if not names:
        names = ["const1"]
    return names, from_orders


class ServerState:
    def __init__(self, port):
        self.port = port
        self.lock = threading.Lock()
        self.recorded = []
        self.statements = {}
        self.chunks = {}
        self.token_requests = 0
        self.http_429 = 0
        self.http_503 = 0
        self.cancels = 0
        self.seen_429 = set()
        self.seen_503 = set()
        self.seen_401 = set()
        self.schemas = {"sales", "default", "information_schema"}
        self.tables = {}

    def base(self):
        return f"http://{HOST}:{self.port}"



def arrow_schema(names):
    mapping = {
        "const1": ("INT", "v"),
        "null_bigint": ("BIGINT", "rowid"),
        "null_int": ("INT", "void"),
        "id": ("INT", "id"),
        "region": ("STRING", "region"),
        "amount": ("DECIMAL(10,2)", "amount"),
        "note": ("VARIANT", "note"),
        "statement": ("STRING", "statement"),
        "token_requests": ("BIGINT", "token_requests"),
        "http_429": ("BIGINT", "http_429"),
        "http_503": ("BIGINT", "http_503"),
        "cancels": ("BIGINT", "cancels"),
        "n": ("INT", "n"),
    }
    columns = []
    for index, name in enumerate(names):
        type_text, field = mapping.get(name, ("STRING", name))
        columns.append(
            {"name": field, "type_text": type_text, "type_name": type_text.split("(")[0], "position": index}
        )
    return columns


def make_links(statement_id, chunk_index, payload, expired=False, forbidden=False):
    token = uuid.uuid4().hex
    state.STATE.chunks[token] = {
        "body": payload,
        "forbidden_once": forbidden,
        "require_header": ("X-Databricks-Test", "1") if chunk_index == 0 else None,
    }
    return [
        {
            "chunk_index": chunk_index,
            "row_offset": 0,
            "row_count": 1,
            "byte_count": len(payload),
            "external_link": f"{state.STATE.base()}/chunk/{token}",
            "expiration": "2000-01-01T00:00:00.000Z" if expired else "2099-01-01T00:00:00.000Z",
            "http_headers": {"X-Databricks-Test": "1"} if chunk_index == 0 else {},
        }
    ]



def scan_payloads(sql):
    if sql.strip() == "__recorded__":
        rows = [{"statement": text} for text in list(state.STATE.recorded)]
        return ["statement"], [table_for(["statement"], rows)]
    if sql.strip() == "__stats__":
        row = [
            {
                "token_requests": state.STATE.token_requests,
                "http_429": state.STATE.http_429,
                "http_503": state.STATE.http_503,
                "cancels": state.STATE.cancels,
            }
        ]
        names = ["token_requests", "http_429", "http_503", "cancels"]
        return names, [table_for(names, row)]
    names, from_orders = parse_select(sql)
    located = qualified_table(sql)
    if located and located[1] and not (located[0].lower() == "sales" and located[1].lower() == "orders"):
        table = state.STATE.tables.get(located)
        rows = apply_query(sql, list(table["rows"]) if table else [])
        if rows and names == ["const1"]:
            rows = [{} for _ in rows]
        return names, ([table_for(names, rows)] if rows else [])
    source = state.ORDERS if from_orders else [{"n": 1}]
    if not from_orders and names == ["n"]:
        rows = source
    elif not from_orders and names == ["const1"]:
        rows = [{}]
    else:
        rows = source if from_orders else [{}]
    if from_orders:
        rows = apply_query(sql, rows)
    if "__multi__" in sql and len(rows) > 1:
        return names, [table_for(names, rows[:2]), table_for(names, rows[2:])]
    return names, [table_for(names, rows)]


def succeeded_arrow(statement_id, sql, expired=False, forbidden=False):
    names, tables = scan_payloads(sql)
    chunks = []
    links = []
    for index, table in enumerate(tables):
        payload = arrow_stream(table)
        statement = state.STATE.statements.get(statement_id)
        if statement is not None:
            statement.setdefault("payloads", {})[index] = payload
        chunks.append({"chunk_index": index, "row_offset": 0, "row_count": table.num_rows, "byte_count": len(payload)})
        if index == 0:
            links = make_links(statement_id, index, payload, expired=expired, forbidden=forbidden)
    body = {
        "statement_id": statement_id,
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {"column_count": len(names), "columns": arrow_schema(names)},
            "total_chunk_count": len(tables),
            "chunks": chunks,
            "total_row_count": sum(table.num_rows for table in tables),
            "total_byte_count": sum(chunk["byte_count"] for chunk in chunks),
        },
        "result": {"chunk_index": 0, "row_offset": 0, "row_count": chunks[0]["row_count"] if chunks else 0, "external_links": links},
    }
    return body


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        return json.loads(raw.decode())

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except Exception as exc:
            try:
                self._send(500, {"error_code": "SERVER_ERROR", "message": str(exc)})
            except Exception:
                pass

    def _send(self, status, payload, content_type="application/json", extra_headers=None):
        if isinstance(payload, str):
            data = payload.encode()
        elif isinstance(payload, bytes):
            data = payload
        else:
            data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def _authorized(self):
        header = self.headers.get("Authorization", "")
        return header.startswith("Bearer ") and len(header) > 7

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/oidc/v1/token":
            length = int(self.headers.get("Content-Length", "0") or 0)
            self.rfile.read(length)
            with state.STATE.lock:
                state.STATE.token_requests += 1
            self._send(200, {"access_token": f"access-{state.STATE.token_requests}", "expires_in": 3600, "token_type": "Bearer"})
            return
        if not self._authorized():
            self._send(401, {"error_code": "UNAUTHORIZED", "message": "missing token"})
            return
        if parsed.path == "/api/2.0/sql/statements":
            body = self._read_json()
            self._submit(body)
            return
        if parsed.path.endswith("/cancel"):
            statement_id = parsed.path.split("/")[-2]
            with state.STATE.lock:
                state.STATE.cancels += 1
                statement = state.STATE.statements.get(statement_id)
                if statement:
                    statement["state"] = "CANCELED"
            self._send(200, {"statement_id": statement_id, "status": {"state": "CANCELED"}})
            return
        self._send(404, {"error_code": "NOT_FOUND", "message": parsed.path})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/chunk/"):
            token = parsed.path.split("/")[-1]
            chunk = state.STATE.chunks.get(token)
            if not chunk:
                self._send(404, {"error_code": "NOT_FOUND", "message": "unknown chunk"})
                return
            if chunk.get("forbidden_once"):
                chunk["forbidden_once"] = False
                self._send(403, {"error_code": "FORBIDDEN", "message": "link expired"})
                return
            header = chunk.get("require_header")
            if header and self.headers.get(header[0]) != header[1]:
                self._send(400, {"error_code": "BAD_REQUEST", "message": "missing link header"})
                return
            self._send(200, chunk["body"], "application/octet-stream")
            return
        if not self._authorized():
            self._send(401, {"error_code": "UNAUTHORIZED", "message": "missing token"})
            return
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 5 and parts[:4] == ["api", "2.0", "sql", "statements"] and parts[-2:][0:1] != ["result"]:
            if "result" in parts and "chunks" in parts:
                statement_id = parts[4]
                chunk_index = int(parts[-1])
                self._chunk(statement_id, chunk_index)
                return
            statement_id = parts[4]
            self._poll(statement_id)
            return
        self._send(404, {"error_code": "NOT_FOUND", "message": parsed.path})

    def _submit(self, body):
        sql = body.get("statement", "")
        affected = None
        with state.STATE.lock:
            state.STATE.recorded.append(sql)
            affected = apply_statement(sql)
            if "__429__" in sql and sql not in state.STATE.seen_429:
                state.STATE.seen_429.add(sql)
                state.STATE.http_429 += 1
                self._send(429, {"error_code": "TEMPORARILY_UNAVAILABLE", "message": "slow down"}, extra_headers={"Retry-After": "0"})
                return
            if "__503__" in sql and sql not in state.STATE.seen_503:
                state.STATE.seen_503.add(sql)
                state.STATE.http_503 += 1
                self._send(503, {"error_code": "TEMPORARILY_UNAVAILABLE", "message": "unavailable"}, extra_headers={"Retry-After": "0"})
                return
            if "__401__" in sql and sql not in state.STATE.seen_401:
                state.STATE.seen_401.add(sql)
                self._send(401, {"error_code": "UNAUTHORIZED", "message": "token expired"})
                return
        # 429 response above already sent, but Retry-After was not set because _send ended headers.
        # Re-issue is too late. Set Retry-After by handling 429 before _send. Fixed below for the
        # first version by sending Retry-After inside a dedicated path. See _send_status.
        statement_id = str(uuid.uuid4())
        statement = {
            "sql": sql,
            "polls": 0,
            "state": "SUCCEEDED",
            "payloads": {},
            "parameters": body.get("parameters") or [],
            "format": body.get("format"),
        }
        if "__fail__" in sql:
            statement["state"] = "FAILED"
            body_out = {
                "statement_id": statement_id,
                "status": {"state": "FAILED", "error": {"error_code": "TEST_ERROR", "message": "boom"}},
            }
        elif "__slow__" in sql or "__pending__" in sql:
            statement["state"] = "PENDING"
            body_out = {"statement_id": statement_id, "status": {"state": "PENDING"}}
        elif body.get("format") == "JSON_ARRAY" or "information_schema" in sql.lower():
            meta = metadata_result(sql, statement["parameters"])
            if meta is None and affected is not None:
                meta = json_table([("num_affected_rows", "BIGINT")], [[affected]])
            if meta is None:
                meta = json_table([("n", "INT")], [[1]])
            body_out = {"statement_id": statement_id, "status": {"state": "SUCCEEDED"}, **meta}
        else:
            expired = "__expire__" in sql
            forbidden = "__403__" in sql
            state.STATE.statements[statement_id] = statement
            body_out = succeeded_arrow(statement_id, sql, expired=expired, forbidden=forbidden)
            statement["body"] = body_out
            state.STATE.statements[statement_id] = statement
            self._send(200, body_out)
            return
        statement["body"] = body_out
        state.STATE.statements[statement_id] = statement
        self._send(200, body_out)

    def _poll(self, statement_id):
        statement = state.STATE.statements.get(statement_id)
        if not statement:
            self._send(404, {"error_code": "NOT_FOUND", "message": "unknown statement"})
            return
        statement["polls"] += 1
        if statement["state"] == "CANCELED":
            self._send(
                200,
                {
                    "statement_id": statement_id,
                    "status": {"state": "CANCELED", "error": {"error_code": "CANCELED", "message": "canceled"}},
                },
            )
            return
        if "__slow__" in statement["sql"] and statement["state"] == "PENDING":
            self._send(200, {"statement_id": statement_id, "status": {"state": "PENDING"}})
            return
        if "__pending__" in statement["sql"] and statement["polls"] == 1:
            self._send(200, {"statement_id": statement_id, "status": {"state": "PENDING"}})
            return
        if statement["state"] == "FAILED":
            self._send(200, statement["body"])
            return
        if "body" not in statement or statement["body"].get("status", {}).get("state") == "PENDING":
            expired = "__expire__" in statement["sql"]
            forbidden = "__403__" in statement["sql"]
            statement["body"] = succeeded_arrow(statement_id, statement["sql"], expired=expired, forbidden=forbidden)
            statement["state"] = "SUCCEEDED"
        self._send(200, statement["body"])

    def _chunk(self, statement_id, chunk_index):
        statement = state.STATE.statements.get(statement_id)
        if not statement:
            self._send(404, {"error_code": "NOT_FOUND", "message": "unknown statement"})
            return
        payload = statement.get("payloads", {}).get(chunk_index)
        if payload is None:
            self._send(404, {"error_code": "NOT_FOUND", "message": "unknown chunk"})
            return
        links = make_links(statement_id, chunk_index, payload, expired=False, forbidden=False)
        self._send(200, {"external_links": links, "chunk_index": chunk_index})


def main():
    server = ThreadingHTTPServer((HOST, 0), Handler)
    state.STATE = ServerState(server.server_address[1])
    print(state.STATE.port, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

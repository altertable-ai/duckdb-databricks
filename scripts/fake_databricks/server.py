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
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.ipc as ipc

HOST = "127.0.0.1"
ORDERS = [
    {"id": 1, "region": "east", "amount": Decimal("10.50"), "note": '{"a": 1}'},
    {"id": 2, "region": "west", "amount": Decimal("20.00"), "note": None},
    {"id": 3, "region": None, "amount": None, "note": "null"},
]


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


STATE = None


def json_table(columns, rows):
    return {
        "manifest": {
            "format": "JSON_ARRAY",
            "schema": {
                "column_count": len(columns),
                "columns": [
                    {
                        "name": name,
                        "type_text": type_text,
                        "type_name": type_text.split("(")[0],
                        "position": index,
                    }
                    for index, (name, type_text) in enumerate(columns)
                ],
            },
            "total_chunk_count": 1 if rows else 0,
            "total_row_count": len(rows),
        },
        "result": {"chunk_index": 0, "row_count": len(rows), "row_offset": 0, "data_array": rows},
    }


def qualified_table(sql):
    match = re.search(r"(?:FROM|INTO|TABLE|SCHEMA)\s+`([^`]+)`\.`([^`]+)`(?:\.`([^`]+)`)?", sql, re.I)
    if not match:
        return None
    if match.group(3):
        return match.group(2), match.group(3)
    return match.group(2), None


def parse_tuples(body):
    tuples = []
    i = 0
    while i < len(body):
        while i < len(body) and body[i] in " \t\r\n,":
            i += 1
        if i >= len(body) or body[i] != "(":
            break
        i += 1
        values = []
        current = []
        quote = None
        depth = 1
        while i < len(body) and depth:
            ch = body[i]
            if quote:
                current.append(ch)
                if ch == "\\" and i + 1 < len(body):
                    current.append(body[i + 1])
                    i += 2
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch == "'":
                quote = ch
                current.append(ch)
                i += 1
                continue
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    values.append("".join(current).strip())
                    tuples.append(values)
                    i += 1
                    break
                current.append(ch)
            elif ch == "," and depth == 1:
                values.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
            i += 1
    return tuples


def literal_to_python(text):
    text = text.strip()
    if text.upper() == "NULL" or text.upper().startswith("CAST(NULL"):
        return None
    if text.startswith("'") or text.startswith("DATE'") or text.startswith("TIMESTAMP"):
        quoted = text[text.find("'") :]
        return unquote_sql(quoted)
    if text.endswith("BD"):
        return Decimal(text[:-2])
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return text


def apply_statement(sql):
    upper = sql.lstrip().upper()
    if upper.startswith("CREATE SCHEMA"):
        match = re.search(r"CREATE SCHEMA(?: IF NOT EXISTS)? `[^`]+`\.`([^`]+)`", sql, re.I)
        if match:
            STATE.schemas.add(match.group(1))
        return
    if upper.startswith("DROP SCHEMA"):
        match = re.search(r"DROP SCHEMA(?: IF EXISTS)? `[^`]+`\.`([^`]+)`", sql, re.I)
        if match and match.group(1) not in ("sales", "default", "information_schema"):
            STATE.schemas.discard(match.group(1))
            STATE.tables = {key: value for key, value in STATE.tables.items() if key[0] != match.group(1)}
        return
    if upper.startswith("CREATE ") and "TABLE" in upper.split("TBLPROPERTIES", 1)[0]:
        match = re.search(
            r"CREATE(?: OR REPLACE)? TABLE(?: IF NOT EXISTS)? `[^`]+`\.`([^`]+)`\.`([^`]+)`\s*\(",
            sql,
            re.I,
        )
        if not match:
            return
        schema, table = match.group(1), match.group(2)
        body_start = sql.find("(", match.end() - 1)
        props = sql.upper().find("TBLPROPERTIES")
        end = sql.rfind(")", body_start, props if props > 0 else len(sql))
        body = sql[body_start + 1 : end]
        columns = []
        for part in split_top(body, ","):
            part = part.strip()
            if not part or part.upper().startswith("CONSTRAINT"):
                continue
            col = re.match(r"`([^`]+)`\s+(.+)", part)
            if not col:
                continue
            nullable = "NO" if "NOT NULL" in part.upper() else "YES"
            default = None
            default_at = part.upper().find(" DEFAULT ")
            if default_at >= 0:
                default = part[default_at + len(" DEFAULT ") :].strip()
            type_text = col.group(2)
            for marker in (" NOT NULL", " DEFAULT "):
                at = type_text.upper().find(marker)
                if at >= 0:
                    type_text = type_text[:at]
            columns.append((col.group(1), type_text.strip(), nullable, default))
        STATE.tables[(schema, table)] = {"columns": columns, "rows": []}
        STATE.schemas.add(schema)
        return
    if upper.startswith("DROP TABLE"):
        located = qualified_table(sql)
        if located and located[1]:
            STATE.tables.pop(located, None)
        return
    if upper.startswith("INSERT INTO"):
        located = qualified_table(sql)
        if not located or not located[1]:
            return
        values_at = sql.upper().find(" VALUES ")
        if values_at < 0:
            return
        columns_at = sql.find("(")
        column_body = sql[columns_at + 1 : sql.find(")", columns_at)]
        columns = [part.strip().strip("`") for part in split_top(column_body, ",")]
        rows = []
        for values in parse_tuples(sql[values_at + len(" VALUES ") :]):
            row = {column: literal_to_python(value) for column, value in zip(columns, values)}
            rows.append(row)
        if located[1].lower() == "orders" and located[0].lower() == "sales":
            for row in rows:
                ORDERS.append(
                    {
                        "id": row.get("id"),
                        "region": row.get("region"),
                        "amount": row.get("amount"),
                        "note": row.get("note"),
                    }
                )
            return
        table = STATE.tables.get(located)
        if table is not None:
            for row in rows:
                filled = {}
                for column_name, _type_text, _nullable, default in table["columns"]:
                    if column_name in row:
                        filled[column_name] = row[column_name]
                    elif default:
                        filled[column_name] = literal_to_python(default)
                    else:
                        filled[column_name] = None
                table["rows"].append(filled)


def metadata_result(sql, parameters):
    params = {item.get("name"): item.get("value") for item in parameters}
    lower = sql.lower()
    if "information_schema.catalogs" in lower:
        catalog = params.get("catalog", "")
        rows = [[catalog]] if catalog == "main" else []
        return json_table([("catalog_name", "STRING")], rows)
    if "information_schema.schemata" in lower:
        rows = [[name, None] for name in sorted(STATE.schemas)]
        return json_table([("schema_name", "STRING"), ("comment", "STRING")], rows)
    if "information_schema.tables" in lower:
        schema = params.get("schema", "")
        rows = [["orders", "TABLE", None]] if schema == "sales" else []
        for (table_schema, table_name), _table in STATE.tables.items():
            if table_schema == schema:
                rows.append([table_name, "TABLE", None])
        return json_table([("table_name", "STRING"), ("table_type", "STRING"), ("comment", "STRING")], rows)
    if "information_schema.columns" in lower:
        schema = params.get("schema", "")
        rows = []
        if schema == "sales":
            rows = [
                ["orders", "id", 0, "INT", "NO", None, None],
                ["orders", "region", 1, "STRING", "YES", None, None],
                ["orders", "amount", 2, "DECIMAL(10,2)", "YES", None, None],
                ["orders", "note", 3, "VARIANT", "YES", None, None],
            ]
        for (table_schema, table_name), table in STATE.tables.items():
            if table_schema != schema:
                continue
            for position, (column_name, type_text, nullable, default) in enumerate(table["columns"]):
                rows.append([table_name, column_name, position, type_text, nullable, default, None])
        return json_table(
            [
                ("table_name", "STRING"),
                ("column_name", "STRING"),
                ("ordinal_position", "INT"),
                ("full_data_type", "STRING"),
                ("is_nullable", "STRING"),
                ("column_default", "STRING"),
                ("comment", "STRING"),
            ],
            rows,
        )
    return None


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
    STATE.chunks[token] = {
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
            "external_link": f"{STATE.base()}/chunk/{token}",
            "expiration": "2000-01-01T00:00:00.000Z" if expired else "2099-01-01T00:00:00.000Z",
            "http_headers": {"X-Databricks-Test": "1"} if chunk_index == 0 else {},
        }
    ]


def split_top(pred, sep):
    parts = []
    depth = 0
    quote = None
    current = []
    i = 0
    while i < len(pred):
        ch = pred[i]
        if quote:
            current.append(ch)
            if ch == "\\" and quote == "'" and i + 1 < len(pred):
                current.append(pred[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in "'`":
            quote = ch
            current.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth == 0 and pred[i : i + len(sep)].upper() == sep.upper():
            parts.append("".join(current).strip())
            current = []
            i += len(sep)
            continue
        current.append(ch)
        i += 1
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def unquote_sql(text):
    text = text.strip()
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        body = text[1:-1]
        return body.replace("\\'", "'").replace("\\\\", "\\")
    if text.upper().endswith("BD"):
        text = text[:-2]
    return text


def cell_value(row, name):
    if name not in row:
        raise ValueError(f"unknown filter column {name}")
    return row[name]


def compare_values(left, op, right_text):
    if left is None:
        return False
    literal = unquote_sql(right_text)
    if isinstance(left, str):
        right = literal
    else:
        right = type(left)(literal) if not isinstance(left, float) else float(literal)
        if isinstance(left, int) and not isinstance(left, bool):
            right = int(float(literal))
    if op == "=":
        return left == right
    if op in ("!=", "<>"):
        return left != right
    if op == "<":
        return left < right
    if op == ">":
        return left > right
    if op == "<=":
        return left <= right
    if op == ">=":
        return left >= right
    raise ValueError(f"unknown comparison {op}")


def eval_cmp(row, pred):
    pred = pred.strip()
    if pred == "1 = 0":
        return False
    match = re.match(r"^`([^`]+)`\s+IS\s+NOT\s+NULL$", pred, re.I)
    if match:
        return cell_value(row, match.group(1)) is not None
    match = re.match(r"^`([^`]+)`\s+IS\s+NULL$", pred, re.I)
    if match:
        return cell_value(row, match.group(1)) is None
    match = re.match(r"^`([^`]+)`\s+NOT\s+IN\s*\((.*)\)$", pred, re.I)
    if match:
        return not any(compare_values(cell_value(row, match.group(1)), "=", item) for item in split_top(match.group(2), ","))
    match = re.match(r"^`([^`]+)`\s+IN\s*\((.*)\)$", pred, re.I)
    if match:
        return any(compare_values(cell_value(row, match.group(1)), "=", item) for item in split_top(match.group(2), ","))
    match = re.match(r"^`([^`]+)`\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$", pred, re.I)
    if match:
        value = cell_value(row, match.group(1))
        return compare_values(value, ">=", match.group(2)) and compare_values(value, "<=", match.group(3))
    match = re.match(r"^`([^`]+)`\s+LIKE\s+('.+')\s+ESCAPE\s+('.+')$", pred, re.I)
    if match:
        value = cell_value(row, match.group(1))
        if value is None:
            return False
        pattern = unquote_sql(match.group(2))
        if not pattern.endswith("%") or "_" in pattern[:-1] or "%" in pattern[:-1]:
            raise ValueError(f"unsupported LIKE {pred}")
        return str(value).startswith(pattern[:-1])
    match = re.match(r"^`([^`]+)`\s*(=|!=|<>|>=|<=|>|<)\s*(.+)$", pred)
    if match:
        return compare_values(cell_value(row, match.group(1)), match.group(2), match.group(3))
    raise ValueError(f"unsupported predicate {pred}")


def eval_pred(row, pred):
    pred = pred.strip()
    while pred.startswith("(") and pred.endswith(")") and len(split_top(pred[1:-1], " OR ")) >= 1:
        inner = pred[1:-1].strip()
        if inner.count("(") == inner.count(")"):
            pred = inner
            continue
        break
    if re.match(r"^`[^`]+`\s+BETWEEN\s+.+\s+AND\s+.+$", pred, re.I):
        return eval_cmp(row, pred)
    ors = split_top(pred, " OR ")
    if len(ors) > 1:
        return any(eval_pred(row, part) for part in ors)
    ands = split_top(pred, " AND ")
    if len(ands) > 1:
        return all(eval_pred(row, part) for part in ands)
    return eval_cmp(row, pred)


def clause_span(sql):
    upper = sql.upper()
    where_at = upper.find(" WHERE ")
    order_at = upper.find(" ORDER BY ")
    limit_at = upper.find(" LIMIT ")
    pred = ""
    if where_at >= 0:
        end = len(sql)
        for marker in (order_at, limit_at):
            if marker > where_at:
                end = min(end, marker)
        pred = sql[where_at + len(" WHERE ") : end].strip()
    order_keys = []
    if order_at >= 0:
        end = limit_at if limit_at > order_at else len(sql)
        body = sql[order_at + len(" ORDER BY ") : end]
        for key in split_top(body, ","):
            match = re.match(r"`([^`]+)`\s*(ASC|DESC)?\s*(NULLS FIRST|NULLS LAST)?", key.strip(), re.I)
            if not match:
                raise ValueError(f"unsupported ORDER BY {key}")
            order_keys.append(
                (
                    match.group(1),
                    (match.group(2) or "ASC").upper() == "DESC",
                    (match.group(3) or "").upper() == "NULLS FIRST",
                )
            )
    limit = None
    offset = 0
    if limit_at >= 0:
        match = re.search(r"LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?", sql[limit_at:], re.I)
        if not match:
            raise ValueError(f"unsupported LIMIT in {sql}")
        limit = int(match.group(1))
        if match.group(2):
            offset = int(match.group(2))
    return pred, order_keys, limit, offset


def apply_query(sql, rows):
    pred, order_keys, limit, offset = clause_span(sql)
    if pred:
        rows = [row for row in rows if eval_pred(row, pred)]
    if order_keys:

        class _Reverse:
            def __init__(self, value):
                self.value = value

            def __lt__(self, other):
                return self.value > other.value

        def sort_key(row):
            key = []
            for name, descending, nulls_first in order_keys:
                value = cell_value(row, name)
                missing = value is None
                key.append(0 if missing == nulls_first else 1)
                if missing:
                    key.append(0)
                elif descending:
                    key.append(_Reverse(value))
                else:
                    key.append(value)
            return key

        rows = sorted(rows, key=sort_key)
    if limit is not None:
        rows = rows[offset : offset + limit]
    return rows


def scan_payloads(sql):
    if sql.strip() == "__recorded__":
        rows = [{"statement": text} for text in list(STATE.recorded)]
        return ["statement"], [table_for(["statement"], rows)]
    if sql.strip() == "__stats__":
        row = [
            {
                "token_requests": STATE.token_requests,
                "http_429": STATE.http_429,
                "http_503": STATE.http_503,
                "cancels": STATE.cancels,
            }
        ]
        names = ["token_requests", "http_429", "http_503", "cancels"]
        return names, [table_for(names, row)]
    names, from_orders = parse_select(sql)
    located = qualified_table(sql)
    if located and located[1] and not (located[0].lower() == "sales" and located[1].lower() == "orders"):
        table = STATE.tables.get(located)
        rows = apply_query(sql, list(table["rows"]) if table else [])
        if rows and names == ["const1"]:
            rows = [{} for _ in rows]
        return names, ([table_for(names, rows)] if rows else [])
    source = ORDERS if from_orders else [{"n": 1}]
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
        statement = STATE.statements.get(statement_id)
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
            with STATE.lock:
                STATE.token_requests += 1
            self._send(200, {"access_token": f"access-{STATE.token_requests}", "expires_in": 3600, "token_type": "Bearer"})
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
            with STATE.lock:
                STATE.cancels += 1
                statement = STATE.statements.get(statement_id)
                if statement:
                    statement["state"] = "CANCELED"
            self._send(200, {"statement_id": statement_id, "status": {"state": "CANCELED"}})
            return
        self._send(404, {"error_code": "NOT_FOUND", "message": parsed.path})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/chunk/"):
            token = parsed.path.split("/")[-1]
            chunk = STATE.chunks.get(token)
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
        with STATE.lock:
            STATE.recorded.append(sql)
            apply_statement(sql)
            if "__429__" in sql and sql not in STATE.seen_429:
                STATE.seen_429.add(sql)
                STATE.http_429 += 1
                self._send(429, {"error_code": "TEMPORARILY_UNAVAILABLE", "message": "slow down"}, extra_headers={"Retry-After": "0"})
                return
            if "__503__" in sql and sql not in STATE.seen_503:
                STATE.seen_503.add(sql)
                STATE.http_503 += 1
                self._send(503, {"error_code": "TEMPORARILY_UNAVAILABLE", "message": "unavailable"}, extra_headers={"Retry-After": "0"})
                return
            if "__401__" in sql and sql not in STATE.seen_401:
                STATE.seen_401.add(sql)
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
            if meta is None:
                meta = json_table([("n", "INT")], [[1]])
            body_out = {"statement_id": statement_id, "status": {"state": "SUCCEEDED"}, **meta}
        else:
            expired = "__expire__" in sql
            forbidden = "__403__" in sql
            STATE.statements[statement_id] = statement
            body_out = succeeded_arrow(statement_id, sql, expired=expired, forbidden=forbidden)
            statement["body"] = body_out
            STATE.statements[statement_id] = statement
            self._send(200, body_out)
            return
        statement["body"] = body_out
        STATE.statements[statement_id] = statement
        self._send(200, body_out)

    def _poll(self, statement_id):
        statement = STATE.statements.get(statement_id)
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
        statement = STATE.statements.get(statement_id)
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
    global STATE
    server = ThreadingHTTPServer((HOST, 0), Handler)
    STATE = ServerState(server.server_address[1])
    print(STATE.port, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

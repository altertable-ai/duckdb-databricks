"""SQL the fake warehouse applies locally: DDL, DML, metadata, and scan filters."""

import re
from decimal import Decimal

import state

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


def table_rows(schema, table):
    if schema.lower() == "sales" and table.lower() == "orders":
        return state.ORDERS
    for key, value in state.STATE.tables.items():
        if key[0].lower() == schema.lower() and key[1].lower() == table.lower():
            return value["rows"]
    return None


def replace_rows(schema, table, rows):
    if schema.lower() == "sales" and table.lower() == "orders":
        state.ORDERS[:] = rows
        return
    for key, value in state.STATE.tables.items():
        if key[0].lower() == schema.lower() and key[1].lower() == table.lower():
            value["rows"] = rows
            return


def dml_target(sql):
    match = re.search(r"(?:UPDATE|FROM|TABLE)\s+`([^`]+)`\.`([^`]+)`\.`([^`]+)`", sql, re.I)
    if not match:
        return None
    return match.group(2), match.group(3)


def strip_cast(text):
    match = re.fullmatch(r"CAST\((.*) AS [A-Za-z0-9_(), ]+\)", text.strip(), re.I)
    if match:
        return match.group(1).strip()
    return text.strip()


def apply_assignments(rows, body):
    assignments = []
    for part in split_top(body, ","):
        match = re.match(r"`([^`]+)`\s*=\s*(.+)", part.strip(), re.S)
        if not match:
            continue
        value = strip_cast(match.group(2))
        if value.upper() == "DEFAULT" or not (
            value.startswith("'") or re.fullmatch(r"-?\d+(?:BD)?", value) or value.upper() in ("TRUE", "FALSE", "NULL")
        ):
            continue
        assignments.append((match.group(1), literal_to_python(value)))
    for row in rows:
        for column, value in assignments:
            row[column] = value


def apply_dml(sql):
    upper = sql.lstrip().upper()
    if not upper.startswith(("UPDATE ", "DELETE ", "TRUNCATE ")):
        return None
    located = dml_target(sql)
    if not located:
        return 0
    rows = table_rows(*located)
    if rows is None:
        return 0
    if upper.startswith("TRUNCATE "):
        count = len(rows)
        replace_rows(*located, [])
        return count
    pred, _order, _limit, _offset = clause_span(sql)
    if upper.startswith("DELETE "):
        if not pred:
            count = len(rows)
            replace_rows(*located, [])
            return count
        kept = []
        removed = 0
        for row in rows:
            if eval_pred(row, pred):
                removed += 1
            else:
                kept.append(row)
        replace_rows(*located, kept)
        return removed
    set_at = upper.find(" SET ")
    where_at = upper.find(" WHERE ")
    if set_at < 0:
        return 0
    body_end = where_at if where_at > set_at else len(sql)
    body = sql[set_at + len(" SET ") : body_end]
    matched = [row for row in rows if not pred or eval_pred(row, pred)]
    apply_assignments(matched, body)
    return len(matched)


def apply_statement(sql):
    upper = sql.lstrip().upper()
    if upper.startswith(("UPDATE ", "DELETE ", "TRUNCATE ")):
        try:
            return apply_dml(sql)
        except Exception:
            return 0
    if upper.startswith("CREATE SCHEMA"):
        match = re.search(r"CREATE SCHEMA(?: IF NOT EXISTS)? `[^`]+`\.`([^`]+)`", sql, re.I)
        if match:
            state.STATE.schemas.add(match.group(1))
        return
    if upper.startswith("DROP SCHEMA"):
        match = re.search(r"DROP SCHEMA(?: IF EXISTS)? `[^`]+`\.`([^`]+)`", sql, re.I)
        if match and match.group(1) not in ("sales", "default", "information_schema"):
            state.STATE.schemas.discard(match.group(1))
            state.STATE.tables = {key: value for key, value in state.STATE.tables.items() if key[0] != match.group(1)}
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
        state.STATE.tables[(schema, table)] = {"columns": columns, "rows": []}
        state.STATE.schemas.add(schema)
        return
    if upper.startswith("DROP TABLE"):
        located = qualified_table(sql)
        if located and located[1]:
            state.STATE.tables.pop(located, None)
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
                state.ORDERS.append(
                    {
                        "id": row.get("id"),
                        "region": row.get("region"),
                        "amount": row.get("amount"),
                        "note": row.get("note"),
                    }
                )
            return
        table = state.STATE.tables.get(located)
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
        rows = [[name, None] for name in sorted(state.STATE.schemas)]
        return json_table([("schema_name", "STRING"), ("comment", "STRING")], rows)
    if "information_schema.tables" in lower and "information_schema.columns" in lower:
        schema = params.get("schema", "")
        definitions = []
        if schema == "sales":
            definitions.append(
                (
                    "orders",
                    "TABLE",
                    None,
                    [
                        ("id", "INT", "NO", None),
                        ("region", "STRING", "YES", None),
                        ("amount", "DECIMAL(10,2)", "YES", None),
                        ("note", "VARIANT", "YES", None),
                    ],
                )
            )
        for (table_schema, table_name), table in state.STATE.tables.items():
            if table_schema == schema:
                definitions.append((table_name, "TABLE", None, table["columns"]))
        rows = []
        for table_name, table_type, table_comment, columns in definitions:
            if not columns:
                rows.append([table_name, table_type, table_comment, None, None, None, None, None, None])
                continue
            for position, (column_name, type_text, nullable, default) in enumerate(columns):
                rows.append(
                    [
                        table_name,
                        table_type,
                        table_comment,
                        column_name,
                        position,
                        type_text,
                        nullable,
                        default,
                        None,
                    ]
                )
        return json_table(
            [
                ("table_name", "STRING"),
                ("table_type", "STRING"),
                ("table_comment", "STRING"),
                ("column_name", "STRING"),
                ("ordinal_position", "INT"),
                ("full_data_type", "STRING"),
                ("is_nullable", "STRING"),
                ("column_default", "STRING"),
                ("column_comment", "STRING"),
            ],
            rows,
        )
    if "information_schema.tables" in lower:
        schema = params.get("schema", "")
        rows = [["orders", "TABLE", None]] if schema == "sales" else []
        for (table_schema, table_name), _table in state.STATE.tables.items():
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
        for (table_schema, table_name), table in state.STATE.tables.items():
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


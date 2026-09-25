# Databricks extension for DuckDB

Query a Databricks SQL warehouse from DuckDB. One `ATTACH` is one Unity Catalog catalog. The extension uses the [SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution) only: it does not read Delta files, and it does not open a Thrift session.

```sql
CREATE SECRET dbx (
    TYPE databricks,
    HOST 'adb-123.azuredatabricks.net',
    WAREHOUSE_ID 'abc123',
    TOKEN 'dapi...'
);
ATTACH 'main' AS db (TYPE databricks, SECRET dbx, SCHEMA 'sales');
SELECT * FROM db.orders;
```

`HOST` may include `https://` or be just the hostname. Set `WAREHOUSE_ID` on the secret or on `ATTACH`. Authentication is a personal access token (`TOKEN`) or a service principal (`CLIENT_ID` and `CLIENT_SECRET`). `TOKEN` together with `CLIENT_ID` is rejected. `CATALOG` on the secret is used when the `ATTACH` path is empty.

`SCHEMA 'sales'` exposes only that schema and makes it the default, so `db.orders` is `sales.orders`. `databricks_query` and `databricks_execute` can still name any schema in the catalog. `information_schema` is hidden.

The first metadata lookup starts the warehouse. A serverless warehouse is usually ready in seconds. A classic warehouse can take several minutes.

## Reading

Scans project the columns DuckDB needs. `COUNT(*)` asks the warehouse for `1` and uses the row count.

```sql
SELECT * FROM databricks_query('db', 'SELECT * FROM main.sales.orders TABLESAMPLE (1 PERCENT)');
```

`databricks_type_mapping(type_text)` returns the DuckDB type name for a Databricks type string.

```sql
SELECT databricks_type_mapping('ARRAY<STRUCT<a: INT, b: STRING>>');
```

## Statements and cache

```sql
CALL databricks_execute('db', 'OPTIMIZE main.sales.orders');
CALL databricks_clear_cache();
```

`databricks_execute` runs one statement in the warehouse, returns `Success`, and clears that database's metadata cache. It is rejected when the database was attached `READ_ONLY`:

```text
Cannot write to a read-only Databricks database
```

`databricks_clear_cache` clears every attached Databricks database.

Schema, table, and column metadata is cached on the attached database until then. `INSERT`, `UPDATE`, `DELETE`, and `CREATE TABLE` from DuckDB syntax are not available yet; send those statements with `databricks_execute`.

Filters, `LIMIT`, and `ORDER BY ... LIMIT` are sent to the warehouse when `dbx_filter_pushdown` and `dbx_order_pushdown` are on. A pushed filter is not checked again in DuckDB. `FLOAT` and `DOUBLE` comparisons, collated strings, and `JSON` stay in DuckDB. `EXPLAIN` shows a pushed `ORDER BY` or `LIMIT` as `Pushed Down`. `EXPLAIN ANALYZE` shows the statement text as `SQL`.

## Settings

| Setting | Default | |
| --- | --- | --- |
| `dbx_filter_pushdown` | `true` | Send filters to the warehouse |
| `dbx_order_pushdown` | `true` | Send `ORDER BY ... LIMIT` to the warehouse |
| `dbx_insert_max_statement_bytes` | `12582912` | Maximum text of one `INSERT`. Hard cap 16 MiB |
| `dbx_http_timeout_ms` | `60000` | Per-request HTTP timeout |
| `dbx_http_retries` | `5` | Retries for HTTP 429, 503, and connection errors |
| `dbx_statement_timeout_ms` | `0` | Cancel a statement that is still running after this many milliseconds. `0` waits |
| `dbx_ca_cert` | `system` | PEM bundle to trust |
| `dbx_debug_show_queries` | `false` | Print each statement text and id |

Secrets redact `TOKEN` and `CLIENT_SECRET`. Errors name the HTTP step (`auth`, `submit`, `poll`, `chunk download`) and do not include authorization headers or the query string of a presigned URL.

## Tests

`make test` runs the sqllogictests that do not need a server.

`make test-fake` starts `scripts/fake_databricks/server.py` and runs `test/sql/fake`. The server needs `pyarrow`.

`make smoke` runs `scripts/test_with_databricks.sh`. Set `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_TEST_CATALOG`, and `DATABRICKS_TOKEN` (or `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`). The script creates `duckdb_test_<run id>` in that catalog and drops it afterwards.

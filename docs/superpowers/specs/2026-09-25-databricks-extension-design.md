# databricks: DuckDB extension for Databricks SQL — Design

- **Date:** 2026-09-25
- **Status:** Approved (brainstorming). Pending the four implementation plans.
- **Goal:** Attach a Databricks Unity Catalog catalog to DuckDB and read and write its tables with plain DuckDB SQL, with every
  query executed by a Databricks SQL warehouse.
- **Model:** `duckdb-clickhouse` (`clickhouse_scanner`) for the DuckDB side (storage extension, catalog, pushdown, writes,
  escape hatches, tests); `hafenkran/duckdb-bigquery` for the dependency setup (curl + Arrow C++ through vcpkg).
- **Not** `duckdb/unity_catalog`: that extension vends storage credentials and reads/writes Delta files itself. This one
  goes through Databricks SQL, so Databricks enforces permissions (row filters, column masks), every securable works
  (views, materialized views, federated tables), and the warehouse does the compute.

## 1. Decisions

| #   | Decision |
| --- | -------- |
| D1  | Transport: the SQL Statement Execution REST API (`/api/2.0/sql/statements`). Thrift/HiveServer2 (large undocumented protocol) and the Go ADBC driver (native Go library packaging) were rejected. |
| D2  | HTTP through libcurl, Arrow decoding through Arrow C++, both from vcpkg, as in duckdb-bigquery. No runtime dependency on `httpfs`. No DuckDB-WASM build. |
| D3  | Extension name `databricks`; `TYPE databricks` for secrets and `ATTACH`; functions `databricks_*`; settings `dbx_*`. |
| D4  | One `ATTACH` = one Databricks catalog. Databricks `catalog.schema.table` maps to DuckDB `db.schema.table`. |
| D5  | Auth: personal access token and OAuth M2M (service principal). U2M and Azure Entra ID are out of scope for v1. |
| D6  | Metadata comes from `information_schema` through the warehouse, not from the Unity Catalog REST API. |
| D7  | Writes use batched `INSERT … VALUES` only (no staging volume). Batches are sized by statement bytes. |
| D8  | Every statement commits on its own. `COMMIT` is a no-op; `ROLLBACK` warns that sent writes are not undone. The API has no sessions, so interactive `BEGIN TRANSACTION` is not used. |
| D9  | Writable by default; `ATTACH … (READ_ONLY)` rejects every write. |
| D10 | v1 scope: reads with pushdown, `INSERT`/`COPY`/CTAS, DDL, `UPDATE`/`DELETE`/`TRUNCATE`, `databricks_query`, `databricks_execute`, `databricks_clear_cache`, `databricks_type_mapping`. |
| D11 | `UPDATE`/`DELETE` are translated to one Databricks statement each, or rejected before anything runs (same approach as ClickHouse D6). |
| D12 | Tests: offline suite against a fake Statement Execution server, plus a live smoke suite against a real workspace. |

## 2. User surface

```sql
INSTALL databricks; LOAD databricks;

CREATE SECRET dbx (TYPE databricks, HOST 'dbc-123.cloud.databricks.com', WAREHOUSE_ID 'abc123', TOKEN 'dapi…');
-- or: CLIENT_ID '…', CLIENT_SECRET '…' instead of TOKEN

ATTACH 'main' AS dbx (TYPE databricks, SECRET dbx [, WAREHOUSE_ID '…'] [, SCHEMA 'sales'] [, READ_ONLY]);

SELECT region, sum(amount) FROM dbx.sales.orders WHERE day >= DATE '2026-09-01' GROUP BY ALL;
INSERT INTO dbx.sales.orders SELECT * FROM read_parquet('orders/*.parquet');
UPDATE dbx.sales.orders SET status = 'void' WHERE id = 42;

SELECT * FROM databricks_query('dbx', 'SELECT * FROM main.sales.orders TABLESAMPLE (1 PERCENT)');
CALL databricks_execute('dbx', 'OPTIMIZE main.sales.orders');
CALL databricks_clear_cache();
SELECT databricks_type_mapping('ARRAY<STRUCT<a: INT, b: STRING>>');
```

**Secret parameters**

| Parameter       | Required               | Description |
| --------------- | ---------------------- | ----------- |
| `HOST`          | yes                    | Workspace host name, with or without `https://` |
| `WAREHOUSE_ID`  | yes (secret or ATTACH) | SQL warehouse that runs every statement |
| `TOKEN`         | one auth method        | Personal access token |
| `CLIENT_ID`     | one auth method        | Service principal application ID (with `CLIENT_SECRET`) |
| `CLIENT_SECRET` | with `CLIENT_ID`       | Service principal OAuth secret |
| `CATALOG`       | no                     | Catalog to attach when the `ATTACH` path is empty |

`TOKEN` together with `CLIENT_ID` is an error. Secret values are redacted in `duckdb_secrets()` and never appear in errors or logs.

**ATTACH**

- The path is the Databricks catalog name. It can be empty only if the secret has a `CATALOG` parameter (optional, same meaning).
- `ATTACH` checks the catalog exists (`system.information_schema.catalogs`) and fails with a clear error otherwise.
- `SCHEMA 'x'` exposes only that schema and makes it the default, so `dbx.orders` means `dbx.x.orders`.
  `databricks_query`/`databricks_execute` still reach everything.
- `information_schema` of the catalog is hidden.

**Settings**

| Setting                          | Default   | Description |
| -------------------------------- | --------- | ----------- |
| `dbx_filter_pushdown`            | `true`    | Send filters to Databricks |
| `dbx_order_pushdown`             | `true`    | Send `LIMIT` and `ORDER BY … LIMIT` |
| `dbx_insert_max_statement_bytes` | `12582912` (12 MiB) | Flush an `INSERT … VALUES` batch at this size (hard cap 16 MiB) |
| `dbx_statement_timeout_ms`       | `0` (none) | Cancel a statement that has not finished after this long |
| `dbx_http_timeout_ms`            | `60000`   | Per-request HTTP timeout |
| `dbx_http_retries`               | `5`       | Retries on 429/503/connection errors, with exponential backoff honouring `Retry-After` |
| `dbx_ca_cert`                    | system    | PEM bundle to trust (needed on some Linux distributions) |
| `dbx_debug_show_queries`         | `false`   | Print every statement sent |

## 3. Architecture

```
src/
  databricks_extension.cpp           registration (secret type, storage extension, functions, settings)
  databricks_secrets.cpp             secret type and validation
  databricks_auth.cpp                PAT / OAuth M2M token provider
  databricks_http.cpp                libcurl wrapper
  databricks_statement.cpp           StatementClient: submit / poll / cancel / chunks
  databricks_arrow.cpp               ArrowChunkReader: IPC stream → DuckDB vectors
  databricks_types.cpp               Databricks type text ↔ DuckDB LogicalType
  databricks_literal.cpp             DuckDB Value → Databricks SQL literal
  databricks_expression.cpp          DuckDB Expression → Databricks SQL (filters, UPDATE/DELETE)
  databricks_scanner.cpp             table scan function (+ pushdown)
  databricks_query.cpp               databricks_query()
  databricks_execute.cpp             databricks_execute()
  databricks_type_mapping_function.cpp
  storage/
    databricks_storage_extension.cpp, databricks_catalog.cpp, databricks_schema_{set,entry}.cpp,
    databricks_table_{set,entry}.cpp, databricks_transaction.cpp, databricks_insert.cpp,
    databricks_ddl.cpp, databricks_dml.cpp, databricks_clear_cache.cpp, databricks_optimizer.cpp
```

### 3.1 Dependencies

`vcpkg.json`: `curl` (`ssl` on non-Windows, `schannel` on Windows), `arrow` with `default-features: false` plus the
`lz4` codec feature, `nlohmann-json`, `openssl`. Overlay ports live in `vcpkg_overlays/` next to
`extension-ci-tools/vcpkg_ports`; version pins are copied from duckdb-bigquery only where the build needs them. CI caches
the vcpkg binary cache as duckdb-bigquery does. Our targets compile as C++17 with `target_compile_features` (not the
shared cache variable), and GCC builds use `-fno-gnu-unique`, both as in duckdb-clickhouse.

### 3.2 `DatabricksAuth`

- One provider per attached database, built from the secret.
- PAT: returns the token as is.
- M2M: `POST https://<host>/oidc/v1/token` with `grant_type=client_credentials&scope=all-apis`, basic auth with the
  client ID/secret. Caches the access token and refreshes it 60 s before `expires_in`. A mutex is held only while
  refreshing. A 401 on any API call forces one refresh and one retry.

### 3.3 `DatabricksHttp`

- libcurl easy handles, one per thread per attached database (kept in a thread-local map keyed by the attached
  database), so connections and TLS sessions are reused. HTTP/1.1 keep-alive; HTTP/2 when curl negotiates it.
- Retries: 429, 503, and connection/timeouts, up to `dbx_http_retries`, exponential backoff with jitter, honouring
  `Retry-After`. Other 4xx/5xx are not retried.
- Errors: parse Databricks' `{error_code, message}` body; the DuckDB error is `Databricks error <error_code>: <message>`
  plus the HTTP status. Authorization headers and presigned URLs' query strings are never included in errors or logs.
- Checks `DuckDB::interrupted` between retries and during long transfers (curl progress callback) so queries cancel
  promptly.

### 3.4 `StatementClient`

- `Execute(sql, catalog, schema, mode)` → `POST /api/2.0/sql/statements` with `warehouse_id`, `catalog`, `schema`,
  `wait_timeout: "10s"`, `on_wait_timeout: "CONTINUE"`.
  - `mode = Small` (metadata, DDL, DML): `disposition: INLINE`, `format: JSON_ARRAY`. Results over 25 MiB are an error
    (never expected for these statements).
  - `mode = Scan` (tables, `databricks_query`): `disposition: EXTERNAL_LINKS`, `format: ARROW_STREAM`.
- While the state is `PENDING`/`RUNNING`: `GET /api/2.0/sql/statements/{id}` with backoff (100 ms doubling to 2 s).
  `FAILED`/`CANCELED`/`CLOSED` become errors with Databricks' message.
- On DuckDB interrupt, or when `dbx_statement_timeout_ms` passes: `POST …/{id}/cancel`, then throw.
- For `Scan`, the result is the manifest (schema, `total_chunk_count`, `total_row_count`) plus the external links of
  the first chunk(s). Any chunk's links are fetched from `GET …/{id}/result/chunks/{i}`.
- `dbx_debug_show_queries` prints the statement text and statement ID.

### 3.5 `ArrowChunkReader`

- Downloads a presigned link **without** the `Authorization` header (the link's own headers from `http_headers` are
  sent). If the link has expired (per `expiration`, or a 403 from storage), fetch fresh links for that chunk once and
  retry.
- Decodes the body with `arrow::ipc::RecordBatchStreamReader` (LZ4 frame buffer compression handled by Arrow), exports
  each batch with `arrow::ExportRecordBatch`, and converts it with DuckDB's Arrow → vector conversion, as duckdb-bigquery
  does.
- The first phase-1 task confirms against a real warehouse how nested types, `TIMESTAMP`, `TIMESTAMP_NTZ`, `INTERVAL`,
  `VARIANT` and `DECIMAL` arrive in the stream. If nested types arrive as JSON strings, the reader parses them into the
  DuckDB type from §4.3 (the catalog type is the source of truth), and this spec is updated.

### 3.6 Parallel scans

One statement per scan. When it succeeds, its chunk indexes form the global scan state's work queue; each DuckDB
thread claims the next index, downloads and decodes that chunk, and emits its rows. `MaxThreads` = `total_chunk_count`
(capped by DuckDB's thread count). Row order is preserved only when the query needs it (`ORDER BY` pushed down): the
scan then declares itself order-preserving and emits chunks in index order.

### 3.7 Transactions

`DatabricksTransaction` holds nothing on the server. Writes are sent when their operator runs. `COMMIT` is a no-op;
`ROLLBACK` after a write emits a warning ("Databricks writes are committed immediately and were not rolled back"). A
DuckDB transaction may write to only one attached database, as with other storage extensions.

## 4. Catalog and reads

### 4.1 Metadata

Queries (all parameterised with named parameters `:catalog`, `:schema`; mode `Small`):

- Schemas: `SELECT schema_name, comment FROM <cat>.information_schema.schemata`.
- Tables/views: `SELECT table_schema, table_name, table_type, comment FROM <cat>.information_schema.tables`.
- Columns, loaded per schema on first use of any table in it:
  `SELECT table_name, column_name, ordinal_position, full_data_type, is_nullable, column_default, comment
   FROM <cat>.information_schema.columns WHERE table_schema = :schema ORDER BY table_name, ordinal_position`.

All three are cached on the attached database. `databricks_clear_cache()` clears every attached Databricks database;
`databricks_execute` and every DDL statement clear the affected database. Metadata queries start the warehouse; the
README says so (serverless starts in seconds, classic warehouses can take minutes).

Identifiers are always quoted with backticks, with embedded backticks doubled.

### 4.2 Pushdown

- **Projection:** only referenced columns are selected. `COUNT(*)`-only scans select `1`.
- **Filters** (`dbx_filter_pushdown`): comparisons, `IN`, `IS [NOT] NULL`, `AND`/`OR`/`NOT`, `BETWEEN`, prefix `LIKE`
  (`LIKE 'abc%'` with an explicit `ESCAPE`). Constants use typed literals from §5.1. Filters stay in DuckDB when semantics
  differ: columns whose `full_data_type` has a non-default collation (`STRING COLLATE …`), `FLOAT`/`DOUBLE`
  comparisons against `NaN`, and anything the translator does not recognise. A filter that is pushed is not re-applied.
- **Limit/order** (`dbx_order_pushdown`): `LIMIT n [OFFSET m]` and `ORDER BY … LIMIT n`, always with explicit
  `NULLS FIRST`/`NULLS LAST` matching DuckDB (Spark defaults to `NULLS FIRST` for `ASC`). Done by an optimizer
  extension, as in duckdb-clickhouse.
- `EXPLAIN` and `EXPLAIN ANALYZE` show the SQL sent in the scan's extra info.

### 4.3 Type mapping (Databricks → DuckDB)

| Databricks | DuckDB |
| ---------- | ------ |
| `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE` | same |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |
| `STRING`, `STRING COLLATE …`, `CHAR(n)`, `VARCHAR(n)` | `VARCHAR` |
| `BINARY` | `BLOB` |
| `DATE` | `DATE` |
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` |
| `TIMESTAMP_NTZ` | `TIMESTAMP` |
| `INTERVAL YEAR…MONTH`, `INTERVAL DAY…SECOND` | `INTERVAL` |
| `ARRAY<T>`, `MAP<K, V>`, `STRUCT<…>` | `LIST`, `MAP`, `STRUCT` |
| `VARIANT` | `JSON` |
| `GEOMETRY`, `GEOGRAPHY`, `OBJECT`, unknown | `VARCHAR` |
| `VOID` | `INTEGER` (always `NULL`) |

`databricks_type_mapping(type_text)` returns the DuckDB type name for a Databricks type string.

## 5. Writes

### 5.1 Literals (`DatabricksLiteral`)

| DuckDB value | Databricks literal |
| ------------ | ------------------ |
| `NULL` | `NULL` (typed as `CAST(NULL AS <type>)` where the type would otherwise be ambiguous) |
| `BOOLEAN` | `true` / `false` |
| integers | decimal text (Databricks casts it to the target column type) |
| `HUGEINT`/unsigned | decimal text with `BD` |
| `DECIMAL` | exact text with `BD` |
| `FLOAT`/`DOUBLE` | round-trip text with `F`/`D`; `float('NaN')`, `double('Infinity')`, `double('-Infinity')` |
| `VARCHAR`, `UUID`, `ENUM` | `'…'` with `\` → `\\` and `'` → `\'` |
| `BLOB` | `X'…'` |
| `DATE` | `DATE'YYYY-MM-DD'` |
| `TIMESTAMP` | `TIMESTAMP_NTZ'YYYY-MM-DD HH:MM:SS.ffffff'` |
| `TIMESTAMPTZ` | `TIMESTAMP'YYYY-MM-DD HH:MM:SS.ffffff+00:00'` |
| `INTERVAL` | `INTERVAL '…' DAY TO SECOND` / `YEAR TO MONTH`; an interval with both a month part and a day/time part is an error |
| `LIST` | `array(…)` |
| `MAP` | `map(k1, v1, …)` |
| `STRUCT` | `named_struct('a', …, 'b', …)` |
| `JSON` | `parse_json('…')` |
| `TIME`, `TIME_NS`, `BIT`, `UNION`, others | error: not representable in Databricks |

Values outside Databricks' range (e.g. dates before `0001-01-01`) are an error, never clamped.

### 5.2 `INSERT` / `COPY … FROM` / CTAS

- `DatabricksInsert` is a parallel sink. Each thread renders
  ``INSERT INTO `cat`.`sch`.`t` (`c1`, …) VALUES (…), (…)`` and sends it once adding the next row would pass
  `dbx_insert_max_statement_bytes`. A single row larger than the 16 MiB hard limit is an error.
- Batches from different threads run concurrently (Delta allows concurrent appends). Each batch commits on its own; a
  failure part-way keeps the batches already sent. An insert that fits in one batch is atomic.
- Columns omitted from the DuckDB `INSERT` are omitted from the column list, so Databricks applies their `DEFAULT`.
- The reported row count is the sum of rows sent in successful batches.
- CTAS = `CREATE TABLE` (§5.3) then the insert. If the insert fails, the table is dropped (best effort, warning if that
  fails too).
- Not supported: `RETURNING`, `ON CONFLICT`/`INSERT OR REPLACE` → error pointing to `databricks_execute` with `MERGE`.

### 5.3 DDL

- `CREATE TABLE` → ``CREATE TABLE `cat`.`sch`.`t` (…) TBLPROPERTIES ('delta.columnMapping.mode' = 'name')``
  (managed Delta). `NOT NULL`, `DEFAULT` (adds `'delta.feature.allowColumnDefaults' = 'supported'`), `PRIMARY KEY`
  (Unity Catalog informational constraint, not enforced; requires `NOT NULL`, which is added). `IF NOT EXISTS` kept.
  Other constraints (`CHECK`, `UNIQUE`, `FOREIGN KEY`) are an error.
- DuckDB → Databricks types: `VARCHAR`/`UUID`/`ENUM` → `STRING`; `BLOB` → `BINARY`; `TIMESTAMP` → `TIMESTAMP_NTZ`;
  `TIMESTAMPTZ` → `TIMESTAMP`; `HUGEINT` → `DECIMAL(38,0)`; `UTINYINT` → `SMALLINT`, `USMALLINT` → `INT`,
  `UINTEGER` → `BIGINT`, `UBIGINT` → `DECIMAL(20,0)`, `UHUGEINT` → error; `JSON` → `VARIANT`; `LIST`/`ARRAY` →
  `ARRAY`; `STRUCT` → `STRUCT`; `MAP` → `MAP`; `TIME`, `BIT`, `UNION`, others → error.
- `DROP TABLE [IF EXISTS]`; `CREATE SCHEMA [IF NOT EXISTS]`; `DROP SCHEMA [IF EXISTS] [CASCADE]`.
- `ALTER TABLE`: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `RENAME TO`, `ALTER COLUMN SET/DROP NOT NULL`,
  `SET/DROP DEFAULT`, `TYPE` (sent as `ALTER COLUMN … TYPE`; Databricks accepts it only when type widening is enabled on
  the table, otherwise its error is passed through). Drop/rename column on tables without column mapping also pass
  through Databricks' error.
- Every DDL statement clears the metadata cache of its attached database.
- `CREATE VIEW`, indexes, sequences, macros: error pointing to `databricks_execute`.

### 5.4 `UPDATE` / `DELETE` / `TRUNCATE`

- `DELETE FROM t WHERE …` and `UPDATE t SET … WHERE …` become one native Delta statement each (atomic). The row count
  is `num_affected_rows` from the result. `TRUNCATE` → `TRUNCATE TABLE`.
- `DatabricksExpression` translates: columns of the target table, constants (§5.1), comparisons, `AND`/`OR`/`NOT`,
  `IN`, `BETWEEN`, `IS [NOT] NULL`, `+ - * / %` and `//` (→ `div`), `CASE`, `coalesce`, `nullif`, casts to types in §5.3,
  `LIKE`/`ILIKE` (always with `ESCAPE '\\'`, pattern re-escaped), and `lower`, `upper`, `trim`, `ltrim`, `rtrim`,
  `length`, `substring`, `concat`, `replace`, `starts_with`, `ends_with`, `contains`, `abs`, `round`, `floor`, `ceil`.
- Rejected before anything runs, with a pointer to `databricks_execute` + `MERGE`: joins, subqueries, `UPDATE … FROM`,
  `RETURNING`, other functions.
- Known differences, documented in the README: Databricks warehouses run in ANSI mode, so division by zero and integer
  overflow raise errors (DuckDB returns `NULL` for division by zero); string functions count characters the same way;
  `NaN` compares equal to itself in both.

### 5.5 `databricks_query` / `databricks_execute`

- `databricks_query(db, sql)`: table function; runs `sql` in `Scan` mode with the attached catalog as default; the result
  schema comes from the manifest. Binding runs the statement (DuckDB needs the schema at bind time); the scan then reads
  that statement's chunks, so the query runs once.
- `databricks_execute(db, sql)`: runs `sql` in `Small` mode, returns nothing, clears the attached database's metadata
  cache. Rejected on a `READ_ONLY` attachment.

## 6. Errors

- Databricks errors keep their `error_code` and message; HTTP transport errors say which step failed (auth, submit,
  poll, chunk download).
- Auth failures say which method was used and never echo secrets.
- Writes on `READ_ONLY` databases: `Cannot write to a read-only Databricks database`.
- Unsupported statements say what to use instead (`databricks_execute`, `MERGE`).

## 7. Testing

**Offline (`make test`, every CI run)**

- sqllogictests needing no server: load, secret validation, `ATTACH` errors, `databricks_type_mapping`.
- `scripts/fake_databricks/server.py` (Python stdlib `http.server` + `pyarrow`): serves `/oidc/v1/token`,
  `/api/2.0/sql/statements` (submit/poll/cancel/chunks) and presigned chunk URLs, from scripted responses keyed by
  SQL text. It records every statement received; tests read them back through `databricks_query` on a special
  `__recorded__` statement, so the SQL sent for pushdown, `INSERT` batches and `UPDATE`/`DELETE` is checked exactly.
  Scripted edge cases: pending → polling, multi-chunk results, expired link → refresh, 429/503 retries, `FAILED` with
  message, token expiring mid-scan, cancel on interrupt.
- `make test-fake` starts the fake server and runs `test/sql/fake/**`.

**Live (`make smoke`)**

- Needs `DATABRICKS_HOST`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_TEST_CATALOG`, and `DATABRICKS_TOKEN` or
  `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`.
- `scripts/test_with_databricks.sh` creates `duckdb_test_<run id>` in the test catalog, runs `test/sql/**` (tests use
  `require-env`), and always drops the schema (`CASCADE`) at the end.
- CI: nightly and `workflow_dispatch`, using repository secrets; never on pull requests from forks.

## 8. Delivery

Four plans, one PR each:

1. **Foundation and read.** Arrow-encoding probe against a real warehouse (updates §3.5/§4.3); rename the template;
   vcpkg + overlays + CI cache; secret; auth (PAT, M2M); HTTP; `StatementClient`; `ArrowChunkReader`;
   `databricks_query`, `databricks_execute`; `ATTACH`, catalog, metadata cache, `databricks_clear_cache`; scans with
   projection pushdown; type mapping and `databricks_type_mapping`; fake server; smoke script.
2. **Pushdown and parallelism.** Filter, `LIMIT` and `ORDER BY … LIMIT` pushdown; parallel chunk scans; SQL in
   `EXPLAIN`; settings.
3. **Inserts and DDL.** Literals, `INSERT`/`COPY`/CTAS, `CREATE`/`DROP`/`ALTER`, `READ_ONLY`.
4. **`UPDATE`, `DELETE`, `TRUNCATE`.** Expression translator and statement translation.

## 9. Out of scope for v1

Staging-volume bulk loads, OAuth U2M, Azure Entra ID, interactive transactions, `MERGE` from DuckDB syntax,
`CREATE VIEW`, `hive_metastore` specifics, DuckDB-WASM.

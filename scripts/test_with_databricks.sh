#!/usr/bin/env bash
# Run sqllogictests against a real Databricks warehouse.
#
# Required: DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, DATABRICKS_TEST_CATALOG,
# and either DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID plus DATABRICKS_CLIENT_SECRET.
# Creates duckdb_test_<run id> in the test catalog and drops it on exit.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

BUILD="${SMOKE_BUILD:-release}"
UNITTEST="./build/${BUILD}/test/unittest"
DUCKDB="./build/${BUILD}/duckdb"
EXTENSION="./build/${BUILD}/extension/databricks/databricks.duckdb_extension"

for name in DATABRICKS_HOST DATABRICKS_WAREHOUSE_ID DATABRICKS_TEST_CATALOG; do
	if [[ -z "${!name:-}" ]]; then
		echo "ERROR: ${name} is not set" >&2
		exit 1
	fi
done
if [[ -z "${DATABRICKS_TOKEN:-}" && ( -z "${DATABRICKS_CLIENT_ID:-}" || -z "${DATABRICKS_CLIENT_SECRET:-}" ) ]]; then
	echo "ERROR: set DATABRICKS_TOKEN, or DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET" >&2
	exit 1
fi
if [[ ! -x "${UNITTEST}" || ! -x "${DUCKDB}" ]]; then
	echo "ERROR: ${UNITTEST} not found; run 'make ${BUILD}' first." >&2
	exit 1
fi

RUN_ID="$(date +%Y%m%d%H%M%S)_$$"
export DATABRICKS_TEST_SCHEMA="duckdb_test_${RUN_ID}"

sql() {
	"${DUCKDB}" -unsigned -c "$1"
}

auth_sql() {
	if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
		printf "%s" "TOKEN getenv('DATABRICKS_TOKEN')"
	else
		printf "%s" "CLIENT_ID getenv('DATABRICKS_CLIENT_ID'), CLIENT_SECRET getenv('DATABRICKS_CLIENT_SECRET')"
	fi
}

cleanup() {
	sql "
LOAD '${EXTENSION}';
CREATE SECRET dbx (TYPE databricks, HOST getenv('DATABRICKS_HOST'), WAREHOUSE_ID getenv('DATABRICKS_WAREHOUSE_ID'), $(auth_sql), CATALOG getenv('DATABRICKS_TEST_CATALOG'));
ATTACH '' AS db (TYPE databricks, SECRET dbx);
CALL databricks_execute('db', 'DROP SCHEMA IF EXISTS ${DATABRICKS_TEST_SCHEMA} CASCADE');
" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> Creating schema ${DATABRICKS_TEST_SCHEMA}"
sql "
LOAD '${EXTENSION}';
CREATE SECRET dbx (TYPE databricks, HOST getenv('DATABRICKS_HOST'), WAREHOUSE_ID getenv('DATABRICKS_WAREHOUSE_ID'), $(auth_sql), CATALOG getenv('DATABRICKS_TEST_CATALOG'));
ATTACH '' AS db (TYPE databricks, SECRET dbx);
CALL databricks_execute('db', 'CREATE SCHEMA IF NOT EXISTS ${DATABRICKS_TEST_SCHEMA}');
" >/dev/null

OUTPUT="$(mktemp)"
set +e
if [[ $# -gt 0 ]]; then
	"${UNITTEST}" "$@" 2>&1 | tee "${OUTPUT}"
else
	"${UNITTEST}" "test/sql/*" 2>&1 | tee "${OUTPUT}"
fi
STATUS="${PIPESTATUS[0]}"
set -e
if grep -q "No tests ran" "${OUTPUT}"; then
	echo "ERROR: no tests ran" >&2
	exit 1
fi
exit "${STATUS}"

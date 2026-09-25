#!/usr/bin/env bash
# Start the fake Databricks server and run test/sql/fake.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

BUILD="${SMOKE_BUILD:-release}"
UNITTEST="./build/${BUILD}/test/unittest"
if [[ ! -x "${UNITTEST}" ]]; then
	echo "ERROR: ${UNITTEST} not found; run 'make ${BUILD}' first." >&2
	exit 1
fi

PYTHON="${FAKE_DATABRICKS_PYTHON:-}"
if [[ -z "${PYTHON}" && -x /tmp/dbx-probe/bin/python ]]; then
	PYTHON=/tmp/dbx-probe/bin/python
fi
if [[ -z "${PYTHON}" ]]; then
	PYTHON=python3
fi
if ! "${PYTHON}" -c "import pyarrow" >/dev/null 2>&1; then
	echo "ERROR: ${PYTHON} cannot import pyarrow. Set FAKE_DATABRICKS_PYTHON." >&2
	exit 1
fi

TMP="$(mktemp -d)"
cleanup() {
	if [[ -n "${SERVER_PID:-}" ]]; then
		kill "${SERVER_PID}" >/dev/null 2>&1 || true
		wait "${SERVER_PID}" >/dev/null 2>&1 || true
	fi
	rm -rf "${TMP}"
}
trap cleanup EXIT

"${PYTHON}" "${ROOT}/scripts/fake_databricks/server.py" >"${TMP}/port" 2>"${TMP}/server.log" &
SERVER_PID=$!
for _ in $(seq 1 50); do
	if [[ -s "${TMP}/port" ]]; then
		break
	fi
	if ! kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
		echo "ERROR: fake Databricks server exited" >&2
		cat "${TMP}/server.log" >&2
		exit 1
	fi
	sleep 0.1
done
PORT="$(head -n 1 "${TMP}/port" | tr -d '[:space:]')"
if [[ -z "${PORT}" ]]; then
	echo "ERROR: fake Databricks server did not report a port" >&2
	cat "${TMP}/server.log" >&2
	exit 1
fi
export FAKE_DATABRICKS_PORT="${PORT}"

OUTPUT="$(mktemp)"
set +e
"${UNITTEST}" "test/sql/fake/*" 2>&1 | tee "${OUTPUT}"
STATUS="${PIPESTATUS[0]}"
set -e
if grep -q "No tests ran" "${OUTPUT}"; then
	echo "ERROR: no fake Databricks tests ran" >&2
	exit 1
fi
exit "${STATUS}"

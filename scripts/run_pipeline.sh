#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
	PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
	PYTHON_BIN="python3"
fi

"$PYTHON_BIN" -m etl.pipeline
"$PYTHON_BIN" tests/test_sql_views.py

echo "Pipeline and validation completed."

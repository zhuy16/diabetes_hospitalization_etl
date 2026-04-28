#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" -m etl.generate_sample_hl7v2
"$PYTHON_BIN" -m etl.pipeline_hl7v2
"$PYTHON_BIN" -m etl.data_quality
"$PYTHON_BIN" tests/test_hl7_pipeline.py
"$PYTHON_BIN" tests/test_sql_views.py

echo "Phase II run complete."

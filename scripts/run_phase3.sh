#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" tests/test_parse_hl7v2_parser.py
bash scripts/run_phase2.sh
"$PYTHON_BIN" -m etl.pipeline
"$PYTHON_BIN" -m etl.data_quality

printf "\nRecent ETL run log entries:\n"
"$PYTHON_BIN" -c "import duckdb; c=duckdb.connect('db/clinical.duckdb'); print(c.execute('select pipeline_name, source_type, status, round(duration_seconds,2) as sec from etl_run_log order by finished_at_utc desc limit 5').fetchdf()); c.close()"

echo "Phase III run complete."

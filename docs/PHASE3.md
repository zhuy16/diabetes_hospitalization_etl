# Phase III Enhancements

This phase adds production-oriented polish on top of the interview-ready project.

## Added capabilities

1. Reproducible developer workflows
- `Makefile` targets for setup, run, phase2, phase3, tests, and dashboard.

2. ETL run observability
- Pipeline run metadata captured in DuckDB tables:
  - `etl_run_log`
  - `etl_table_row_counts`
- Captures status, runtime, source type, and table counts per run.

3. HL7 parser robustness testing
- `tests/test_parse_hl7v2_parser.py` validates malformed/unknown segment handling.

4. Automation and CI
- GitHub Actions workflow in `.github/workflows/ci.yml`.
- Runs parser robustness, Phase II, and Phase III scripts.

## How to run

- Local Phase III full run: `bash scripts/run_phase3.sh`
- Make target: `make phase3`

## Notes

- Pipelines remain idempotent by rebuilding canonical tables each run.
- Metadata tables retain run history for auditability and demos.

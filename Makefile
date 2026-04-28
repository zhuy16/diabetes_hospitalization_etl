.PHONY: setup run run-hl7 phase2 phase3 dq dashboard test clean

PYTHON_BIN := $(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; else echo python3; fi)

setup:
	$(PYTHON_BIN) -m pip install -r requirements.txt

run:
	$(PYTHON_BIN) -m etl.pipeline

run-hl7:
	$(PYTHON_BIN) -m etl.generate_sample_hl7v2
	$(PYTHON_BIN) -m etl.pipeline_hl7v2

phase2:
	bash scripts/run_phase2.sh

phase3:
	bash scripts/run_phase3.sh

dq:
	$(PYTHON_BIN) -m etl.data_quality

test:
	$(PYTHON_BIN) tests/test_parse_hl7v2_parser.py
	$(PYTHON_BIN) tests/test_hl7_pipeline.py
	$(PYTHON_BIN) tests/test_sql_views.py

dashboard:
	streamlit run dashboard/app.py

clean:
	rm -f db/clinical.duckdb
	rm -rf data/processed/from_hl7v2 data/processed/reports

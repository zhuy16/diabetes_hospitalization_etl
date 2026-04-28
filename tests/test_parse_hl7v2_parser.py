from __future__ import annotations

from pathlib import Path
import shutil
import sys
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.parse_hl7v2 import HL7ParseConfig, parse_hl7v2_to_tables


def main() -> None:
    temp_dir = Path(tempfile.mkdtemp(prefix="hl7_parse_test_"))
    try:
        hl7_dir = temp_dir / "hl7"
        out_dir = temp_dir / "out"
        hl7_dir.mkdir(parents=True, exist_ok=True)

        sample = "\n".join(
            [
                "MSH|^~\\&|TEST|SITE|APP|DST|20260428103000||ADT^A01|MSGX|P|2.5",
                "PID|1||PX001||PX001^DEMO||19700101|M|||^^MA^02139",
                "PV1|1|OUTPATIENT|||PR001||||||||||||ENC001",
                "DG1|1||E11.9^Type 2 diabetes mellitus^I10||20230101",
                "OBX|1|NM|4548-4^HbA1c^LN||8.1|%|||||F|||20240201",
                "ZZZ|ignored|segment",
                "|bad|segment",
            ]
        )
        (hl7_dir / "PX001.hl7").write_text(sample + "\n", encoding="utf-8")

        tables = parse_hl7v2_to_tables(HL7ParseConfig(hl7_dir=hl7_dir, output_dir=out_dir))

        assert len(tables["patients"]) == 1
        assert len(tables["observations"]) == 1

        summary = pd.read_csv(out_dir / "parse_summary.csv")
        assert int(summary.loc[0, "files_processed"]) == 1
        assert int(summary.loc[0, "unknown_segments"]) >= 1
        assert int(summary.loc[0, "malformed_segments"]) >= 1

        print("HL7 parser robustness test passed.")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()

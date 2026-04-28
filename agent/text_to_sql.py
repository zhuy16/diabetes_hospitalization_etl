from __future__ import annotations

from pathlib import Path
import os
import re
from typing import Tuple

import anthropic
import duckdb
from dotenv import load_dotenv


BLOCKED_KEYWORDS = [
    "drop",
    "delete",
    "update",
    "insert",
    "alter",
    "create",
    "attach",
    "copy",
    "pragma",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_prompt() -> str:
    return (_repo_root() / "agent" / "prompt_template.txt").read_text(encoding="utf-8")


def _sanitize_sql(sql: str) -> str:
    cleaned = sql.strip().strip("`")
    cleaned = re.sub(r";+$", "", cleaned)
    return cleaned


def validate_read_only(sql: str) -> None:
    lowered = sql.lower().strip()
    if not lowered.startswith("select") and not lowered.startswith("with"):
        raise ValueError("Generated SQL must start with SELECT or WITH.")
    for kw in BLOCKED_KEYWORDS:
        if re.search(rf"\\b{kw}\\b", lowered):
            raise ValueError(f"Blocked keyword found in SQL: {kw}")


def text_to_sql(question: str) -> str:
    load_dotenv(_repo_root() / ".env")
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY is missing. Set it in .env.")

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=700,
        system=_load_prompt(),
        messages=[{"role": "user", "content": question}],
    )
    sql = _sanitize_sql(msg.content[0].text)
    validate_read_only(sql)
    return sql


def run_query(sql: str, db_path: Path | None = None):
    resolved_db = db_path or (_repo_root() / "db" / "clinical.duckdb")
    conn = duckdb.connect(str(resolved_db))
    try:
        return conn.execute(sql).df()
    finally:
        conn.close()


def ask(question: str) -> Tuple[str, object]:
    sql = text_to_sql(question)
    result = run_query(sql)
    return sql, result


if __name__ == "__main__":
    q = input("Ask a clinical SQL question: ").strip()
    generated_sql, df = ask(q)
    print("\nGenerated SQL:\n")
    print(generated_sql)
    print("\nResults:\n")
    print(df.head(25))

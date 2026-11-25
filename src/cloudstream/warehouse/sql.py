"""Load and split the SQL scripts shared by DuckDB and Snowflake."""

from __future__ import annotations

from pathlib import Path

from cloudstream.config import project_root


def script_path(*parts: str) -> Path:
    return project_root().joinpath("sql", *parts)


def split_sql(text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip()
            if statement.endswith(";"):
                statement = statement[:-1].strip()
            if statement:
                statements.append(statement)
            current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def read_script(*parts: str) -> list[str]:
    return split_sql(script_path(*parts).read_text(encoding="utf-8"))

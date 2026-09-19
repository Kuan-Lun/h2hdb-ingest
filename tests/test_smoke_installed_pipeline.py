"""Installed-wheel smoke must distinguish source summaries from other metrics."""

from __future__ import annotations

import runpy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts/smoke-installed-pipeline.py"


@pytest.fixture(scope="module")
def smoke_module() -> dict[str, Any]:
    return runpy.run_path(str(_SCRIPT))


def _source_line(*, level: str = "INFO", status: str = "completed") -> str:
    return (
        f"2026-09-19 00:00:00 [{level}] ingest_metric "
        f"scope=source operation=synchronize elapsed_ns=1 status={status}"
    )


def test_source_summaries_ignore_other_scopes_and_accept_final_status_token(
    tmp_path: Path, smoke_module: dict[str, Any]
) -> None:
    source = _source_line()
    other = [
        "[INFO] ingest_metric scope=publication operation=synchronize status=completed",
        "[INFO] ingest_metric scope=artifact_totals operation=publication status=completed",
        "[INFO] ingest_metric scope=source_monitor operation=inventory status=completed",
        "[DEBUG] ingest_metric scope=artifact operation=render_archive status=completed",
    ]
    log_path = tmp_path / "ingest.log"
    for count in (1, 2, 3):
        log_path.write_text("\n".join([source, *other] * count), encoding="utf-8")
        assert (
            smoke_module["_require_source_summaries"](log_path, expected=count)
            == [source] * count
        )


@pytest.mark.parametrize("count", (0, 2))
def test_source_summary_count_remains_exact(
    tmp_path: Path, smoke_module: dict[str, Any], count: int
) -> None:
    log_path = tmp_path / "ingest.log"
    log_path.write_text("\n".join([_source_line()] * count), encoding="utf-8")
    with pytest.raises(AssertionError, match="exactly one summary"):
        smoke_module["_require_source_summaries"](log_path, expected=1)


@pytest.mark.parametrize(
    "change",
    (
        lambda line: line.replace("[INFO]", "[DEBUG]"),
        lambda line: line.replace("status=completed", "status=failed"),
        lambda line: line.replace("status=completed", "status=interrupted"),
        lambda line: line.replace("operation=synchronize", "operation=inventory"),
        lambda line: line.replace("scope=source", "scope=source_monitor"),
    ),
    ids=("debug", "failed", "interrupted", "wrong-operation", "scope-prefix"),
)
def test_invalid_source_summary_cannot_satisfy_installed_smoke(
    tmp_path: Path,
    smoke_module: dict[str, Any],
    change: Callable[[str], str],
) -> None:
    log_path = tmp_path / "ingest.log"
    log_path.write_text(change(_source_line()), encoding="utf-8")
    with pytest.raises(AssertionError):
        smoke_module["_require_source_summaries"](log_path, expected=1)

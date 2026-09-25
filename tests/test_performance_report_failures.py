"""Missing durable evidence must never use the cost-violation exit code."""

from __future__ import annotations

import errno
import json
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPTS = Path(__file__).parents[1] / "scripts"


@pytest.mark.parametrize("tool", ("source", "library-cleanup"))
@pytest.mark.parametrize("failure", ("space", "permission", "race"))
def test_failed_report_install_is_incomplete_and_preserves_other_writer(
    tool: str,
    failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    script = SCRIPTS / f"check-{tool}-cost.py"
    probe = runpy.run_path(str(script))
    output = tmp_path / "report.json"
    report = {"status": "completed", "acceptance": {"status": "violated"}}
    bindings = probe["main"].__globals__

    def fail_install(*_args: Any, **_kwargs: Any) -> None:
        if failure == "race":
            output.write_bytes(b"another writer's evidence")
            raise FileExistsError(errno.EEXIST, "concurrent report")
        if failure == "space":
            raise OSError(errno.ENOSPC, "no space for evidence")
        raise PermissionError(errno.EACCES, "cannot write evidence")

    if tool == "source":
        monkeypatch.setitem(bindings, "_provenance", lambda **_kwargs: {})
        monkeypatch.setitem(
            bindings,
            "_bounded_worker",
            lambda *_args, **_kwargs: subprocess.CompletedProcess(
                [], 0, json.dumps(report), ""
            ),
        )
        monkeypatch.setitem(
            bindings, "_validate_report", lambda *_args, **_kwargs: None
        )
        monkeypatch.setitem(probe["_HELPERS"], "_atomic_report", fail_install)
        monkeypatch.setattr(sys, "argv", [str(script), "--output", str(output)])
        result = probe["main"]()
    else:
        monkeypatch.setitem(bindings, "_run", lambda **_kwargs: report)
        monkeypatch.setattr(Path, "hardlink_to", fail_install)
        result = probe["main"](["--output", str(output)])

    assert result == 2
    captured = capsys.readouterr()
    diagnostic = json.loads(captured.err)
    assert diagnostic["status"] == "error"
    assert diagnostic["acceptance"] == "incomplete"
    assert not captured.out
    assert not list(tmp_path.glob(".journal-cost-*"))
    if failure == "race":
        assert output.read_bytes() == b"another writer's evidence"
    else:
        assert not output.exists()

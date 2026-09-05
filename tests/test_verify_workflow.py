from __future__ import annotations

from pathlib import Path

_WORKFLOWS = Path(__file__).parents[1] / ".github" / "workflows"


def test_generated_jobs_install_all_required_pytest_plugins() -> None:
    workflow = (_WORKFLOWS / "verify.yml").read_text(encoding="utf-8")
    for job in (
        "incremental-reference-properties",
        "incremental-reference-properties-deep",
    ):
        # Read the complete job, stopping at the next two-space job declaration.
        lines = workflow.split(f"  {job}:\n", 1)[1].splitlines()
        job_lines: list[str] = []
        for line in lines:
            if line.startswith("  ") and not line.startswith("    "):
                break
            job_lines.append(line)
        section = "\n".join(job_lines)
        assert (
            "scripts/install-ci-dependencies.py --pytest-plugins pytest hypothesis"
            in section
        )
        assert (
            "python -m pytest -q tests/test_vnext_incremental_state_machine.py"
            in section
        )
    assert "pytest==" not in workflow
    assert "hypothesis==" not in workflow
    assert "python scripts/install-ci-dependencies.py pytest\n" in workflow
    assert "python -m pytest -o addopts= -o required_plugins=" in workflow


def test_publish_tools_use_manifest_requirements() -> None:
    workflow = (_WORKFLOWS / "publish.yml").read_text(encoding="utf-8")
    assert "python scripts/install-ci-dependencies.py packaging build" in workflow

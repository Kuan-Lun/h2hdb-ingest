from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def _git(
    worktree: Path,
    *arguments: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", "-C", str(worktree), *arguments),
        check=check,
        capture_output=True,
        text=True,
    )


def _operation_path(worktree: Path, name: str) -> Path:
    result = _git(
        worktree,
        "rev-parse",
        "--path-format=absolute",
        "--git-path",
        name,
    )
    return Path(result.stdout.strip())


def test_failed_merge_gate_aborts_the_primary_worktree_and_retains_task(
    tmp_path: Path,
) -> None:
    source_root = Path(__file__).resolve().parents[1]
    primary_worktree = tmp_path / "primary"
    task_worktree = tmp_path / "task"
    primary_worktree.mkdir()
    _git(primary_worktree, "init", "-b", "main")
    _git(primary_worktree, "config", "user.name", "Merge Test")
    _git(primary_worktree, "config", "user.email", "merge@example.invalid")
    _git(primary_worktree, "config", "core.hooksPath", ".githooks")
    _git(primary_worktree, "config", "workflow.primaryBranch", "main")

    scripts = primary_worktree / "scripts"
    hooks = primary_worktree / ".githooks"
    scripts.mkdir()
    hooks.mkdir()
    for name in ("detect-primary-branch.sh", "git-flow-merge.sh"):
        shutil.copy2(source_root / "scripts" / name, scripts / name)
    pre_merge_hook = hooks / "pre-merge-commit"
    pre_merge_hook.write_text("#!/usr/bin/env bash\nexit 42\n", encoding="utf-8")
    pre_merge_hook.chmod(0o755)
    (primary_worktree / "payload.txt").write_text("primary\n", encoding="utf-8")
    _git(primary_worktree, "add", ".")
    _git(primary_worktree, "commit", "-m", "chore: initialize fixture")
    primary_head = _git(primary_worktree, "rev-parse", "HEAD").stdout.strip()

    _git(primary_worktree, "worktree", "add", "-b", "perf/task", str(task_worktree))
    (task_worktree / "payload.txt").write_text("task\n", encoding="utf-8")
    _git(task_worktree, "add", "payload.txt")
    _git(task_worktree, "commit", "-m", "feat: change payload")
    task_head = _git(task_worktree, "rev-parse", "HEAD").stdout.strip()

    result = subprocess.run(
        (str(task_worktree / "scripts" / "git-flow-merge.sh"),),
        cwd=task_worktree,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "merge or merge gate failed; task branch was retained" in result.stderr
    assert not _operation_path(primary_worktree, "MERGE_HEAD").exists()
    assert _git(primary_worktree, "status", "--porcelain=v1").stdout == ""
    assert _git(primary_worktree, "rev-parse", "HEAD").stdout.strip() == primary_head
    assert (primary_worktree / "payload.txt").read_text(encoding="utf-8") == (
        "primary\n"
    )

    assert _git(
        primary_worktree,
        "show-ref",
        "--verify",
        "refs/heads/perf/task",
    ).stdout
    assert _git(task_worktree, "rev-parse", "HEAD").stdout.strip() == task_head
    assert _git(task_worktree, "status", "--porcelain=v1").stdout == ""
    worktrees = _git(primary_worktree, "worktree", "list", "--porcelain").stdout
    assert f"worktree {task_worktree}\n" in worktrees
    assert "branch refs/heads/perf/task\n" in worktrees


def test_full_gate_unsets_live_test_opt_ins() -> None:
    source_root = Path(__file__).resolve().parents[1]
    check_full = (source_root / "scripts" / "check-full.sh").read_text(encoding="utf-8")

    assert "unset H2HDB_INGEST_TEST_PRIVATE_CORPUS" in check_full
    assert "unset H2HDB_TEST_MARIADB" in check_full

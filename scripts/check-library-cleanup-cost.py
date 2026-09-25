"""Measure cleanup-selection VM work on isolated, seeded production journals.

This is an SQLite engine cost experiment, not public end-to-end library cleanup.
Production uses its exact v5 index; an extra fixture index and forced scans
independently exercise the unchanged cost budget and negative control.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import sqlite3
import sys
import tempfile
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any

from h2hdb_ingest import _library_journal as journal
from h2hdb_ingest import library

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIZES = (0, 127, 128, 129, 4096, 32768)
FULL_INVENTORY_TOKENS = 132046 * 2
REPETITIONS = 3
SCENARIOS = ("no_eligible", "sparse_eligible")
VARIANTS = ("production", "fixture_index", "forced_scan")
COLUMNS = (
    "token",
    "storage_codec",
    "storage_path",
    "object_sha256",
    "size_bytes",
    "staging_leaf",
    "device",
    "inode",
    "modified_ns",
    "changed_ns",
)
_INDEX = "fixture_cleanup_eligible_idx"
_INDEX_SQL = (
    f"CREATE INDEX {_INDEX} ON protection_tokens(token) "
    "WHERE state = 'RELEASED' AND staging_leaf IS NOT NULL"
)
MODEL = {
    "version": 1,
    "unit": "SQLite VM instructions; progress_handler called every instruction",
    "dimensions": "N retained tokens, E eligible tokens, R selected rows; three replay cycles",
    "budget": "192 + 48 * max(1, (N + E).bit_length()) + 48 * R per SELECT",
    "rationale": (
        "A predicate-matching ordered index can seek/traverse eligible entries without "
        "visiting retained tokens. The fixed allowances cover cursor setup, logarithmic "
        "tree traversal and ten-column materialization. They are declared engineering "
        "targets, not a SQLite opcode theorem or a budget fitted to current scans."
    ),
    "controls": (
        "Same real SQL, data and exact row oracle: an additional index in the disposable "
        "fixture must satisfy the target; NOT INDEXED must violate it at N >= 4096."
    ),
    "limits": (
        "No Core SQL, filesystem locks, hashing, unlink, fsync, journal-open validation, "
        "pending-entry deletion, full lifecycle throughput or NAS completion-time claim. "
        "Wall time includes VM callback overhead; VM instructions are the acceptance unit."
    ),
}


def _source() -> dict[str, Any]:
    hashes = {}
    for module, name in ((library, "library.py"), (journal, "_library_journal.py")):
        expected = ROOT / "src" / "h2hdb_ingest" / name
        actual = Path(str(module.__file__)).resolve()
        if actual != expected.resolve():
            raise ValueError(f"imported {name} is not the measured checkout")
        hashes[name] = {
            "path": str(actual),
            "sha256": sha256(actual.read_bytes()).hexdigest(),
        }
    return {
        "imported_sources": hashes,
        "journal_format": journal.FORMAT_VERSION,
        "sqlite_version": sqlite3.sqlite_version,
        "python": sys.version,
        "acceptance_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
    }


def _queries() -> tuple[dict[str, str], int]:
    """Extract production SQL, rejecting changed authoring shapes rather than copying it."""
    source = ROOT / "src" / "h2hdb_ingest" / "library.py"
    tree = ast.parse(source.read_text())
    methods = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_maintain_cleanup"
    ]
    if len(methods) != 1:
        raise ValueError("cleanup method authoring surface is ambiguous")
    found = {}
    for node in ast.walk(methods[0]):
        if (
            not isinstance(node, ast.Call)
            or not isinstance(node.func, ast.Attribute)
            or node.func.attr != "execute"
            or not node.args
        ):
            continue
        if not isinstance(node.args[0], ast.Constant) or not isinstance(
            node.args[0].value, str
        ):
            continue
        query = node.args[0].value
        if not query.startswith("SELECT ") or "FROM protection_tokens " not in query:
            continue
        kind = "remaining" if query.startswith("SELECT EXISTS(") else "page"
        if kind in found:
            raise ValueError("duplicate cleanup selection query")
        if kind == "page" and (
            len(node.args) != 2
            or ast.dump(node.args[1])
            != ast.dump(ast.parse("(_MAX_CLEANUP_ITEMS,)", mode="eval").body)
        ):
            raise ValueError("cleanup page no longer uses the production fixed limit")
        if kind == "remaining" and len(node.args) != 1:
            raise ValueError("cleanup existence binding shape changed")
        found[kind] = query
    limits = [
        n.value.value
        for n in tree.body
        if isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "_MAX_CLEANUP_ITEMS" for t in n.targets
        )
        and isinstance(n.value, ast.Constant)
    ]
    if (
        set(found) != {"page", "remaining"}
        or limits != [library._MAX_CLEANUP_ITEMS]
        or type(limits[0]) is not int
        or not 1 <= limits[0] <= 128
    ):
        raise ValueError("cleanup selection authoring contract changed")
    journal_tree = ast.parse((source.parent / "_library_journal.py").read_text())
    schemas = [
        n.value.value
        for n in journal_tree.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "SCHEMA" for t in n.targets)
        and isinstance(n.value, ast.Constant)
    ]
    if schemas != [journal.SCHEMA]:
        raise ValueError("imported journal DDL differs from checkout source")
    code = library.ManagedFilesystemLibraryAdapter._maintain_cleanup.__code__
    if any(query not in code.co_consts for query in found.values()):
        raise ValueError("imported cleanup SQL differs from checkout source")
    return found, int(limits[0])


def _encoded(rows: Sequence[Sequence[Any]]) -> list[list[Any]]:
    return [
        [{"hex": value.hex()} if isinstance(value, bytes) else value for value in row]
        for row in rows
    ]


def _record(position: int, *, eligible: bool) -> tuple[Any, ...]:
    token = position.to_bytes(32, "big")
    thumbnail = position % 2 == 0
    return (
        token,
        "fixture-codec",
        f"fixture/{position}.{'jpg' if thumbnail else 'cbz'}",
        sha256(token).digest(),
        position + 1,
        token.hex() + (".jpg" if thumbnail else ".cbz") if eligible else None,
        (1).to_bytes(8, "big") if eligible else None,
        (position + 1).to_bytes(8, "big") if eligible else None,
        1000 if eligible else None,
        1001 if eligible else None,
    )


def _seed(connection: sqlite3.Connection, retained: int, eligible: int) -> None:
    journal.create_fresh_journal(
        connection, bytes.fromhex("00000000000040008000000000000001")
    )
    journal.require_exact_schema(connection)
    query = (
        "INSERT INTO protection_tokens ("
        + ", ".join(COLUMNS)
        + ", state, published_modified_at) VALUES ("
        + ",".join("?" for _ in range(12))
        + ")"
    )
    for start in range(0, retained + eligible, 4096):
        connection.executemany(
            query,
            (
                (
                    *_record(i, eligible=i >= retained),
                    "RELEASED" if i >= retained or i % 2 else "INSTALLED",
                    "2026-01-01T00:00:00Z",
                )
                for i in range(start, min(start + 4096, retained + eligible))
            ),
        )
    connection.commit()
    if connection.execute("SELECT COUNT(*) FROM protection_tokens").fetchone() != (
        retained + eligible,
    ):
        raise ValueError("fixture token count differs from requested dimensions")


def _measure(
    connection: sqlite3.Connection, query: str, params: tuple[int, ...]
) -> dict[str, Any]:
    instructions = 0

    def tick() -> int:
        nonlocal instructions
        instructions += 1
        return 0

    connection.set_progress_handler(tick, 1)
    started = perf_counter()
    try:
        cursor = connection.execute(query, params)
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description]
    finally:
        elapsed = perf_counter() - started
        connection.set_progress_handler(None, 0)
    return {
        "query": query,
        "query_sha256": sha256(query.encode()).hexdigest(),
        "parameters": list(params),
        "columns": columns,
        "rows": _encoded(rows),
        "vm_instructions": instructions,
        "elapsed_seconds": elapsed,
        "query_plan": _encoded(
            connection.execute("EXPLAIN QUERY PLAN " + query, params).fetchall()
        ),
    }


def _budget(total: int, rows: int) -> int:
    return 192 + 48 * max(1, total.bit_length()) + 48 * rows


def _case(
    connection: sqlite3.Connection,
    *,
    retained: int,
    scenario: str,
    variant: str,
    queries: dict[str, str],
    limit: int,
) -> dict[str, Any]:
    eligible = limit + 1 if scenario == "sparse_eligible" else 0
    active_queries = queries.copy()
    if variant == "forced_scan":
        active_queries = {
            kind: sql.replace(
                "FROM protection_tokens ", "FROM protection_tokens NOT INDEXED ", 1
            )
            for kind, sql in queries.items()
        }
    cycles = []
    for cycle in range(REPETITIONS):
        connection.execute("SAVEPOINT fixture_replay")
        pages = []
        try:
            for offset in range(0, eligible + limit, limit):
                page = _measure(connection, active_queries["page"], (limit,))
                expected = [
                    _record(i, eligible=True)
                    for i in range(
                        retained + offset, retained + min(offset + limit, eligible)
                    )
                ]
                if page["rows"] != _encoded(expected) or page["columns"] != list(
                    COLUMNS
                ):
                    raise ValueError(
                        "cleanup page differs from independent seed oracle"
                    )
                # Emulate only the journal eligibility transition, never filesystem work.
                connection.executemany(
                    "UPDATE protection_tokens SET staging_leaf = NULL WHERE token = ?",
                    ((row[0],) for row in expected),
                )
                remaining = _measure(connection, active_queries["remaining"], ())
                if remaining["rows"] != [[int(offset + limit < eligible)]]:
                    raise ValueError(
                        "cleanup EXISTS differs from independent seed oracle"
                    )
                pages.append({"page": page, "remaining": remaining})
        finally:
            connection.execute("ROLLBACK TO fixture_replay")
            connection.execute("RELEASE fixture_replay")
        cycles.append({"cycle": cycle, "pages": pages})
    return {
        "retained_tokens": retained,
        "eligible_tokens": eligible,
        "total_tokens": retained + eligible,
        "retained_installed_tokens": (retained + 1) // 2,
        "retained_released_tokens": retained // 2,
        "scenario": scenario,
        "variant": variant,
        "cycles": cycles,
    }


def _assess_case(
    case: dict[str, Any], *, queries: dict[str, str], limit: int
) -> dict[str, Any]:
    try:
        retained, eligible = case["retained_tokens"], case["eligible_tokens"]
        scenario, variant = case["scenario"], case["variant"]
        if (
            type(retained) is not int
            or retained < 0
            or scenario not in SCENARIOS
            or variant not in VARIANTS
            or type(eligible) is not int
            or eligible != (limit + 1 if scenario == "sparse_eligible" else 0)
            or case["total_tokens"] != retained + eligible
            or case["retained_installed_tokens"] != (retained + 1) // 2
            or case["retained_released_tokens"] != retained // 2
        ):
            raise ValueError("invalid fixture dimensions")
        if len(case["cycles"]) != REPETITIONS:
            raise ValueError("missing replay cycle")
        checks = []
        for number, cycle in enumerate(case["cycles"]):
            if cycle["cycle"] != number or len(cycle["pages"]) != (
                math.ceil(eligible / limit) + 1
            ):
                raise ValueError("incomplete cleanup page coverage")
            for index, pair in enumerate(cycle["pages"]):
                offset = index * limit
                expected = _encoded(
                    [
                        _record(i, eligible=True)
                        for i in range(
                            retained + offset, retained + min(offset + limit, eligible)
                        )
                    ]
                )
                for kind, rows in (
                    ("page", expected),
                    ("remaining", [[int(offset + limit < eligible)]]),
                ):
                    event = pair[kind]
                    query = queries[kind]
                    if variant == "forced_scan":
                        query = query.replace(
                            "FROM protection_tokens ",
                            "FROM protection_tokens NOT INDEXED ",
                            1,
                        )
                    if (
                        event["query"] != query
                        or event["query_sha256"] != sha256(query.encode()).hexdigest()
                        or event["parameters"] != ([limit] if kind == "page" else [])
                        or event["rows"] != rows
                    ):
                        raise ValueError(
                            "query or result differs from independent oracle"
                        )
                    if kind == "page" and event["columns"] != list(COLUMNS):
                        raise ValueError("cleanup projection changed")
                    if not event["query_plan"] or not event["columns"]:
                        raise ValueError("missing SQL plan or columns")
                    count, elapsed = event["vm_instructions"], event["elapsed_seconds"]
                    if (
                        type(count) is not int
                        or count <= 0
                        or type(elapsed) not in (int, float)
                        or not math.isfinite(elapsed)
                        or elapsed < 0
                    ):
                        raise ValueError("invalid instruction or timing measurement")
                    ceiling = _budget(
                        retained + eligible, len(expected) if kind == "page" else 1
                    )
                    checks.append(
                        {
                            "cycle": number,
                            "page": index,
                            "query": kind,
                            "observed_vm_instructions": count,
                            "ceiling": ceiling,
                            "met": count <= ceiling,
                        }
                    )
        return {
            "status": "satisfied" if all(c["met"] for c in checks) else "violated",
            "checks": checks,
        }
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        return {"status": "incomplete", "reasons": [str(error)]}


def _assess(
    report: dict[str, Any],
    *,
    sizes: tuple[int, ...],
    queries: dict[str, str],
    limit: int,
) -> dict[str, Any]:
    try:
        if (
            report["schema_version"] != 1
            or report["status"] != "completed"
            or report["evidence_profile"] != "seeded_isolated_sqlite_engine"
            or report["production_queries"] != queries
            or report["cleanup_limit"] != limit
            or report["provenance"] != _source()
            or report["model"] != MODEL
        ):
            raise ValueError("missing or changed provenance/contract")
        expected = {(n, s, v) for n in sizes for s in SCENARIOS for v in VARIANTS}
        observed = [
            (c["retained_tokens"], c["scenario"], c["variant"]) for c in report["cases"]
        ]
        if len(observed) != len(expected) or set(observed) != expected:
            raise ValueError("missing or duplicate matrix case")
        for case in report["cases"]:
            case["acceptance"] = _assess_case(case, queries=queries, limit=limit)
        if any(c["acceptance"]["status"] == "incomplete" for c in report["cases"]):
            raise ValueError("one or more matrix cases lack complete evidence")
        positives = [c for c in report["cases"] if c["variant"] == "fixture_index"]
        negatives = [
            c
            for c in report["cases"]
            if c["variant"] == "forced_scan" and c["retained_tokens"] >= 4096
        ]
        if (
            not negatives
            or any(c["acceptance"]["status"] != "violated" for c in negatives)
            or any(c["acceptance"]["status"] != "satisfied" for c in positives)
        ):
            raise ValueError(
                "positive or negative control did not establish discrimination"
            )
        production = [c for c in report["cases"] if c["variant"] == "production"]
        return {
            "status": "violated"
            if any(c["acceptance"]["status"] == "violated" for c in production)
            else "satisfied",
            "controls": "passed",
        }
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        return {"status": "incomplete", "reasons": [str(error)]}


def _run(*, sizes: tuple[int, ...], workspace: Path) -> dict[str, Any]:
    provenance = _source()
    queries, limit = _queries()
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "evidence_profile": "seeded_isolated_sqlite_engine",
        "model": MODEL,
        "provenance": provenance,
        "production_queries": queries,
        "cleanup_limit": limit,
        "cases": [],
    }
    for retained in sizes:
        for scenario in SCENARIOS:
            path = workspace / f"tokens-{retained}-{scenario}.sqlite3"
            if path.exists():
                raise ValueError("fixture destination already exists")
            connection = sqlite3.connect(path, cached_statements=0)
            try:
                _seed(
                    connection,
                    retained,
                    limit + 1 if scenario == "sparse_eligible" else 0,
                )
                for variant in VARIANTS:
                    if variant == "fixture_index":
                        connection.execute(_INDEX_SQL)
                    report["cases"].append(
                        _case(
                            connection,
                            retained=retained,
                            scenario=scenario,
                            variant=variant,
                            queries=queries,
                            limit=limit,
                        )
                    )
                connection.execute(f"DROP INDEX {_INDEX}")
                journal.require_exact_schema(connection)
            finally:
                connection.close()
    if provenance != _source():
        raise ValueError("measured sources changed during experiment")
    report["status"] = "completed"
    report["acceptance"] = _assess(report, sizes=sizes, queries=queries, limit=limit)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--retained-tokens", default=",".join(map(str, DEFAULT_SIZES)))
    parser.add_argument(
        "--full-inventory",
        action="store_true",
        help="also seed 132046 galleries x two resources = 264092 retained tokens",
    )
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output already exists; choose a new report path")
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "error",
        "acceptance": {"status": "incomplete"},
        "evidence_profile": "seeded_isolated_sqlite_engine",
    }
    try:
        sizes = tuple(int(value) for value in args.retained_tokens.split(","))
        if args.full_inventory:
            sizes = tuple(dict.fromkeys((*sizes, FULL_INVENTORY_TOKENS)))
        if (
            not sizes
            or len(set(sizes)) != len(sizes)
            or any(n < 0 or n > FULL_INVENTORY_TOKENS for n in sizes)
            or len(sizes) > 16
        ):
            raise ValueError("invalid or unbounded retained token dimensions")
        with tempfile.TemporaryDirectory(prefix="h2hdb-library-cost-") as directory:
            report = _run(sizes=sizes, workspace=Path(directory))
    except (OSError, ValueError, TypeError, sqlite3.Error) as error:
        report["status"] = "error"
        report["acceptance"] = {"status": "incomplete", "reasons": [str(error)]}
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        # Install only a complete report, without replacing a concurrent writer.
        with tempfile.TemporaryDirectory(
            prefix=".journal-cost-", dir=args.output.parent
        ) as directory:
            staged = Path(directory) / "report.json"
            staged.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
            args.output.hardlink_to(staged)
    except (OSError, TypeError, ValueError) as error:
        print(
            json.dumps(
                {
                    "status": "error",
                    "acceptance": "incomplete",
                    "error_type": type(error).__name__,
                    "error": f"cannot store journal cost report: {error}",
                }
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "acceptance": report["acceptance"]["status"],
                "output": str(args.output),
            }
        )
    )
    return {"satisfied": 0, "violated": 1, "incomplete": 2}[
        report["acceptance"]["status"]
    ]


if __name__ == "__main__":
    raise SystemExit(main())

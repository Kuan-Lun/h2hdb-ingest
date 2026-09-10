#!/usr/bin/env python3
"""Build the standalone, standard-library-only journal relocation executable."""

from __future__ import annotations

import argparse
import tempfile
import zipapp
from pathlib import Path
from shutil import copyfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    source = repository / "src" / "h2hdb_ingest"
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="h2hdb-relocation-tool-") as temporary:
        root = Path(temporary)
        package = root / "h2hdb_ingest"
        package.mkdir()
        (package / "__init__.py").write_text(
            '"""Standalone library maintenance; no ingest runtime dependencies."""\n'
        )
        modules = {
            "_library_journal.py",
            "_storage_paths.py",
            "journal_upgrade.py",
            "library_relocation.py",
            "relocate.py",
        }
        modules.update(path.name for path in source.glob("_relocation_*.py"))
        for name in sorted(modules):
            copyfile(source / name, package / name)
        zipapp.create_archive(
            root,
            target=arguments.output,
            interpreter="/usr/bin/env python3",
            main="h2hdb_ingest.relocate:main",
            compressed=True,
        )
    print(arguments.output.resolve())


if __name__ == "__main__":
    main()

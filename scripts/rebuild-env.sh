#!/usr/bin/env bash
# Recreate the repository-local environment without a lockfile.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

uv venv --clear --python 3.14

EDITABLES=(-e ".[dev]")
for sibling in ../h2hdb.clone ../h2h-galleryinfo-parser.clone; do
    if [[ -f "$sibling/pyproject.toml" ]]; then
        EDITABLES+=(-e "$sibling")
    fi
done
uv pip install --python .venv/bin/python "${EDITABLES[@]}"

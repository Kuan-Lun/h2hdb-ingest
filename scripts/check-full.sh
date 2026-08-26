#!/usr/bin/env bash

set -euo pipefail

repository_root="$(git rev-parse --show-toplevel)"
cd "$repository_root"

scripts/check-fast.sh
.venv/bin/pytest

.venv/bin/python scripts/verify-formal.py lean
.venv/bin/python scripts/verify-formal.py tla \
    --tla-jar .formal-tools/tla2tools-1.7.4.jar \
    --tla-runtime auto

artifact_root="$(mktemp -d "${TMPDIR:-/tmp}/h2hdb-ingest-check.XXXXXX")"
cleanup() {
    rm -rf -- "$artifact_root"
}
trap cleanup EXIT

.venv/bin/python -m build --no-isolation --outdir "$artifact_root/dist"
wheel="$(find "$artifact_root/dist" -maxdepth 1 -type f -name '*.whl' -print -quit)"
[[ -n "$wheel" ]] || {
    printf 'Wheel build did not produce an artifact\n' >&2
    exit 1
}

UV_CACHE_DIR="$artifact_root/uv-cache" \
    uv venv --python "$repository_root/.venv/bin/python" \
    "$artifact_root/smoke-venv"
smoke_python="$artifact_root/smoke-venv/bin/python"
UV_CACHE_DIR="$artifact_root/uv-cache" \
    uv pip install --python "$smoke_python" --no-deps "$wheel"
development_site="$("$repository_root/.venv/bin/python" -c \
    'import sysconfig; print(sysconfig.get_path("purelib"))')"
smoke_site="$("$smoke_python" -c \
    'import sysconfig; print(sysconfig.get_path("purelib"))')"
printf '%s\n' "$development_site" > \
    "$smoke_site/development-dependencies.pth"
(
    cd "$artifact_root"
    "$smoke_python" -I -c \
        'import h2hdb_ingest, pathlib, sys; assert pathlib.Path(h2hdb_ingest.__file__).resolve().is_relative_to(pathlib.Path(sys.prefix).resolve())'
    "$smoke_python" -I -m h2hdb_ingest --help >/dev/null
    "$artifact_root/smoke-venv/bin/h2hdb-ingest-bootstrap" --help >/dev/null
)

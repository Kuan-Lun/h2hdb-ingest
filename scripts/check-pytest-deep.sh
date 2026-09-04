#!/usr/bin/env bash

set -euo pipefail

repository_root="$(git rev-parse --show-toplevel)"
cd "$repository_root"

export H2HDB_TEST_MARIADB=1
unset H2HDB_INGEST_TEST_PRIVATE_CORPUS
exec .venv/bin/pytest -o addopts= --strict-markers -m deep -n 0 "$@"

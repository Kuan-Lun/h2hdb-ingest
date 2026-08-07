import json
import subprocess
import sys
from pathlib import Path

from h2hdb import H2HDB, CoreConfig, DatabaseConfig
from PIL import Image


def _run(*arguments: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "h2hdb_ingest.bootstrap",
            *(str(argument) for argument in arguments),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_bootstrap_catalog_initializes_one_fresh_catalog(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    core = CoreConfig(
        database=DatabaseConfig(sql_type="sqlite", database=str(database_path))
    )
    H2HDB(core).migrate()
    download_path = tmp_path / "download"
    gallery = download_path / "Bootstrap Gallery [301]"
    gallery.mkdir(parents=True)
    (gallery / "galleryinfo.txt").write_text(
        "\n".join(
            (
                "Title: Bootstrap Gallery",
                "Upload Time: 2026-08-07 10:00",
                "Uploaded By: bootstrap-user",
                "Downloaded: 2026-08-07 11:00",
                "Tags: artist:Bootstrap Artist, language:english",
                "Uploader's Comments:",
                "One-shot bootstrap test",
                "Downloaded from E-Hentai Galleries by the Hentai@Home "
                "Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    Image.new("RGB", (8, 8), (10, 20, 30)).save(gallery / "001.png")
    config_path = tmp_path / "ingest.json"
    config_path.write_text(
        json.dumps(
            {
                "core": {
                    "database": {
                        "sql_type": "sqlite",
                        "database": str(database_path),
                    }
                },
                "paths": {
                    "download_path": str(download_path),
                    "cbz_path": str(tmp_path / "cbz"),
                    "artifact_store_path": str(tmp_path / "artifacts"),
                    "max_image_short_side": 8,
                    "hash_workers": 1,
                },
            }
        ),
        encoding="utf-8",
    )

    first = _run("--config", config_path)
    assert first.returncode == 0, first.stderr
    assert "publications=1" in first.stdout
    assert H2HDB(core).get_catalog_revision().revision >= 1

    second = _run("--config", config_path)
    assert second.returncode == 2
    assert "already run" in second.stderr

"""Manual real-codec memory probe; creates temporary 100 MP gradient sources.

Run with the candidate ingest environment, outside the bounded merge profile:
    .venv/bin/python scripts/benchmark-source-images.py

RSS includes Python, native libraries, and final encoding. It is an observation
on these fixtures, not a hard memory bound for arbitrary source images.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic

import pyvips

from h2hdb_ingest.artifact import ArtifactRenderPolicy, load_source_page_image
from h2hdb_ingest.source_image import SOURCE_IMAGE_NATIVE_VERSIONS


def _measure(path: Path) -> None:
    import resource

    started = monotonic()
    with path.open("rb") as stream:
        with load_source_page_image(stream, policy=ArtifactRenderPolicy()) as image:
            encoded = BytesIO()
            image.save(encoded, format="JPEG", quality=90, optimize=True)
            dimensions = image.size
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        rss *= 1024
    print(
        json.dumps(
            {
                "source": path.name,
                "source_encoded_bytes": path.stat().st_size,
                "output_dimensions": dimensions,
                "output_encoded_bytes": len(encoded.getvalue()),
                "elapsed_seconds": round(monotonic() - started, 3),
                "peak_rss_mib": round(rss / (1024 * 1024), 1),
                "native_versions": SOURCE_IMAGE_NATIVE_VERSIONS,
            }
        ),
        flush=True,
    )


def _fixtures(root: Path) -> tuple[Path, ...]:
    xy = pyvips.Image.xyz(10_000, 10_000)
    x, y = xy[0], xy[1]
    image = (
        (x / 39)
        .bandjoin([y / 39, (x + y) / 78])
        .cast("uchar")
        .copy(interpretation="srgb")
    )
    files: list[Path] = []
    for progressive in (False, True):
        jpeg = root / ("progressive.jpg" if progressive else "sequential.jpg")
        png = root / ("interlaced.png" if progressive else "sequential.png")
        image.jpegsave(str(jpeg), Q=90, interlace=progressive)
        image.pngsave(str(png), compression=1, interlace=progressive)
        files.extend((jpeg, png))
    return tuple(files)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if sys.platform == "win32":
        parser.error("this manual peak-RSS probe requires POSIX resource.getrusage")
    if arguments.worker is not None:
        _measure(arguments.worker)
        return
    with TemporaryDirectory(prefix="h2hdb-source-image-benchmark-") as temporary:
        for path in _fixtures(Path(temporary)):
            subprocess.run(
                (sys.executable, __file__, "--worker", str(path)),
                check=True,
                timeout=60,
            )


if __name__ == "__main__":
    main()

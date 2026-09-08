from __future__ import annotations

from h2hdb import ArtifactFailureContext

from h2hdb_ingest.artifact_errors import (
    PageFailureContext,
    attach_image_dimensions,
    attach_page_failure_context,
    format_artifact_failure,
    get_page_failure_context,
)


def test_worker_context_retains_original_error_and_header_dimensions() -> None:
    decoder_error = ValueError("cannot decode image")
    attach_image_dimensions(
        decoder_error, width=10000, height=10000, source_kind="pngload_source"
    )
    failure = RuntimeError("page rendering failed")
    failure.__cause__ = decoder_error
    attach_page_failure_context(
        failure,
        source_position=4,
        source_name=b"004.png",
        expected_size_bytes=512,
    )
    assert type(failure) is RuntimeError
    assert failure.__cause__ is decoder_error
    assert get_page_failure_context(failure) == PageFailureContext(
        4, b"004.png", 512, 10000, 10000, "pngload_source"
    )


def test_failure_log_identifies_unicode_gallery_and_exact_worker_page() -> None:
    failure = ValueError("cannot decode image")
    attach_page_failure_context(
        failure,
        source_position=4,
        source_name="插圖.png".encode(),
        expected_size_bytes=512,
        width=10000,
        height=10000,
        source_kind="pngload_source",
    )
    line = format_artifact_failure(
        failure, context=ArtifactFailureContext(712, ("galleries",), ("作品", "第二冊"))
    )
    assert line is not None
    assert 'gid=712 gallery_folder="/galleries/作品/第二冊"' in line
    assert 'file="插圖.png" source_bytes=512 source_position=4' in line
    assert 'source_kind="pngload_source"' in line
    assert "image_width=10000 image_height=10000 image_pixels=100000000" in line
    assert 'error_type="ValueError" reason="cannot decode image"' in line


def test_failure_log_escapes_line_breaks_controls_and_bounds_reason() -> None:
    failure = OSError("failed\nforged log\u2028\u202e" + "x" * 20000)
    attach_page_failure_context(
        failure,
        source_position=1,
        source_name=b"page\n.jpg",
        expected_size_bytes=16,
    )
    line = format_artifact_failure(
        failure, context=ArtifactFailureContext(7, ("root",), ("gallery\nname",))
    )
    assert line is not None
    assert "\n" not in line
    assert "\u2028" not in line
    assert "\u202e" not in line
    assert r"gallery\nname" in line
    assert r"page\n.jpg" in line
    assert r"failed\nforged log\u2028\u202e" in line
    assert "[truncated]" in line
    assert len(line) < 5000


def test_source_read_failure_uses_core_member_context_without_worker_guess() -> None:
    line = format_artifact_failure(
        OSError("cannot read source"),
        context=ArtifactFailureContext(7, ("root",), ("gallery",), b"002.jpg", 123),
    )
    assert line is not None
    assert 'file="002.jpg" source_bytes=123' in line
    assert "image_width=" not in line
    assert "source_position=" not in line


def test_diagnostics_do_not_invent_gallery_for_unrelated_failure() -> None:
    failure = RuntimeError("database unavailable")
    assert format_artifact_failure(failure) is None
    assert get_page_failure_context(failure) is None


def test_page_context_lookup_terminates_on_cyclic_cause_and_keeps_first_member() -> (
    None
):
    failure = RuntimeError("cycle")
    failure.__cause__ = failure
    assert get_page_failure_context(failure) is None
    attach_page_failure_context(
        failure, source_position=2, source_name=b"exact.jpg", expected_size_bytes=12
    )
    attach_page_failure_context(
        failure, source_position=3, source_name=b"wrong.jpg", expected_size_bytes=13
    )
    assert get_page_failure_context(failure) == PageFailureContext(2, b"exact.jpg", 12)

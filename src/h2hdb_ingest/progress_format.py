"""Readable operational summaries, with separate diagnostic records."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .progress import ProgressSnapshot

_PHASE_LABELS = {
    "startup_check": "Validating the database and library at startup",
    "coordination": "Checking for pending ingest work",
    "maintenance": "Cleaning up resources from previous work",
    "waiting_for_work": "Waiting to continue pending ingest work",
    "ingest": "Starting an ingest batch",
    "policy": "Checking the ingest settings",
    "recovery": "Recovering an interrupted publication",
    "source": "Preparing gallery data for this batch",
    "analysis": "Analyzing gallery selection and duplicate pages",
    "publication": "Preparing and publishing the catalog",
}

_OPERATION_LABELS = {
    "initialize_storage": "Initializing the library storage",
    "inspect_storage": "Checking the library storage",
    "claim_ingest": "Acquiring permission to start an ingest batch",
    "waiting_for_ingest_lease": "Waiting for the current ingest owner to release its lease",
    "waiting_for_source_quiet_period": "Waiting for source changes to settle",
    "waiting_for_library_cleanup": "Waiting for library cleanup",
    "waiting_for_catalog_cleanup": "Waiting for database cleanup",
    "waiting_for_staging_slot": "Waiting for a free staging slot",
    "waiting_for_staging_capacity": "Waiting for staging capacity",
    "library_cleanup": "Cleaning up unused library resources",
    "catalog_cleanup": "Cleaning up unused database records",
    "waiting_publication_guard": "Waiting for permission to publish the library",
    "source_prepare": "Preparing the complete gallery inventory and selecting this batch",
    "source_discovery": "Discovering gallery folders",
    "source_discovery_verify": "Rechecking discovered folders",
    "source_discovery_commit": "Finishing the folder inventory",
    "source_discovery_transfer": "Copying the gallery inventory into the batch plan",
    "source_discovery_order": "Sorting the complete gallery inventory",
    "source_batch_selection": "Selecting existing and new galleries for this batch",
    "source_batch_order": "Sorting the selected galleries",
    "source_discovery_cleanup": "Removing the temporary gallery inventory",
    "source_source_freeze": "Freezing the selected gallery observations",
    "source_freeze": "Freezing the selected gallery observations",
    "source_completion_marker": "Checking whether gallery metadata has changed",
    "source_gallery_observation": "Reading gallery metadata and indexing its files",
    "source_file_read": "Reading and verifying source file contents",
    "source_issue": "Loading the next source database operation",
    "source_prepare_step": "Preparing the next source database operation",
    "source_commit": "Saving source observations to the database",
    "analysis_prepare": "Preparing database analysis of galleries and pages",
    "analysis_issue": "Loading the next gallery analysis operation",
    "analysis_prepare_step": "Analyzing gallery selection and duplicate pages",
    "analysis_commit": "Saving gallery and page analysis results",
    "publication_issue": "Loading the next catalog publication operation",
    "recovery_issue": "Loading the next publication recovery operation",
    "archive_preflight": "Checking source pages before building a CBZ",
    "archive_render_pages": "Rendering pages for a CBZ",
    "archive_write_pages": "Writing rendered pages into a CBZ",
    "archive_inspect": "Verifying the completed CBZ contents",
    "archive_copy": "Writing the verified CBZ to staging storage",
    "presentation_inspect": "Checking the catalog presentation and cover",
    "thumbnail_render": "Rendering a catalog thumbnail",
    "library_install": "Installing prepared library files",
    "library_remove_stale": "Removing library files no longer selected",
    "library_stage_write": "Writing a prepared file to staging storage",
    "library_layout": "Checking the library directory layout",
    "library_state_lock_wait": "Waiting for the library state lock",
    "library_state": "Updating the library state",
    "library_publication_lock_wait": "Waiting for the library publication lock",
    "library_activation": "Activating the prepared library files",
    "library_storage": "Accessing library storage",
    "library_protection_lock_wait": "Waiting for the library resource protection lock",
    "library_protect": "Protecting library resources during publication",
}

# Only counters with a useful, precise user-facing meaning appear at INFO.
_COUNTER_LABELS = {
    "galleries_discovered": "gallery folders discovered",
    "gallery_indexes_built": "gallery file indexes built",
    "file_observations_completed": "source files read and verified",
    "source_bytes_read": "source bytes read",
    "pages_rendered": "pages rendered",
    "pages_written": "pages written into CBZs",
    "archives_rendered": "CBZs rendered",
    "presentations_rendered": "catalog presentations prepared",
    "library_resources_reconciled": "library files installed or reconciled",
    "library_resources_removed": "obsolete library files removed",
    "publication_batches_finalized": "catalog batches published",
}


def format_progress(
    event: str,
    snapshot: ProgressSnapshot,
    previous: ProgressSnapshot | None,
    *,
    status: str | None = None,
) -> str:
    """Describe observed work without exposing implementation identifiers."""
    lead = {
        "periodic": "Ingest progress",
        "phase_started": "Ingest stage started",
        "phase_ended": "Ingest stage ended",
        "work_finished": {
            "completed": "Ingest work completed",
            "failed": "Ingest work failed",
            "retry": "Ingest work will be retried",
            "stopped": "Ingest work stopped",
        }.get(status or "", "Ingest work ended"),
    }[event]
    name = snapshot.operation or snapshot.phase
    label = _OPERATION_LABELS.get(name, _PHASE_LABELS.get(name))
    if label is None:
        label = _describe_operation(name)
    parts = [f"{lead}: {label}"]
    if snapshot.operation_completed is not None:
        unit = snapshot.operation_unit or "items"
        if snapshot.operation_total is None:
            parts.append(
                f"{snapshot.operation_completed:,} {unit} completed (total unknown)"
            )
        else:
            parts.append(
                f"{snapshot.operation_completed:,} / {snapshot.operation_total:,} {unit} completed"
            )
    elif event == "periodic":
        count = "completion count unavailable for this operation"
        if snapshot.operation_total is not None:
            count += f" (total {snapshot.operation_total:,} {snapshot.operation_unit or 'items'})"
        parts.append(count)
    if event == "periodic":
        parts.append(_interval_summary(snapshot, previous))
        parts.append(
            f"last measured advance {_duration(snapshot.last_progress_age_seconds)} ago"
        )
    if snapshot.operation is not None:
        parts.append(
            f"current operation elapsed {_duration(snapshot.operation_elapsed_seconds)}"
        )
    parts.append(f"work elapsed {_duration(snapshot.elapsed_seconds)}")
    parts.extend(_results(snapshot))
    return "; ".join(parts)


def format_diagnostics(
    event: str, snapshot: ProgressSnapshot, *, status: str | None = None
) -> str:
    """Keep exact tokens and every counter available to DEBUG consumers."""
    parts = [
        "ingest_progress",
        f"event={event}",
        f"generation={snapshot.generation}",
        f"phase={snapshot.phase}",
        f"elapsed_seconds={snapshot.elapsed_seconds:.1f}",
        f"phase_elapsed_seconds={snapshot.phase_elapsed_seconds:.1f}",
        f"last_progress_age_seconds={snapshot.last_progress_age_seconds:.1f}",
    ]
    if snapshot.operation is not None:
        parts.extend(
            (
                f"operation={snapshot.operation}",
                f"operation_generation={snapshot.operation_generation}",
                f"operation_elapsed_seconds={snapshot.operation_elapsed_seconds:.1f}",
            )
        )
    for name, value in (
        ("operation_completed", snapshot.operation_completed),
        ("operation_total", snapshot.operation_total),
        ("operation_unit", snapshot.operation_unit),
    ):
        if value is not None:
            parts.append(f"{name}={value}")
    if status is not None:
        parts.append(f"status={status}")
    parts.extend(f"counter.{name}={value}" for name, value in snapshot.counters)
    return " ".join(parts)


def _describe_operation(name: str) -> str:
    for prefix, purpose in (
        ("publication_", "catalog publication"),
        ("recovery_", "interrupted publication recovery"),
    ):
        if name.startswith(prefix):
            detail = name.removeprefix(prefix)
            for suffix, verb in (("_prepare", "Preparing"), ("_commit", "Saving")):
                if detail.endswith(suffix):
                    words = detail.removesuffix(suffix).replace("_", " ")
                    return f"{verb} {words} for {purpose}"
    return name.replace("_", " ").replace("-", " ").capitalize()


def _interval_summary(
    snapshot: ProgressSnapshot, previous: ProgressSnapshot | None
) -> str:
    if previous is None or previous.generation != snapshot.generation:
        seconds = snapshot.elapsed_seconds
        old: dict[str, int] = {}
        reference = "since work started"
    else:
        seconds = max(0.0, snapshot.elapsed_seconds - previous.elapsed_seconds)
        old = dict(previous.counters)
        reference = "since previous report"
        if (
            snapshot.operation_generation == previous.operation_generation
            and snapshot.operation_completed is not None
            and previous.operation_completed is not None
        ):
            delta = snapshot.operation_completed - previous.operation_completed
            return (
                f"since previous report ({_duration(seconds)}): "
                f"{delta:+,} {snapshot.operation_unit or 'items'} completed"
            )
    changes = []
    current = dict(snapshot.counters)
    for key, label in _COUNTER_LABELS.items():
        delta = current.get(key, 0) - old.get(key, 0)
        if delta:
            changes.append(f"{delta:+,} {label}")
    if not changes:
        if snapshot.operation_completed is not None:
            if reference == "since work started":
                return (
                    f"current operation: {snapshot.operation_completed:,} "
                    f"{snapshot.operation_unit or 'items'} completed "
                    f"in {_duration(snapshot.operation_elapsed_seconds)}"
                )
            return (
                f"current operation started since previous report ({_duration(seconds)}); "
                f"{snapshot.operation_completed:,} {snapshot.operation_unit or 'items'} completed"
            )
        return f"{reference} ({_duration(seconds)}): no completed-item counts available"
    return f"{reference} ({_duration(seconds)}): {', '.join(changes)}"


def _results(snapshot: ProgressSnapshot) -> list[str]:
    counts = dict(snapshot.counters)
    parts = []
    if "batch_new_gallery_limit" in counts:
        parts.append(f"batch limit {counts['batch_new_gallery_limit']:,} new galleries")
    if "batch_selected_galleries" in counts:
        parts.append(
            f"galleries in this batch {counts['batch_selected_galleries']:,} (existing and new)"
        )
    if counts.get("cbz_enabled") or "archives_rendered" in counts:
        parts.append(f"CBZs rendered this work {counts.get('archives_rendered', 0):,}")
    if counts.get("publication_batches_finalized", 0):
        parts.append(
            f"catalog batches published {counts['publication_batches_finalized']:,}"
        )
    elif snapshot.phase in {
        "ingest",
        "policy",
        "recovery",
        "source",
        "analysis",
        "publication",
    }:
        parts.append("catalog publication pending")
    return parts


def _duration(seconds: float) -> str:
    whole = int(seconds)
    hours, remainder = divmod(whole, 3600)
    minutes, seconds_part = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds_part}s"
    if minutes:
        return f"{minutes}m {seconds_part}s"
    return f"{seconds_part}s"

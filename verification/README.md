# Library reliability

This guide explains the verification evidence behind `h2hdb-ingest` and what it
means when you operate a library. You do not need to install proof tools or run
the verification suite to use the service. For installation, configuration,
recovery, and library relocation, start with the [user guide](../README.md).

## What is covered

| Concern | Evidence in this repository | Meaning for library users |
| --- | --- | --- |
| Interrupted publication | [Activation model](tla/CbzLibraryActivation.tla) and [library tests](../tests/test_library.py) | Publication is staged, and unfinished activation keeps readers fenced until recovery completes. |
| Parallel page processing | [Ordered-rendering proofs](lean/OrderedPageRendering.lean), [model](tla/OrderedPageRendering.tla), and [artifact tests](../tests/test_artifact_vnext.py) | Changing worker concurrency preserves page order under the stated deterministic-rendering assumptions. |
| Service shutdown and restart | [Lifecycle proofs](lean/IngestRuntimeLifecycle.lean), [model](tla/IngestRuntimeLifecycle.tla), and [process recovery tests](../tests/test_runtime_process_lifecycle.py) | The evidence includes graceful termination and forced termination at named publication-preparation boundaries. |
| Moving a complete library | [Relocation proofs](lean/LibraryRelocation.lean), [model](tla/LibraryRelocation.tla), and [relocation tests](../tests/test_library_relocation.py) | The offline tool preserves the library identity and checks managed file contents before accepting their new filesystem identities. |
| Reusing previous work | [Source-index proofs](lean/GalleryIndexReuse.lean), [library-authority proofs](lean/LibraryAuthorityReuse.lean), and [archive-inspection proofs](lean/ArchiveInspectionReuse.lean) | Reuse depends on matching recorded authority and byte checks; a caller's digest alone is not proof that a file is unchanged. |
| Changes affecting other books | [Incremental-equivalence proofs](lean/IncrementalEquivalence.lean) and [independent reference tests](../tests/test_vnext_incremental_state_machine.py) | A changed gallery may affect deduplication or spam decisions elsewhere in the collection, requiring other books to be rebuilt or removed. |

These links lead to the underlying evidence for readers who want to inspect it.
They describe checked properties and test scenarios, not a certification of any
particular deployment or a record that every test ran for every release.

## How to interpret verification results

**Lean proofs** establish mathematical statements under explicit assumptions.
For example, ordered rendering assumes a deterministic page result and exact
indexed worker results. Reuse proofs assume the specified unchanged-byte or
exact-equality conditions. They do not prove that image libraries, Python,
SQLite, MariaDB, or the operating system satisfy every assumption.

**TLA+ model checking** explores all reachable states of the selected finite
configuration. The required `Small` profiles cover bounded examples; the larger
`Deep` profile is separate. Passing either profile is not an unbounded proof for
arbitrary collection size. The [tool manifest](tools.lock.toml) records the
versions and checksums used by the verification commands.

**Runtime tests** exercise the application using synthetic galleries, temporary
libraries, byte comparisons, and injected failures. They connect specific
implementation paths to the modeled rules. They cannot simulate every filesystem,
storage device, concurrent external writer, or power-loss event.

The normal full check uses a bounded offline pytest selection, Lean verification,
small TLA+ profiles, and an installed-package pipeline smoke. The smoke checks
real CBZ and thumbnail output from the built ingest wheel. Its default environment
shares installed development dependencies; it is not a fresh installation of
every dependency. Manual deep tests, private source collections, and live MariaDB
cases are separate evidence. A successful normal check does not imply those ran.

The [process supervision model](tla/PytestProcessSupervision.tla) concerns the
test runner's deadline and child-process cleanup. It is evidence about the test
infrastructure, not a guarantee that an ingest operation finishes within five
minutes. Windows process ownership is checked separately by the platform test
job.

## Practical limits

- **Keep one writer.** Library safety depends on ingest owning its private and
  coordination directories. Directory identity is checked at operation
  boundaries; those checks cannot prevent an external writer swapping a path
  between a check and its next filesystem operation.
- **Keep backups.** Signal-recovery tests cover SIGTERM and SIGKILL at named
  boundaries. They do not establish arbitrary power-loss or storage-device
  durability, and cannot recover bytes lost by the storage device.
- **Allow time and disk space.** Bounded batches and worker counts limit units
  of work, not total wall-clock time or peak process memory. Large individual
  galleries still need source checks and temporary storage. Verification does
  not establish linear processing time for arbitrarily large galleries.
- **Treat relocation as offline maintenance.** The relocation checks cover
  journal-managed resources and their authorized staging names. Unrelated files
  are preserved; successful verification does not certify those files.
- **Distinguish the model from the catalog authority.** Incremental-analysis
  examples here are independent reference models. The installed `h2hdb` core
  performs production analysis and deduplication.

If a release or investigation reports verification results, look for the exact
profile, backend, tested boundaries, and any skipped cases. A result from a
small synthetic SQLite library is different evidence from a live MariaDB run
or a measurement of your own collection.

For startup failures, interrupted work, and upgrade instructions, return to the
[troubleshooting and maintenance guide](../README.md#troubleshooting).

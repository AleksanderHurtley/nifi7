# NiFi7 flows

This repository holds the scripts, documentation, examples, and operational
assets for the flows on the `nifi7` server. Each flow has its own top-level
NiFi process group and a matching directory here.

| Flow | Directory | Status |
|---|---|---|
| film → bevaring | [film_bevaring/](film_bevaring/README.md) | Existing flow: SAM-FS extraction, fixity validation, RAWcooked migration, and preservation ingest |
| video → eksternvideo | [video_eksternvideo/](video_eksternvideo/README.md) | Prepared for development |
| film → produksjon | [film_produksjon/](film_produksjon/README.md) | Prepared for development |

Directory names use lowercase words separated by underscores to keep paths
easy to use in commands.

For the two new flows, collect small metadata files and source inventories
in each flow's `examples/sam-fs/` directory. Record media filenames, sizes,
and archive members so we can develop the E-ARK mapping without downloading
large payloads. Optional complete packages or partial downloads belong in
[film_produksjon/local-packages/](film_produksjon/local-packages/README.md) or
[video_eksternvideo/local-packages/](video_eksternvideo/local-packages/README.md).
Those directories are ignored by Git except for their READMEs.

## Working on a flow

Keep each flow's scripts and supporting assets inside its directory:

- Numbered stage directories (such as `01_Initialize/`) follow the process
  groups within that flow. Numbering starts independently for each flow.
- The flow's `README.md` describes its purpose, stages, and current status.
- Add `docs/`, `examples/`, `catalog_dump/`, `reporting/`, and
  `deletion-batches/` as needed for that flow.
- Document the flow's NiFi process-group name/ID, parameter contexts, storage
  paths, and database/DPS configuration in its own documentation.

The existing film → bevaring implementation is the reference for the current
conventions. Its storage paths, production-line filter, DPS contract, and
reporting assets are specific to that flow. Confirm the corresponding values
when implementing another flow. Extract shared code only when multiple flows
actually use it.

Paths in a flow's documentation are relative to that flow directory unless
stated otherwise. Commands labeled "from the repository root" include the
flow directory explicitly.

## Existing flow after the move

All previous flow content now lives under `film_bevaring/`, including
`Add event.groovy`, documentation, examples, catalog samples, reporting, and
deletion-batch evidence. For example,
`01_Initialize/01_Initialize Flowfile.groovy` is now
`film_bevaring/01_Initialize/01_Initialize Flowfile.groovy`.

See the [film → bevaring overview](film_bevaring/docs/FLOW_OVERVIEW.md) and
[deployment notes](film_bevaring/docs/OPERATIONS.md) when working on the
existing flow.

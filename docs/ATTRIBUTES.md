# Attributes reference

This document defines common FlowFile attributes used across the pipeline.
Names are case-sensitive.

## Identity
- `package.name` (required)
  - Meaning: package identifier used as `packageId` in events and database stats
  - Example: `digifilm_685019_20140516_FP--20080348`

## Paths
- `events.payload.path` (required)
  - Meaning: absolute path to NDJSON file where events are appended
- `workDirectoryRoot` (parameter context)
  - Meaning: root working directory for staging/processing
  - Example: `/mnt/fs00/LB/workspace/nifi_protected`
- `deleteDirectory`
  - Meaning: directory path used by cleanup/deletion steps (when applicable)
- `dpxmeta.manifest.path`
  - Meaning: absolute path to DPX manifest XML generated from metadata extraction
  - Example: `/.../metadata/preservation/dpx/<package.name>_dpx_manifest.xml`

(Additional path attributes may exist in flow; list them here as you standardize.)

## Event fields (set by scripts before the Add Event step)
- `event.datetime`
  - ISO-8601 timestamp (prefer UTC, seconds)
  - Example: `2026-02-11T14:01:41Z`
- `event.type` (required)
  - Values: `transfer`, `fixity check`, `migration`, …
- `event.outcome`
  - Values: `success` / `failure` (preferred)
- `event.detail`
  - Human-readable explanation designed to be understood years later
- `event.outcomeDetail` (optional)
  - Semi-structured key/value string (e.g. `k=v;k=v`), used for technical provenance

## Agent overrides (optional; default is “NiFi preservation ingest flow”)
- `agent.name`
- `agent.type`
- `agent.version` (optional; omit if blank)

Conventions:
- Most events: agent defaults to the flow identity.
- RAWcooked migration event: set agent explicitly:
  - `agent.name=RAWcooked`, `agent.type=software`, `agent.version=<rawcookedVersion>`

## Timing / stats (used for DB updates)
- `fetch.start`, `fetch.end`, `fetch.duration`
- `checksum.start`, `checksum.end`, `checksum.duration`
- `rawcooked.start`, `rawcooked.end`, `rawcooked.duration`
- `eark.start`, `eark.end`, `eark.duration` (if used)
- `total.pipeline.start`, `total.pipeline.end`, `total.pipeline.duration`
- `package.size.start`, `package.size.end`
- `rawcooked.compression.bytes`, `rawcooked.compression.ratio` (if used)

## DPX Manifest XML contract
- Location: `metadata/preservation/dpx/<package.name>_dpx_manifest.xml`
- Root:
  - `<dpxManifest packageId=\"<package.name>\" checksumAlgorithm=\"MD5\" createdFrom=\"metadata.extract.dir\">`
- Batch/file structure:
  - `<batches><batch id=\"batchPRE|batch0001...\">`
  - `<file name=\"<dpx file name>\" md5=\"<32 lower-case hex>\"/>`
- File order is authoritative for downstream batch processing.

## Error reporting (recommended)
- `error.message`
  - Short, user-readable cause (path missing, mismatch, command failed, etc.)
- `error.details` (optional)
  - Longer context (first mismatch, counts, stderr excerpt)
  - Recommended cap: 2048 characters
- `error.stage`
  - Dot-notation stage source for failures
  - Example: `fetch.tar.untar`, `checksum.verify`, `rawcooked.run`
- `events.append.status`
  - `OK` / `FAIL` from the add-event appender
- `events.append.error`
  - Error text if append fails

## Delivery margin / buffer control (DPS-2)
- `error.buffer.capacity`
  - Maximum retained failures for manual review (default `2` if missing)
- `error.buffer.op`
  - Operation for buffer manager: `acquire` (default) or `release`
- `error.buffer.action`
  - Gate decision: `retain` or `autocleanup`
- `error.buffer.slot`
  - Reserved slot id when retained (for example `1`, `2`)
- `error.buffer.reason`
  - Human-readable decision reason from the gate
- `error.buffer.count`
  - Number of occupied slots at decision time
- `error.autocleaned`
  - `true` if package is auto-cleaned because buffer is full, else `false`
- `error.buffer.release.released`
  - `true` if one or more slots were released for the package
- `error.buffer.release.slots`
  - Comma-separated released slot ids (empty if none)

## Package cleanup attributes
- `cleanup.package.status`
  - `OK` or `FAIL`
- `cleanup.package.deleted.any`
  - `true` if at least one package directory was removed
- `cleanup.package.exists.payloads|transfer|work`
  - Whether target package directory existed in each base path
- `cleanup.package.deleted.payloads|transfer|work`
  - Whether each target package directory was deleted
- `cleanup.package.path.payloads|transfer|work`
  - Resolved target path used for each cleanup root

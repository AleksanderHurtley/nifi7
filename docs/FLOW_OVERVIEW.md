# Flow overview

This describes the logical NiFi pipeline implemented by the scripts in this repository.
(Processor names may differ; script names are the source of truth.)

## Pipeline stages

### 0) Initialize
**Goal:** create workspace directories and initialize core attributes.
Scripts:
- `01_Initialize/01_Initialize Flowfile.groovy`
- `01_Initialize/02_Initialize Directories.groovy`

Outputs (typical):
- `package.name` (packageId)
- `workDirectoryRoot` (parameter context)
- derived local paths for staging, rawcooked output, metadata output
- event payload path (NDJSON): `events.payload.path`
- `total.pipeline.start` (epoch ms — pipeline-wide timing anchor)

Failure:
- route to failure, set `error.message`

---

### 1) Fetch files from SAM-FS
**Goal:** stage required content + metadata from SAM-FS archival storage to local disk.
Scripts (by content type):
- Metadata:
  - `02_Fetch files from SAM-FS/03_Metadata files/01_Fetch and Organize metadata files.groovy`
  - `02_Fetch files from SAM-FS/03_Metadata files/02_Extract checksums + batch map.groovy`
  - `02_Fetch files from SAM-FS/03_Metadata files/03_Cleanup extracted metadata.groovy`
  - `02_Fetch files from SAM-FS/03_Metadata files/04_Extract creation event from METS.groovy` (parses `mix:GeneralCaptureInformation` + `mix:ScannerCapture` from deprecated METS, stages a `creation` event when found; best-effort)
- Tar:
  - `02_Fetch files from SAM-FS/01_Tar files/01_Tar fragments.groovy`
  - `02_Fetch files from SAM-FS/01_Tar files/02_Fetch and Untar.groovy`
- Timing/stat updates:
  - `02_Fetch files from SAM-FS/04_Set fetch.end, fetch.duration, package.size.start.groovy`

Events (recommended):
- `eventType=creation` (best-effort, when METS contains mix data) — agent is the scanner hardware (e.g. Scanity), set by `04_Extract creation event from METS.groovy`.
- `eventType=transfer` — agent is Apache NiFi (default), set via NiFi UpdateAttribute. Detail in Norwegian: "Overført pakke fra Oracle HSM (SAM-FS) til lokalt arbeidsområde for videre behandling; DPX-sjekksummer verifisert mot SAM-FS-metadata med md5sum (GNU coreutils); Opprettet E-ARK SIP med commons-ip2."

Notes:
- Avoid commands like `tree/du/find` on SAM-FS if it can trigger recall/staging.
- Metadata extraction writes a single DPX manifest:
  - `metadata/preservation/dpx/<package.name>_dpx_manifest.xml`
  - root: `dpxManifest`
  - key fields: batch id, DPX file name, MD5 checksum.
- Audio files are fetched outside this repo (e.g. rsync/copy step in the NiFi flow).

---

### 2) Fixity validation (DPX)
**Goal:** confirm bit-level integrity after transfer by comparing computed MD5 against SAM-FS metadata checksums.
Script:
- `03_Checksum/01_Verify DPX Checksums.groovy`
Input metadata source:
- `dpxmeta.manifest.path` (or fallback path under `metadata.preservation.dpx.dir`)

Note: the fixity check is recorded as part of the lumped `transfer` event
(see `docs/EVENTS.md`); this stage does not emit its own event by default.
On failure, set `event.outcome=failure` on the transfer event and include
mismatch context in `error.message` (and optionally append to `event.detail`).

Outputs (typical):
- `checksum.start`, `checksum.end`, `checksum.durationMs`

---

### 3) Catalog
**Goal:** build the DPS submission payload from package metadata.
Scripts:
- `04_Catalog/01_Create submission body.groovy`

Behaviour:
- Searches for a `_WORK_*.xml` file in `metadata.descriptive.dir` (and `transfer.dir/metadata/descriptive` as fallback)
- Extracts title (prefers `Originaltittel`); falls back to `"Unknown Title"` if no XML found
- Writes a JSON submission payload to `submission.payload.path`

Outputs:
- `submission.payload.status` (`OK` / `FAIL`)
- `submission.payload.path` (confirmed)

---

### 4) RAWcooked migration
**Goal:** convert DPX image sequences to preservation-friendly FFV1-in-Matroska (MKV).
Scripts:
- `05_RAWcooked/01_Batch.groovy`
- `05_RAWcooked/02_RAWcooked.groovy`
- `05_RAWcooked/03_Cleanup.groovy`

Event:
- `eventType=migration`
- Agent: RAWcooked (with version + agentNote)
- Detail (Norwegian, parameters included per DPS guidance):
  "Konvertering av DPX-bildesekvenser til FFV1-video i Matroska (MKV) container for langtidsbevaring. Brukte parametere: command=rawcooked --all --check -y; tool.ffmpeg=\<version>; tool.ffprobe=\<version>."

Outcome detail (result-only):
- container + videoCodec + videoProfile (from ffprobe)

Outputs (typical):
- `rawcooked.start`, `rawcooked.end`, `rawcooked.durationMs`
- `rawcooked.total.input.bytes`, `rawcooked.total.output.bytes`, `rawcooked.total.compression_ratio`
- `rawcooked.batches.count`, `rawcooked.batches.names`, `rawcooked.outputs.names`

---

### 5) Generate checksums for outputs
**Goal:** compute MD5 checksums for all files under `metadata/` and `representations/` in the work directory.
Script:
- `06_Generate checksums/01_Generate checksums.groovy`

Outputs:
- `checksums.md5.path` — path to written checksum file
- `checksums.md5.count`, `checksums.md5.totalBytes`, `checksums.md5.durationMs`
- `checksums.md5.scope` = `metadata+representations`

Note: `checksums.md5.totalBytes` is used downstream as `package.size.end`.

---

### 6) EARK packaging + DPS delivery
**Goal:** package the representation as an EARK-compliant AIP and deliver to DPS.
(Implemented outside this repo in the NiFi flow.)

Outputs expected on the flowfile after this stage:
- `eark.start`, `eark.end`, `eark.duration`

---

### 7) DPS-2 delivery margin control
**Goal:** avoid uncontrolled backpressure on large failures while preserving a small manual review buffer.
Scripts:
- `07_dps-2/01_Failure Buffer Gate.groovy`

Behavior:
- Delivery failures pass through a buffer gate that marks:
  - `error.buffer.action=retain` (manual review queue)
  - `error.buffer.action=autocleanup` (send directly to cleanup/failure finalization)
- Retain up to 2 failed packages by default (`error.buffer.capacity`, default `2`)
- Use one shared processor instance for both operations:
  - `error.buffer.op=acquire` on failure path (default if missing)
  - `error.buffer.op=release` on manual retry/discard path
- On release, slot ownership for `package.name` is removed before continuing

---

### 8) Finalize stats
**Goal:** compute end-of-pipeline timing totals and output size, then record all stats into the database.
Scripts:
- `08_Finalize stats/01_Finalize stats.groovy`
- `08_Finalize stats/02_PutSQL.sql` (PutSQL processor configuration)

Behavior:
- Sets `total.pipeline.end` (epoch ms)
- Computes `total.pipeline.duration = end - start`
- Sets `package.size.end` from `checksums.md5.totalBytes`

Stats written to `DI_PARAMETER`:

| Parameter name | Source attribute |
|---|---|
| `fetch.start/end/duration` | set during fetch stage |
| `checksum.start/end/duration` | `checksum.durationMs` |
| `rawcooked.start/end/duration` | `rawcooked.durationMs` |
| `rawcooked.input/output.bytes` | `rawcooked.total.input/output.bytes` |
| `rawcooked.compression.ratio` | `rawcooked.total.compression_ratio` |
| `eark.start/end/duration` | set outside this repo |
| `pipeline.start/end/duration` | `total.pipeline.*` |
| `package.size.start/end` | fetch stage / `checksums.md5.totalBytes` |

---

### 9) Package cleanup
**Goal:** remove large package directories from local disk when cleanup is required.
Script:
- `09_Package cleanup/01_Delete package directories.groovy`

Behavior:
- Removes `/fc1/payloads/<package.name>`, `/fc1/transfer/<package.name>`, `/fc1/work/<package.name>`
- Uses strict path guardrails and standard `error.*` handling

---

## Event emission
Events are appended as NDJSON via a shared "add event" script (not listed here).
Scripts set attributes:
- `event.datetime`, `event.type`, `event.outcome`, `event.detail`, optional `event.outcomeDetail`
- optional agent overrides: `agent.name`, `agent.type`, `agent.version`

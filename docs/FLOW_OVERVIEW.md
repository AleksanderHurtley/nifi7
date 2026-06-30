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
- `eventType=transfer` — agent is Apache NiFi, set via NiFi UpdateAttribute (see [examples/nifi-updateattribute-transfer.json](../examples/nifi-updateattribute-transfer.json) for the full property list). Documents extraction out of the legacy SAM-FS archive + fixity (the producer→DPS transfer is logged separately by the DPS itself). Detail in Norwegian: "Pakke hentet ut av Nasjonalbibliotekets eldre arkivlager Oracle HSM (SAM-FS) for rearkivering; integritet bekreftet ved at DPX-sjekksummer ble verifisert mot SAM-FS-metadata med md5sum (GNU coreutils)."
- `eventType=information package creation` — agent is `nifi-nb-eark-nar.EarkSIPGenerator` (the NB-developed NiFi processor; the DPS team documents what it consists of), set via NiFi UpdateAttribute placed right after `EarkSIPGenerator` and routed to `Add event.groovy` (see [examples/nifi-updateattribute-information-package-creation.json](../examples/nifi-updateattribute-information-package-creation.json)). This is a separate event from `creation`, emitted later in the flow. Detail in Norwegian: "Opprettet SIP i henhold til Nasjonalbibliotekets profil SIP 1.0."

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

### 6) Submission body
**Goal:** parse the descriptive catalog XML records and assemble the full DPS submission body (JSON) onto the flowfile content.
Scripts (run in order; the first four enrich `metadata.*` attributes, the last renders the body):
- `07_Submission body/01_Get Work.groovy` — parses `_WORK_*.xml`: main/alternative titles, credits (creators/contributors/publishers), dates, work relations (`segment_of`, `av_series`, `related_object`), production countries, descriptions
- `07_Submission body/02_Get DIGITAL ITEM.groovy` — parses `_DIGITAL_ITEM_*.xml` (excluding `_DIGITAL_ITEM_PART_`): language usage
- `07_Submission body/03_Get Analog item part bar_code.groovy` — parses `_ANALOG_ITEM_PART_*.xml`: shelf barcode(s)
- `07_Submission body/04_Get digital item part.groovy` — parses `_DIGITAL_ITEM_PART_*.xml`: PID URNs, `CopiedFrom`/`CopiedTo` relations, provenance
- `07_Submission body/05_Make submission body.groovy` — assembles all `metadata.*` attributes into the final submission JSON and writes it to the flowfile content

Input metadata source:
- `metadata.descriptive.dir` (with `transfer.dir/metadata/descriptive` as fallback)
- Required attributes on each script: `package.name`, `submission.payload.path`, `metadata.descriptive.dir`

Attributes set (consumed by `05_Make submission body.groovy`):
- from `01`: `metadata.mainTitle`, `metadata.alternativeTitles`, `metadata.creators`, `metadata.contributors`, `metadata.publishers`, `metadata.dates`, `metadata.countries`, `metadata.descriptions`, `metadata.workRelations`
- from `02`: `metadata.languages`
- from `03`: `metadata.barcode`
- from `04`: `metadata.pidDataUrns`, `metadata.digitalItemPartRelations`, `metadata.provenance`

Outputs:
- flowfile content = DPS submission body JSON (`objectId`, `priority`, `metadata { type, identifier, title, alternative, creator, contributor, publisher, spatial, date, relation, language, provenance, description }`)
- on missing attributes or parse error: `submission.payload.status=FAIL` + `submission.payload.error`

Note: this expands on the lightweight payload built in `04_Catalog/01_Create submission body.groovy`,
producing the full descriptive metadata body delivered to the DPS.

---

### 7) EARK packaging + DPS delivery
**Goal:** package the representation as an EARK-compliant AIP and deliver to DPS.
(Implemented outside this repo in the NiFi flow.)

Outputs expected on the flowfile after this stage:
- `eark.start`, `eark.end`, `eark.duration`

---

### 8) DPS-2 delivery margin control
**Goal:** avoid uncontrolled backpressure on large failures while preserving a small manual review buffer.
Scripts:
- `08_dps-2/01_Failure Buffer Gate.groovy`

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

### 9) Finalize stats
**Goal:** compute end-of-pipeline timing totals and output size, then record all stats into the database.
Scripts:
- `09_Finalize stats/01_Finalize stats.groovy`
- `09_Finalize stats/02_PutSQL.sql` (PutSQL processor configuration)

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

Reporting:
- `reporting/sql/01_create_transfer_package_metrics_view.sql` pivots
  flow rows into `v_transfer_package_metrics`, using `DIGITIZED_ITEM`
  (`Pline_id = 79`, `STATUS = 'Catalog.done'`) as the package source and
  enriching each row from helper views over `DI_EVENT` and `DI_PARAMETER`.
- `reporting/grafana/transfer-flow-dashboard.json` reads that view and filters
  Catalog completion panels by `completed_time`, RAWcooked throughput/ETA
  panels by `rawcooked_end_time`, and duration panels by
  `processing_completed_time` (`eark.end` when available).

---

### 10) Package cleanup
**Goal:** remove large package directories from local disk when cleanup is required.
Script:
- `10_Package cleanup/01_Delete package directories.groovy`

Behavior:
- Removes `/fc1/payloads/<package.name>`, `/fc1/transfer/<package.name>`, `/fc1/work/<package.name>`
- Uses strict path guardrails and standard `error.*` handling

---

## Event emission
Events are appended as NDJSON via a shared "add event" script (not listed here).
Scripts set attributes:
- `event.datetime`, `event.type`, `event.outcome`, `event.detail`, optional `event.outcomeDetail`
- optional agent overrides: `agent.name`, `agent.type`, `agent.version`

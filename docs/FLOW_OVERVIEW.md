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
- Tar:
  - `02_Fetch files from SAM-FS/01_Tar files/01_Tar fragments.groovy`
  - `02_Fetch files from SAM-FS/01_Tar files/02_Fetch and Untar.groovy`
- Audio:
  - `02_Fetch files from SAM-FS/02_Audio files/01_Copy audio files.bash`
- Timing/stat updates:
  - `02_Fetch files from SAM-FS/04_Set fetch.end, fetch.duration, package.size.start.groovy`

Event (recommended):
- `eventType=transfer`
- Detail: “Package transferred from SAM-FS archival storage to local staging storage for preservation processing.”

Notes:
- Avoid commands like `tree/du/find` on SAM-FS if it can trigger recall/staging.
- Metadata extraction writes a single DPX manifest:
  - `metadata/preservation/dpx/<package.name>_dpx_manifest.xml`
  - root: `dpxManifest`
  - key fields: batch id, DPX file name, MD5 checksum.

---

### 2) Fixity validation (DPX)
**Goal:** confirm bit-level integrity after transfer by comparing computed MD5 against SAM-FS metadata checksums.
Script:
- `03_Checksum/01_Verify DPX Checksums.groovy`
Input metadata source:
- `dpxmeta.manifest.path` (or fallback path under `metadata.preservation.dpx.dir`)

Event detail (selected):
- “Fixity check after transfer: computed MD5 checksums for DPX files in the staging area and compared them to MD5 values recorded in SAM-FS archival storage metadata to confirm bit-level integrity.”

Outputs (typical):
- `checksum.start`, `checksum.end`, `checksum.duration`
- set event fields for the add-event appender

Failure:
- set `event.outcome=failure` and include mismatch context in `error.message` (and/or `event.detail`)
- route failure

---

### 3) RAWcooked migration
**Goal:** convert DPX image sequences to preservation-friendly FFV1-in-Matroska (MKV).
Scripts:
- `04_RAWcooked/01_Batch.groovy`
- `04_RAWcooked/02_RAWcooked.groovy`
- `04_RAWcooked/03_Cleanup.groovy`

Event:
- `eventType=migration`
- Agent: RAWcooked (with version)
- Detail: “DPX image sequences converted to FFV1 video wrapped in a Matroska (MKV) container for preservation storage.”

Outcome detail:
- command + ffmpeg/ffprobe versions + container/codec/profile (probe results)

Outputs (typical):
- `rawcooked.start`, `rawcooked.end`, `rawcooked.duration`
- compression bytes/ratio (if computed)
- output file sizes/paths

---

### 4) (Optional) Generate checksums for outputs
**Goal:** compute checksums for artifacts produced by processing (e.g., MKV).
Script:
- `05_Generate checksums/01_Generate checksums.groovy`

---

### 5) Database update / finalization
**Goal:** record timings and stats into DI_PARAMETER (or equivalent reporting table).
(Implemented outside this repo or as a SQL processor configuration.)

---

### 6) DPS-2 delivery margin control
**Goal:** avoid uncontrolled backpressure on large failures while preserving a small manual review buffer.
Scripts:
- `06_dps-2/01_Failure Buffer Gate.groovy`
- `06_dps-2/02_Release Buffer Slot.groovy`

Behavior:
- Delivery failures pass through a buffer gate that marks:
  - `error.buffer.action=retain` (manual review queue)
  - `error.buffer.action=autocleanup` (send directly to cleanup/failure finalization)
- Retain up to 2 failed packages by default (`error.buffer.capacity`, default `2`)
- On manual retry/discard, release slot ownership before continuing

---

### 7) Package cleanup
**Goal:** remove large package directories from local disk when cleanup is required.
Script:
- `07_Package cleanup/01_Delete package directories.groovy`

Behavior:
- Removes `/fc1/payloads/<package.name>`, `/fc1/transfer/<package.name>`, `/fc1/work/<package.name>`
- Uses strict path guardrails and standard `error.*` handling

---

## Event emission
Events are appended as NDJSON via a shared “add event” script (not listed here).
Scripts set attributes:
- `event.datetime`, `event.type`, `event.outcome`, `event.detail`, optional `event.outcomeDetail`
- optional agent overrides: `agent.name`, `agent.type`, `agent.version`

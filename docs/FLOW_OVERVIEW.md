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

---

### 2) Fixity validation (DPX)
**Goal:** confirm bit-level integrity after transfer by comparing computed MD5 against SAM-FS metadata checksums.
Script:
- `03_Checksum/01_Verify DPX Checksums.groovy`

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

## Event emission
Events are appended as NDJSON via a shared “add event” script (not listed here).
Scripts set attributes:
- `event.datetime`, `event.type`, `event.outcome`, `event.detail`, optional `event.outcomeDetail`
- optional agent overrides: `agent.name`, `agent.type`, `agent.version`

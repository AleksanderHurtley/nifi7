# SAM-FS → Preservation Ingest (NiFi)

This repository contains Groovy and Bash scripts used in an Apache NiFi flow that:
1) transfers packages out of SAM-FS archival storage into a local staging/work area,
2) validates fixity using checksums recorded in a DPX metadata manifest XML generated from SAM-FS metadata,
3) migrates DPX image sequences to FFV1-in-Matroska using RAWcooked,
4) records preservation events (NDJSON) and pipeline timing stats for reporting.

The NiFi flow is organized into logical process groups matching the folder structure in this repo.

## Key concepts

- **Package**: a logical preservation unit identified by `package.name` / `packageId`.
- **SAM-FS archival storage**: source storage for content and associated metadata/checksum files.
- **Staging area**: local disk paths where packages are copied/extracted for processing.
- **Events**: appended as NDJSON records to a file pointed to by `events.payload.path`.
- **Stats**: timestamps/durations/size and tool stats written to a database at the end.

## Repo structure

- `01_Initialize/` – create directories and initial flowfile attributes
- `02_Fetch files from SAM-FS/` – gather metadata + content (tar, audio, etc.) into staging
- `03_Checksum/` – verify DPX fixity after transfer using SAM-FS checksum metadata
- `04_Catalog/` – build the DPS submission payload (objectId, metadata, title) from descriptive XML
- `05_RAWcooked/` – batch conversion + cleanup, and emits migration event (RAWcooked as agent)
- `06_Generate checksums/` – compute checksums for outputs if needed downstream
- `07_dps-2/` – delivery-stage failure margin (single buffer manager script for acquire/release)
- `08_Finalize stats/` – compute end-of-pipeline timing/size totals and write stats to the database
- `09_Package cleanup/` – package-level cleanup of staging/output folders

Note: the `information package creation` and `transfer` events, E-ARK packaging
(`EarkSIPGenerator`), and the events/submission upload to the DPS API happen in the
NiFi flow itself (see `docs/EVENTS.md` and `docs/FLOW_OVERVIEW.md`), not as folders here.
- `reporting/` – MySQL/MariaDB view, validation queries, and Grafana dashboard for completed package reporting

## Operating assumptions

- Servers are offline (no internet); deployment is done via SSH + file transfer.
- RAWcooked/ffmpeg/ffprobe are installed and available on the NiFi host(s).
- SAM-FS is mounted and accessible on the NiFi source host.

## Where to start

Read:
- `docs/FLOW_OVERVIEW.md`
- `docs/ATTRIBUTES.md`
- `docs/EVENTS.md`
- `docs/OPERATIONS.md`
- `reporting/README.md`

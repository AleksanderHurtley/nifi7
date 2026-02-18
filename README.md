# SAM-FS → Preservation Ingest (NiFi)

This repository contains Groovy and Bash scripts used in an Apache NiFi flow that:
1) transfers packages out of SAM-FS archival storage into a local staging/work area,
2) validates fixity using checksums recorded in SAM-FS metadata (DPX/MD5),
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

- `Initialize/` – create directories and initial flowfile attributes
- `Fetch files from SAM-FS/` – gather metadata + content (tar, audio, etc.) into staging
- `Checksum/` – verify DPX fixity after transfer using SAM-FS checksum metadata
- `Generate checksums/` – compute checksums for outputs if needed downstream
- `RAWcooked/` – batch conversion + cleanup, and emits migration event (RAWcooked as agent)

## Operating assumptions

- Servers are offline (no internet); deployment is done via SSH + file transfer.
- RAWcooked/ffmpeg/ffprobe are installed and available on the NiFi host(s).
- SAM-FS is mounted and accessible on the NiFi source host.

## Where to start

Read:
- `FLOW_OVERVIEW.md`
- `ATTRIBUTES.md`
- `EVENTS.md`
- `OPERATIONS.md`

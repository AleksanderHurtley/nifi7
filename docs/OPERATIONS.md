# Operations / deployment

## Environment
- Servers have no internet access; deploy via SSH/VPN file transfer.
- Ensure system has:
  - Java (matching NiFi version requirements)
  - RAWcooked + ffmpeg + ffprobe (for migration stage)
  - SAM-FS mounted (on the host doing the fetch)
  - adequate disk space on staging volume

## NiFi configuration expectations
- Parameter Context includes at least:
  - `workDirectoryRoot` (staging root)
  - any tool paths if not in PATH (rawcooked, ffmpeg, ffprobe)
- Events file:
  - attribute `events.payload.path` must point to a writable file
  - events are appended as NDJSON (one line per event)

## SSL / HTTPS notes
- If running NiFi with HTTPS, ensure certificate CN/SAN matches the hostname used in the browser (SNI).
- For internal CA usage:
  - keystore contains PrivateKeyEntry for host with chain length >= 2 (host + CA)
  - truststore contains CA cert

## Runtime safety
- Avoid recursive filesystem inspection commands on SAM-FS (`tree`, `du`, `find`) unless data is staged; they may trigger recall/staging and hang in D-state.
- When deleting large directories, prefer controlled deletion via ExecuteStreamCommand with explicit path checks.

## Database stats update
At pipeline end, write DI_PARAMETER values using consistent keys:
- `fetch.start/end/duration`
- `checksum.start/end/duration`
- `rawcooked.start/end/duration`
- `rawcooked.compression.bytes/ratio` (if available)
- `eark.start/end/duration` (if used)
- `total.pipeline.start/end/duration`
- `package.size.start/end`

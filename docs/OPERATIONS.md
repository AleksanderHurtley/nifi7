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
- `rawcooked.input/output.bytes`
- `rawcooked.compression.ratio`
- `eark.start/end/duration` (if used)
- `total.pipeline.start/end/duration`
- `package.size.start/end`

## Grafana reporting
Reporting assets live under `reporting/`.

Deploy in this order:
- Run `reporting/sql/01_create_transfer_package_metrics_view.sql` in the schema
  that contains `DIGITIZED_ITEM`, `DI_EVENT`, and `DI_PARAMETER`.
  The script creates helper views for parameters/events plus the final
  `v_transfer_package_metrics` view used by Grafana.
- Replace the password in
  `reporting/sql/02_create_grafana_readonly_user.template.sql`, then run it in
  the same schema to create `grafana_transfer_ro`.
- Run `reporting/sql/03_reporting_validation_queries.sql` and compare one
  sampled `DI_ID` against the raw `DI_PARAMETER` rows.
- If the reporting views are slow, run
  `reporting/sql/04_index_diagnostics.sql` and verify the expected indexes are
  used before browsing the full views in phpMyAdmin.
- Add a Grafana MySQL/MariaDB datasource using `grafana_transfer_ro`.
  Set the datasource session timezone to the timezone used by
  `DI_EVENT.COMPLETED`; use `+00:00` only if those datetimes are stored as UTC.
- Import `reporting/grafana/transfer-flow-dashboard.json` and bind
  `DS_TRANSFER_DB` to that datasource.
- Use the dashboard's `ETA rate window days` variable to switch the ETA between
  the last 30, 7, and 1 day.

The reporting view is completed-package only. It includes `DIGITIZED_ITEM`
rows where `Pline_id = 79` and `STATUS = 'Catalog.done'`, enriches them with
`DI_EVENT` and `DI_PARAMETER`, and period dashboard panels filter on
`completed_time`. RAWcooked throughput and ETA panels use `rawcooked_end_time`
to avoid Catalog queue-release spikes. Processing-duration panels use
`processing_completed_time`, which prefers `eark.end` and falls back to
`rawcooked.end`, `pipeline.end`, then Catalog completion. The dashboard shows
active stage time, which sums fetch, checksum, RAWcooked, and E-ARK durations
without wait time between stages. Overall progress panels intentionally ignore
the time picker.

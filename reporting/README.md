# Transfer flow reporting

This directory contains the database view and Grafana dashboard for reporting
completed transfer-flow packages from `DIGITIZED_ITEM`, `DI_EVENT`, and the
existing `DI_PARAMETER` stats table.

## Database setup

Run the SQL scripts in the database schema that contains `DIGITIZED_ITEM`,
`DI_EVENT`, and `DI_PARAMETER`:

1. `sql/01_create_transfer_package_metrics_view.sql`
2. `sql/02_create_grafana_readonly_user.template.sql`
3. `sql/03_reporting_validation_queries.sql`
4. `sql/04_index_diagnostics.sql` if the views are slow

Before running the read-only user template, replace
`CHANGE_ME_STRONG_PASSWORD` with a generated password.
The template uses `DATABASE()` for the grant, so select the target schema first
with `USE <schema_name>;`.

The view script creates three views:

- `v_transfer_parameter_metrics` pivots `DI_PARAMETER` into one row per `DI_ID`.
- `v_transfer_event_metrics` pivots `DI_EVENT` into Transfer, Bevaring, and
  Catalog status/timestamp columns.
- `v_transfer_package_metrics` is the package-level view used by Grafana.

The final view only includes `DIGITIZED_ITEM` rows where `Pline_id = 79` and
`STATUS = 'Catalog.done'`. It also exposes the full source-size target from
`/global/film/00/film/bevaring/`: `source_input_total_gib = 2709504`
(`2646 TiB`).

## Source table structures

The reporting SQL is based on these production table structures.

### `DIGITIZED_ITEM`

| Column | Type | Null | Default | Notes |
|---|---:|---|---|---|
| `ID` | `int(11)` | no | none | Primary key, auto increment |
| `DESCRIPTION` | `varchar(255)` | yes | `NULL` | Indexed; package-like value such as `transfer_<package>` |
| `TYPE` | `varchar(255)` | yes | `NULL` | Example: `Film` |
| `STATUS` | `varchar(255)` | yes | `NULL` | Done packages use `Catalog.done` |
| `CREATED_BY` | `varchar(255)` | yes | `NULL` | Creator/user/host |
| `CREATED_DATE` | `datetime` | yes | `NULL` | Item creation timestamp |
| `PRIORITY` | `int(2)` | yes | `NULL` | Queue priority |
| `EXTRA_INFO` | `text` | yes | `NULL` | Optional free text |
| `PARENT_ITEM` | `int(11)` | yes | `NULL` | Indexed parent reference |
| `NAME_ID` | `int(11)` | yes | `NULL` | Indexed name reference |
| `Pline_id` | `int(11) unsigned` | yes | `NULL` | Indexed; transfer flow uses `79` |
| `language` | `varchar(255)` | yes | `NULL` | Optional language value |

Reporting filter:

```sql
WHERE Pline_id = 79
  AND STATUS = 'Catalog.done'
```

### `DI_EVENT`

| Column | Type | Null | Default | Notes |
|---|---:|---|---|---|
| `DI_ID` | `int(11)` | no | none | Indexed link to `DIGITIZED_ITEM.ID` |
| `EVENT_ID` | `int(11)` | no | none | Primary key, auto increment |
| `TYPE` | `varchar(255)` | yes | `NULL` | Relevant values: `Transfer`, `Bevaring`, `Catalog` |
| `STATUS` | `varchar(255)` | yes | `NULL` | Indexed; done events use `done` |
| `STATUS_TEXT` | `text` | yes | `NULL` | Optional status details |
| `STARTED` | `datetime` | yes | `NULL` | Event start timestamp |
| `STARTED_BY` | `varchar(255)` | yes | `NULL` | Start actor |
| `COMPLETED` | `datetime` | yes | `NULL` | Indexed event completion timestamp |
| `EVENT_COMPLETED_BY` | `varchar(255)` | yes | `NULL` | Completion actor |
| `EVENT_CONFIGURATION` | `text` | yes | `NULL` | Optional event configuration |
| `step_id` | `bigint(20)` | yes | `NULL` | Indexed step identifier |

Reporting uses `TYPE IN ('Transfer', 'Bevaring', 'Catalog')` and treats
`TYPE = 'Catalog' AND STATUS = 'done'` as the event-level completion signal.

### `DI_PARAMETER`

| Column | Type | Null | Default | Notes |
|---|---:|---|---|---|
| `DI_ID` | `int(11)` | no | none | Keyed link to `DIGITIZED_ITEM.ID` |
| `PARAMETER_NAME` | `varchar(255)` | no | none | Keyed parameter name |
| `PARAMETER_VALUE` | `varchar(500)` | yes | `NULL` | String value cast by the reporting view |

The NiFi flow writes one row per metric parameter. The reporting view pivots
these names into typed columns:

```text
fetch.start, fetch.end, fetch.duration
checksum.start, checksum.end, checksum.duration
rawcooked.start, rawcooked.end, rawcooked.duration
rawcooked.input.bytes, rawcooked.output.bytes, rawcooked.compression.ratio
eark.start, eark.end, eark.duration
pipeline.start, pipeline.end, pipeline.duration
package.size.start, package.size.end
```

## Grafana setup

1. Add a MySQL/MariaDB data source that connects with
   `grafana_transfer_ro`.
   Set the data source session timezone to the timezone used by
   `DI_EVENT.COMPLETED`; use `+00:00` only if those datetimes are stored as UTC.
2. Import `grafana/transfer-flow-dashboard.json`.
3. When Grafana asks for `DS_TRANSFER_DB`, select the MySQL/MariaDB data
   source.
4. Use the `ETA rate window days` dashboard variable to choose whether ETA is
   based on the last `30`, `7`, or `1` day.

Period panels for package completion use `completed_time` with Grafana's
`$__timeFilter(completed_time)` macro. RAWcooked throughput and ETA use
`rawcooked_end_time`, so queue releases in Catalog do not look like processing
throughput. Processing-duration panels use `processing_completed_time`, which
prefers `eark.end` and falls back to `rawcooked.end`, `pipeline.end`, then
Catalog completion. This avoids treating packages held in the final Catalog
queue as if they were still processing. Overall progress panels intentionally
ignore the dashboard time picker so they show total progress against the source
folder. `completed_time` remains the Catalog.done/reporting completion time.

## Metrics

- Completed packages: `COUNT(DISTINCT di_id)` from `Catalog.done` items in
  `Pline_id = 79`
- Processed input: `SUM(rawcooked_input_bytes) / 1024^3`
- Total input target: `MAX(source_input_total_gib)` from the reporting view
- Processed output: `SUM(package_size_end_bytes) / 1024^3`
- RAWcooked input/output: `rawcooked_input_bytes` and
  `rawcooked_output_bytes`
- RAWcooked bytes saved: `rawcooked_input_bytes - rawcooked_output_bytes`
- Weighted compression ratio:
  `SUM(rawcooked_input_bytes) / SUM(rawcooked_output_bytes)`
- Progress percent:
  `processed_input_gib / source_input_total_gib * 100`
- Remaining input:
  `source_input_total_gib - processed_input_gib`
- ETA days:
  `remaining_input_gib / selected_recent_rawcooked_input_gib_per_day`
- Active stage duration: `fetch.duration + checksum.duration +
  rawcooked.duration + eark.duration`; this excludes wait time between stages

Processed output uses the final package size from `package.size.end`, matching
the agreed reporting definition.

## Search performance

For good response times, verify these indexes exist in production:

- `DIGITIZED_ITEM(Pline_id, STATUS, ID)`
- `DI_EVENT(DI_ID, TYPE, STATUS, COMPLETED)`
- `DI_PARAMETER(DI_ID, PARAMETER_NAME)`

The phpMyAdmin screenshots already show indexes on `DIGITIZED_ITEM.Pline_id`
and `DI_EVENT.DI_ID`; the remaining check is mostly to make sure
`DI_PARAMETER` is indexed for the pivot.

Avoid using phpMyAdmin's Browse action on the final view as a performance test:
it can request a broad, unfiltered result. Prefer bounded checks such as:

```sql
SELECT COUNT(*) AS rows_found
FROM v_transfer_package_metrics;

SELECT *
FROM v_transfer_package_metrics
WHERE completed_time >= NOW() - INTERVAL 30 DAY
ORDER BY completed_time DESC
LIMIT 10;
```

If those are slow, run `sql/04_index_diagnostics.sql` and check whether the
queries are using indexes on `DIGITIZED_ITEM`, `DI_EVENT`, and `DI_PARAMETER`.

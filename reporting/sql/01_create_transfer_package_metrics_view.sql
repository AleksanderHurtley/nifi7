-- Creates Grafana-friendly reporting views for transfer-flow items.
-- Run this in the schema that contains DIGITIZED_ITEM, DI_EVENT, and
-- DI_PARAMETER.
--
-- This version intentionally avoids CASE expressions because phpMyAdmin's SQL
-- editor can mark valid CASE syntax as invalid on some MariaDB/MySQL versions.

DROP VIEW IF EXISTS v_transfer_package_metrics;
DROP VIEW IF EXISTS v_transfer_event_metrics;
DROP VIEW IF EXISTS v_transfer_parameter_metrics;

CREATE VIEW v_transfer_parameter_metrics AS
SELECT
  base.`DI_ID` AS di_id,

  CAST(NULLIF(TRIM(pipeline_start.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS pipeline_start_epoch_ms,
  CAST(NULLIF(TRIM(pipeline_end.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS pipeline_end_epoch_ms,

  CAST(NULLIF(TRIM(fetch_start.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS fetch_start_epoch_ms,
  CAST(NULLIF(TRIM(fetch_end.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS fetch_end_epoch_ms,
  CAST(NULLIF(TRIM(checksum_start.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS checksum_start_epoch_ms,
  CAST(NULLIF(TRIM(checksum_end.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS checksum_end_epoch_ms,
  CAST(NULLIF(TRIM(rawcooked_start.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS rawcooked_start_epoch_ms,
  CAST(NULLIF(TRIM(rawcooked_end.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS rawcooked_end_epoch_ms,
  CAST(NULLIF(TRIM(eark_start.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS eark_start_epoch_ms,
  CAST(NULLIF(TRIM(eark_end.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS eark_end_epoch_ms,

  CAST(NULLIF(TRIM(fetch_duration.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS fetch_duration_ms,
  CAST(NULLIF(TRIM(checksum_duration.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS checksum_duration_ms,
  CAST(NULLIF(TRIM(rawcooked_duration.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS rawcooked_duration_ms,
  CAST(NULLIF(TRIM(eark_duration.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS eark_duration_ms,
  CAST(NULLIF(TRIM(pipeline_duration.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS pipeline_duration_ms,

  CAST(NULLIF(TRIM(package_size_start.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS package_size_start_bytes,
  CAST(NULLIF(TRIM(package_size_end.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS package_size_end_bytes,
  CAST(NULLIF(TRIM(rawcooked_input.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS rawcooked_input_bytes,
  CAST(NULLIF(TRIM(rawcooked_output.`PARAMETER_VALUE`), '') AS DECIMAL(20, 0)) AS rawcooked_output_bytes,
  CAST(NULLIF(TRIM(rawcooked_ratio.`PARAMETER_VALUE`), '') AS DECIMAL(20, 8)) AS rawcooked_recorded_compression_ratio
FROM (
  SELECT DISTINCT p0.`DI_ID`
  FROM `DIGITIZED_ITEM` AS di0
  STRAIGHT_JOIN `DI_PARAMETER` AS p0
    ON p0.`DI_ID` = di0.`ID`
  WHERE di0.`Pline_id` = 79
    AND di0.`STATUS` = 'Catalog.done'
    AND p0.`PARAMETER_NAME` IN (
    'fetch.start',
    'fetch.end',
    'fetch.duration',
    'checksum.start',
    'checksum.end',
    'checksum.duration',
    'rawcooked.start',
    'rawcooked.end',
    'rawcooked.duration',
    'rawcooked.input.bytes',
    'rawcooked.output.bytes',
    'rawcooked.compression.ratio',
    'eark.start',
    'eark.end',
    'eark.duration',
    'pipeline.start',
    'pipeline.end',
    'pipeline.duration',
    'package.size.start',
    'package.size.end'
  )
) AS base
LEFT JOIN `DI_PARAMETER` AS pipeline_start
  ON pipeline_start.`DI_ID` = base.`DI_ID` AND pipeline_start.`PARAMETER_NAME` = 'pipeline.start'
LEFT JOIN `DI_PARAMETER` AS pipeline_end
  ON pipeline_end.`DI_ID` = base.`DI_ID` AND pipeline_end.`PARAMETER_NAME` = 'pipeline.end'
LEFT JOIN `DI_PARAMETER` AS fetch_start
  ON fetch_start.`DI_ID` = base.`DI_ID` AND fetch_start.`PARAMETER_NAME` = 'fetch.start'
LEFT JOIN `DI_PARAMETER` AS fetch_end
  ON fetch_end.`DI_ID` = base.`DI_ID` AND fetch_end.`PARAMETER_NAME` = 'fetch.end'
LEFT JOIN `DI_PARAMETER` AS checksum_start
  ON checksum_start.`DI_ID` = base.`DI_ID` AND checksum_start.`PARAMETER_NAME` = 'checksum.start'
LEFT JOIN `DI_PARAMETER` AS checksum_end
  ON checksum_end.`DI_ID` = base.`DI_ID` AND checksum_end.`PARAMETER_NAME` = 'checksum.end'
LEFT JOIN `DI_PARAMETER` AS rawcooked_start
  ON rawcooked_start.`DI_ID` = base.`DI_ID` AND rawcooked_start.`PARAMETER_NAME` = 'rawcooked.start'
LEFT JOIN `DI_PARAMETER` AS rawcooked_end
  ON rawcooked_end.`DI_ID` = base.`DI_ID` AND rawcooked_end.`PARAMETER_NAME` = 'rawcooked.end'
LEFT JOIN `DI_PARAMETER` AS eark_start
  ON eark_start.`DI_ID` = base.`DI_ID` AND eark_start.`PARAMETER_NAME` = 'eark.start'
LEFT JOIN `DI_PARAMETER` AS eark_end
  ON eark_end.`DI_ID` = base.`DI_ID` AND eark_end.`PARAMETER_NAME` = 'eark.end'
LEFT JOIN `DI_PARAMETER` AS fetch_duration
  ON fetch_duration.`DI_ID` = base.`DI_ID` AND fetch_duration.`PARAMETER_NAME` = 'fetch.duration'
LEFT JOIN `DI_PARAMETER` AS checksum_duration
  ON checksum_duration.`DI_ID` = base.`DI_ID` AND checksum_duration.`PARAMETER_NAME` = 'checksum.duration'
LEFT JOIN `DI_PARAMETER` AS rawcooked_duration
  ON rawcooked_duration.`DI_ID` = base.`DI_ID` AND rawcooked_duration.`PARAMETER_NAME` = 'rawcooked.duration'
LEFT JOIN `DI_PARAMETER` AS eark_duration
  ON eark_duration.`DI_ID` = base.`DI_ID` AND eark_duration.`PARAMETER_NAME` = 'eark.duration'
LEFT JOIN `DI_PARAMETER` AS pipeline_duration
  ON pipeline_duration.`DI_ID` = base.`DI_ID` AND pipeline_duration.`PARAMETER_NAME` = 'pipeline.duration'
LEFT JOIN `DI_PARAMETER` AS package_size_start
  ON package_size_start.`DI_ID` = base.`DI_ID` AND package_size_start.`PARAMETER_NAME` = 'package.size.start'
LEFT JOIN `DI_PARAMETER` AS package_size_end
  ON package_size_end.`DI_ID` = base.`DI_ID` AND package_size_end.`PARAMETER_NAME` = 'package.size.end'
LEFT JOIN `DI_PARAMETER` AS rawcooked_input
  ON rawcooked_input.`DI_ID` = base.`DI_ID` AND rawcooked_input.`PARAMETER_NAME` = 'rawcooked.input.bytes'
LEFT JOIN `DI_PARAMETER` AS rawcooked_output
  ON rawcooked_output.`DI_ID` = base.`DI_ID` AND rawcooked_output.`PARAMETER_NAME` = 'rawcooked.output.bytes'
LEFT JOIN `DI_PARAMETER` AS rawcooked_ratio
  ON rawcooked_ratio.`DI_ID` = base.`DI_ID` AND rawcooked_ratio.`PARAMETER_NAME` = 'rawcooked.compression.ratio';

CREATE VIEW v_transfer_event_metrics AS
SELECT
  e.`DI_ID` AS di_id,
  COALESCE(
    MAX(IF(e.`TYPE` = 'Transfer' AND e.`STATUS` = 'done', e.`STATUS`, NULL)),
    MAX(IF(e.`TYPE` = 'Transfer', e.`STATUS`, NULL))
  ) AS transfer_status,
  MAX(IF(e.`TYPE` = 'Transfer', e.`STARTED`, NULL)) AS transfer_started_time,
  MAX(IF(e.`TYPE` = 'Transfer' AND e.`STATUS` = 'done', e.`COMPLETED`, NULL)) AS transfer_completed_time,

  COALESCE(
    MAX(IF(e.`TYPE` = 'Bevaring' AND e.`STATUS` = 'done', e.`STATUS`, NULL)),
    MAX(IF(e.`TYPE` = 'Bevaring', e.`STATUS`, NULL))
  ) AS bevaring_status,
  MAX(IF(e.`TYPE` = 'Bevaring', e.`STARTED`, NULL)) AS bevaring_started_time,
  MAX(IF(e.`TYPE` = 'Bevaring' AND e.`STATUS` = 'done', e.`COMPLETED`, NULL)) AS bevaring_completed_time,

  COALESCE(
    MAX(IF(e.`TYPE` = 'Catalog' AND e.`STATUS` = 'done', e.`STATUS`, NULL)),
    MAX(IF(e.`TYPE` = 'Catalog', e.`STATUS`, NULL))
  ) AS catalog_status,
  MAX(IF(e.`TYPE` = 'Catalog', e.`STARTED`, NULL)) AS catalog_started_time,
  MAX(IF(e.`TYPE` = 'Catalog' AND e.`STATUS` = 'done', e.`COMPLETED`, NULL)) AS catalog_completed_time,
  SUM(IF(e.`TYPE` = 'Catalog' AND e.`STATUS` = 'done', 1, 0)) AS catalog_done_count
FROM `DIGITIZED_ITEM` AS di
STRAIGHT_JOIN `DI_EVENT` AS e
  ON e.`DI_ID` = di.`ID`
WHERE di.`Pline_id` = 79
  AND di.`STATUS` = 'Catalog.done'
  AND e.`TYPE` IN ('Transfer', 'Bevaring', 'Catalog')
GROUP BY e.`DI_ID`;

CREATE VIEW v_transfer_package_metrics AS
SELECT
  di.`ID` AS di_id,
  di.`DESCRIPTION` AS digitized_item_description,
  di.`DESCRIPTION` AS package_name,
  di.`TYPE` AS digitized_item_type,
  di.`STATUS` AS digitized_item_status,
  di.`CREATED_DATE` AS digitized_item_created_time,
  di.`Pline_id` AS pline_id,
  2709504 AS source_input_total_gib,
  2909307767095296 AS source_input_total_bytes,

  ev.transfer_status,
  ev.transfer_started_time,
  ev.transfer_completed_time,
  ev.bevaring_status,
  ev.bevaring_started_time,
  ev.bevaring_completed_time,
  ev.catalog_status,
  ev.catalog_started_time,
  ev.catalog_completed_time,
  ev.catalog_done_count,

  FROM_UNIXTIME(p.pipeline_start_epoch_ms / 1000.0) AS pipeline_start_time,
  FROM_UNIXTIME(p.pipeline_end_epoch_ms / 1000.0) AS pipeline_end_time,
  FROM_UNIXTIME(p.rawcooked_start_epoch_ms / 1000.0) AS rawcooked_start_time,
  FROM_UNIXTIME(p.rawcooked_end_epoch_ms / 1000.0) AS rawcooked_end_time,
  FROM_UNIXTIME(p.eark_start_epoch_ms / 1000.0) AS eark_start_time,
  FROM_UNIXTIME(p.eark_end_epoch_ms / 1000.0) AS eark_end_time,
  COALESCE(ev.catalog_completed_time, FROM_UNIXTIME(p.pipeline_end_epoch_ms / 1000.0)) AS completed_time,
  COALESCE(
    FROM_UNIXTIME(p.eark_end_epoch_ms / 1000.0),
    FROM_UNIXTIME(p.rawcooked_end_epoch_ms / 1000.0),
    FROM_UNIXTIME(p.pipeline_end_epoch_ms / 1000.0),
    ev.catalog_completed_time
  ) AS processing_completed_time,
  IF(p.pipeline_end_epoch_ms IS NULL, 0, 1) AS has_pipeline_stats,
  IF(COALESCE(p.eark_end_epoch_ms, p.rawcooked_end_epoch_ms, p.pipeline_end_epoch_ms) IS NULL, 0, 1) AS has_processing_stats,

  p.pipeline_start_epoch_ms,
  p.pipeline_end_epoch_ms,

  p.fetch_start_epoch_ms,
  p.fetch_end_epoch_ms,
  p.checksum_start_epoch_ms,
  p.checksum_end_epoch_ms,
  p.rawcooked_start_epoch_ms,
  p.rawcooked_end_epoch_ms,
  p.eark_start_epoch_ms,
  p.eark_end_epoch_ms,

  p.fetch_duration_ms / 1000.0 AS fetch_duration_seconds,
  p.checksum_duration_ms / 1000.0 AS checksum_duration_seconds,
  p.rawcooked_duration_ms / 1000.0 AS rawcooked_duration_seconds,
  p.eark_duration_ms / 1000.0 AS eark_duration_seconds,
  p.pipeline_duration_ms / 1000.0 AS pipeline_duration_seconds,
  IF(
    p.pipeline_start_epoch_ms IS NULL
      OR COALESCE(p.eark_end_epoch_ms, p.rawcooked_end_epoch_ms, p.pipeline_end_epoch_ms) IS NULL,
    NULL,
    (COALESCE(p.eark_end_epoch_ms, p.rawcooked_end_epoch_ms, p.pipeline_end_epoch_ms) - p.pipeline_start_epoch_ms) / 1000.0
  ) AS processing_duration_seconds,
  (
    COALESCE(p.fetch_duration_ms, 0)
    + COALESCE(p.checksum_duration_ms, 0)
    + COALESCE(p.rawcooked_duration_ms, 0)
    + COALESCE(p.eark_duration_ms, 0)
  ) / 1000.0 AS active_stage_duration_seconds,

  p.package_size_start_bytes,
  p.package_size_end_bytes,
  p.rawcooked_input_bytes,
  p.rawcooked_output_bytes,
  p.rawcooked_input_bytes - p.rawcooked_output_bytes AS rawcooked_saved_bytes,
  IF(
    p.rawcooked_output_bytes IS NULL OR p.rawcooked_output_bytes = 0,
    NULL,
    p.rawcooked_input_bytes / p.rawcooked_output_bytes
  ) AS rawcooked_compression_ratio,
  p.rawcooked_recorded_compression_ratio
FROM `DIGITIZED_ITEM` AS di
LEFT JOIN v_transfer_parameter_metrics AS p
  ON p.di_id = di.`ID`
LEFT JOIN v_transfer_event_metrics AS ev
  ON ev.di_id = di.`ID`
WHERE di.`Pline_id` = 79
  AND di.`STATUS` = 'Catalog.done'
  AND COALESCE(ev.catalog_completed_time, FROM_UNIXTIME(p.pipeline_end_epoch_ms / 1000.0)) IS NOT NULL;

-- Quick validation queries for the transfer reporting view.
-- Run after creating v_transfer_package_metrics.

-- 1. The view should expose only flow items from Pline_id 79 that are done in Catalog.
SELECT
  COUNT(*) AS view_rows,
  COUNT(DISTINCT di_id) AS distinct_di_ids,
  MIN(pline_id) AS min_pline_id,
  MAX(pline_id) AS max_pline_id,
  SUM(CASE WHEN digitized_item_status <> 'Catalog.done' THEN 1 ELSE 0 END) AS non_catalog_done_rows
FROM v_transfer_package_metrics;

-- 2. Compare view rows with the source DIGITIZED_ITEM done-count.
SELECT
  (SELECT COUNT(*)
   FROM DIGITIZED_ITEM
   WHERE Pline_id = 79
     AND STATUS = 'Catalog.done') AS source_catalog_done_items,
  (SELECT COUNT(*)
   FROM v_transfer_package_metrics) AS view_rows_with_completion_time;

-- 3. Compare one recent package from the typed view to raw source rows.
SET @sample_di_id := (
  SELECT di_id
  FROM v_transfer_package_metrics
  ORDER BY completed_time DESC
  LIMIT 1
);

SELECT *
FROM v_transfer_package_metrics
WHERE di_id = @sample_di_id;

SELECT ID, DESCRIPTION, TYPE, STATUS, CREATED_DATE, Pline_id
FROM DIGITIZED_ITEM
WHERE ID = @sample_di_id;

SELECT TYPE, STATUS, STARTED, COMPLETED, STARTED_BY, EVENT_COMPLETED_BY, step_id
FROM DI_EVENT
WHERE DI_ID = @sample_di_id
ORDER BY STARTED, EVENT_ID;

SELECT PARAMETER_NAME, PARAMETER_VALUE
FROM DI_PARAMETER
WHERE DI_ID = @sample_di_id
  AND PARAMETER_NAME IN (
    'pipeline.end',
    'pipeline.duration',
    'eark.end',
    'eark.duration',
    'package.size.end',
    'rawcooked.input.bytes',
    'rawcooked.output.bytes',
    'rawcooked.compression.ratio'
  )
ORDER BY PARAMETER_NAME;

-- 4. Check planned edge cases and data-quality gaps.
SELECT
  SUM(CASE WHEN has_pipeline_stats = 0 THEN 1 ELSE 0 END) AS done_without_pipeline_stats_count,
  SUM(CASE WHEN has_processing_stats = 0 THEN 1 ELSE 0 END) AS done_without_processing_stats_count,
  SUM(CASE WHEN catalog_done_count IS NULL OR catalog_done_count = 0 THEN 1 ELSE 0 END) AS catalog_done_status_without_catalog_event_count,
  SUM(CASE WHEN eark_end_time IS NULL THEN 1 ELSE 0 END) AS missing_eark_end_time_count,
  SUM(CASE WHEN eark_duration_seconds IS NULL THEN 1 ELSE 0 END) AS missing_eark_duration_count,
  SUM(CASE WHEN package_size_start_bytes IS NULL THEN 1 ELSE 0 END) AS missing_package_size_start_count,
  SUM(CASE WHEN rawcooked_output_bytes = 0 THEN 1 ELSE 0 END) AS zero_rawcooked_output_count,
  SUM(CASE WHEN rawcooked_saved_bytes < 0 THEN 1 ELSE 0 END) AS rawcooked_output_larger_than_input_count
FROM v_transfer_package_metrics;

-- 5. Sanity-check completed-package/output numbers for the last 30 days.
SELECT
  COUNT(DISTINCT di_id) AS completed_packages,
  ROUND(SUM(package_size_end_bytes) / POW(1024, 3), 2) AS output_gib,
  ROUND(SUM(rawcooked_input_bytes) / POW(1024, 3), 2) AS input_gib_from_completed_packages,
  ROUND(SUM(rawcooked_saved_bytes) / POW(1024, 3), 2) AS rawcooked_saved_gib,
  ROUND(SUM(rawcooked_input_bytes) / NULLIF(SUM(rawcooked_output_bytes), 0), 4) AS weighted_rawcooked_ratio
FROM v_transfer_package_metrics
WHERE completed_time >= NOW() - INTERVAL 30 DAY;

-- 5b. Processing duration uses E-ARK end time when available, not Catalog.done.
SELECT
  COUNT(*) AS rows_checked,
  SUM(CASE WHEN eark_end_time IS NOT NULL AND processing_completed_time = eark_end_time THEN 1 ELSE 0 END) AS rows_using_eark_end_time,
  SUM(CASE WHEN eark_end_time IS NULL AND rawcooked_end_time IS NOT NULL AND processing_completed_time = rawcooked_end_time THEN 1 ELSE 0 END) AS rows_falling_back_to_rawcooked_end_time,
  ROUND(AVG(processing_duration_seconds), 1) AS avg_processing_seconds,
  ROUND(AVG(active_stage_duration_seconds), 1) AS avg_active_stage_seconds,
  ROUND(AVG(pipeline_duration_seconds), 1) AS avg_recorded_pipeline_seconds
FROM v_transfer_package_metrics
WHERE processing_completed_time >= NOW() - INTERVAL 30 DAY;

-- 6. Estimate completion the same way the dashboard ETA panel does: progress is
--    measured in package start bytes against source_input_total_bytes, and the
--    processing rate keys on E-ARK end time (the last active stage). The dashboard
--    uses the selected time range via $__timeFilter; validation queries cannot, so
--    this spot-checks fixed 30/7/1-day windows. Values are in decimal TB (POW(1000, 4)).
SELECT
  MAX(source_input_total_bytes) / POW(1000, 4) AS source_input_total_tb,
  ROUND(SUM(package_size_start_bytes) / POW(1000, 4), 2) AS processed_input_tb,
  ROUND(GREATEST(MAX(source_input_total_bytes) - COALESCE(SUM(package_size_start_bytes), 0), 0) / POW(1000, 4), 2) AS remaining_input_tb,
  ROUND((
    SELECT COALESCE(SUM(package_size_start_bytes), 0) / POW(1000, 4) / 30
    FROM v_transfer_package_metrics
    WHERE eark_end_time >= NOW() - INTERVAL 30 DAY
  ), 2) AS input_tb_per_day_30d,
  ROUND(
    GREATEST(MAX(source_input_total_bytes) - COALESCE(SUM(package_size_start_bytes), 0), 0)
    / NULLIF((
      SELECT COALESCE(SUM(package_size_start_bytes), 0) / 30
      FROM v_transfer_package_metrics
      WHERE eark_end_time >= NOW() - INTERVAL 30 DAY
    ), 0),
    1
  ) AS eta_days_at_30d_rate,
  ROUND((
    SELECT COALESCE(SUM(package_size_start_bytes), 0) / POW(1000, 4) / 7
    FROM v_transfer_package_metrics
    WHERE eark_end_time >= NOW() - INTERVAL 7 DAY
  ), 2) AS input_tb_per_day_7d,
  ROUND(
    GREATEST(MAX(source_input_total_bytes) - COALESCE(SUM(package_size_start_bytes), 0), 0)
    / NULLIF((
      SELECT COALESCE(SUM(package_size_start_bytes), 0) / 7
      FROM v_transfer_package_metrics
      WHERE eark_end_time >= NOW() - INTERVAL 7 DAY
    ), 0),
    1
  ) AS eta_days_at_7d_rate,
  ROUND((
    SELECT COALESCE(SUM(package_size_start_bytes), 0) / POW(1000, 4)
    FROM v_transfer_package_metrics
    WHERE eark_end_time >= NOW() - INTERVAL 1 DAY
  ), 2) AS input_tb_per_day_1d,
  ROUND(
    GREATEST(MAX(source_input_total_bytes) - COALESCE(SUM(package_size_start_bytes), 0), 0)
    / NULLIF((
      SELECT COALESCE(SUM(package_size_start_bytes), 0)
      FROM v_transfer_package_metrics
      WHERE eark_end_time >= NOW() - INTERVAL 1 DAY
    ), 0),
    1
  ) AS eta_days_at_1d_rate
FROM v_transfer_package_metrics;

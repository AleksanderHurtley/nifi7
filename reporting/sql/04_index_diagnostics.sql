-- Diagnostics for reporting-view performance.
-- Run these in phpMyAdmin if v_transfer_package_metrics or
-- v_transfer_event_metrics is slow.

SHOW INDEX FROM `DIGITIZED_ITEM`;
SHOW INDEX FROM `DI_EVENT`;
SHOW INDEX FROM `DI_PARAMETER`;

EXPLAIN
SELECT COUNT(*) AS rows_found
FROM v_transfer_package_metrics;

EXPLAIN
SELECT *
FROM v_transfer_package_metrics
WHERE completed_time >= NOW() - INTERVAL 30 DAY
ORDER BY completed_time DESC
LIMIT 10;

-- Optional indexes to discuss/apply if EXPLAIN shows full scans.
-- Do not run these blindly on production during busy hours.
--
-- CREATE INDEX idx_digitized_item_pline_status_id
--   ON `DIGITIZED_ITEM` (`Pline_id`, `STATUS`, `ID`);
--
-- CREATE INDEX idx_di_event_di_type_status_completed
--   ON `DI_EVENT` (`DI_ID`, `TYPE`, `STATUS`, `COMPLETED`);
--
-- CREATE INDEX idx_di_parameter_di_name
--   ON `DI_PARAMETER` (`DI_ID`, `PARAMETER_NAME`);

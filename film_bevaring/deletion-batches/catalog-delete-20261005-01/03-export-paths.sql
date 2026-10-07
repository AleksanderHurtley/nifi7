-- Read-only. Run after batch assignment succeeds.
-- Export this query result, all rows, as CSV using Query results operations.
-- Save as package-paths.txt in this batch directory (CSV quoting is accepted).
-- Do not send for deletion until validation against fresh DPS responses passes.
SELECT TRIM(p.`PARAMETER_VALUE`) AS path
FROM `DIGITIZED_ITEM` AS di
JOIN `DI_PARAMETER` AS batch
    ON batch.`DI_ID` = di.`ID`
   AND HEX(batch.`PARAMETER_NAME`) = HEX('source.deletion.batch')
JOIN `DI_PARAMETER` AS p
    ON p.`DI_ID` = di.`ID`
   AND HEX(p.`PARAMETER_NAME`) = HEX('package_path')
WHERE di.`Pline_id` = 79
  AND HEX(batch.`PARAMETER_VALUE`) = HEX('catalog-delete-20261005-01')
ORDER BY path;

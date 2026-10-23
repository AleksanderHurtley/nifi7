-- Read-only. Run after batch assignment succeeds.
-- Export this query result, all rows, as CSV using Query results operations.
-- Save the original export as DIGITIZED_ITEM.csv in this batch directory.
-- Do not send for deletion until every path passes the consolidated DPS index check.
SELECT TRIM(p.`PARAMETER_VALUE`) AS path
FROM `DIGITIZED_ITEM` AS di
JOIN `DI_PARAMETER` AS batch
    ON batch.`DI_ID` = di.`ID`
   AND HEX(batch.`PARAMETER_NAME`) = HEX('source.deletion.batch')
JOIN `DI_PARAMETER` AS p
    ON p.`DI_ID` = di.`ID`
   AND HEX(p.`PARAMETER_NAME`) = HEX('package_path')
WHERE di.`Pline_id` = 79
  AND HEX(batch.`PARAMETER_VALUE`) = HEX('catalog-delete-20261023-01')
ORDER BY path;

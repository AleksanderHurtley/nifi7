-- Read-only. Run in the production schema used for the previous batch.
-- First result must be 0 before using this proposed batch identifier.
SELECT COUNT(*) AS existing_batch_rows
FROM `DI_PARAMETER`
WHERE HEX(`PARAMETER_NAME`) = HEX('source.deletion.batch')
  AND HEX(`PARAMETER_VALUE`) = HEX('catalog-delete-20261005-01');

-- Newly completed packages with no previous deletion-batch assignment.
-- Review before assigning a batch: candidate_rows, packages and distinct_paths
-- should agree; blank_paths and unsafe_paths should both be zero.
-- LEFT JOIN deliberately includes packages with missing package_path values.
SELECT
    COUNT(*) AS candidate_rows,
    COUNT(DISTINCT di.`ID`) AS packages,
    COUNT(DISTINCT HEX(TRIM(p.`PARAMETER_VALUE`))) AS distinct_paths,
    COALESCE(SUM(
        p.`PARAMETER_VALUE` IS NULL
        OR TRIM(p.`PARAMETER_VALUE`) = ''
    ), 0) AS blank_paths,
    COALESCE(SUM(
        p.`PARAMETER_VALUE` IS NOT NULL
        AND (
            TRIM(p.`PARAMETER_VALUE`) NOT LIKE '/global/film/00/film/bevaring/%'
            OR TRIM(p.`PARAMETER_VALUE`) = '/global/film/00/film/bevaring/'
            OR TRIM(p.`PARAMETER_VALUE`) REGEXP '(^|/)[.][.]?(/|$)'
            OR TRIM(p.`PARAMETER_VALUE`) REGEXP '[[:cntrl:]]'
        )
    ), 0) AS unsafe_paths
FROM `DIGITIZED_ITEM` AS di
LEFT JOIN `DI_PARAMETER` AS p
    ON p.`DI_ID` = di.`ID`
   AND HEX(p.`PARAMETER_NAME`) = HEX('package_path')
WHERE di.`Pline_id` = 79
  AND HEX(di.`STATUS`) = HEX('Catalog.done')
  AND EXISTS (
      SELECT 1
      FROM `DI_EVENT` AS e
      WHERE e.`DI_ID` = di.`ID`
        AND HEX(e.`TYPE`) = HEX('Catalog')
        AND HEX(e.`STATUS`) = HEX('done')
        AND e.`COMPLETED` IS NOT NULL
  )
  AND NOT EXISTS (
      SELECT 1
      FROM `DI_PARAMETER` AS previous
      WHERE previous.`DI_ID` = di.`ID`
        AND HEX(previous.`PARAMETER_NAME`) = HEX('source.deletion.batch')
  );

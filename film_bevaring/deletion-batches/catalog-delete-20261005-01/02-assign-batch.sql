-- WRITES batch membership to DI_PARAMETER. Does not delete files or mark them deleted.
-- Run once, in digitalt_prod, after reviewing 01-preview.sql results.
-- Do not run concurrent batch assignments. The unused_batch join prevents a
-- later sequential rerun from adding newly completed packages to this batch.
-- Expected from the preview: 353 rows; more may have completed since then.
INSERT INTO `DI_PARAMETER` (`DI_ID`, `PARAMETER_NAME`, `PARAMETER_VALUE`)
SELECT DISTINCT
    di.`ID`,
    'source.deletion.batch',
    'catalog-delete-20261005-01'
FROM `DIGITIZED_ITEM` AS di
JOIN `DI_PARAMETER` AS p
    ON p.`DI_ID` = di.`ID`
   AND HEX(p.`PARAMETER_NAME`) = HEX('package_path')
LEFT JOIN `DI_PARAMETER` AS unused_batch
    ON unused_batch.`PARAMETER_NAME` = 'source.deletion.batch'
   AND HEX(unused_batch.`PARAMETER_NAME`) = HEX('source.deletion.batch')
   AND unused_batch.`PARAMETER_VALUE` = 'catalog-delete-20261005-01'
   AND HEX(unused_batch.`PARAMETER_VALUE`) = HEX('catalog-delete-20261005-01')
WHERE di.`Pline_id` = 79
  AND HEX(di.`STATUS`) = HEX('Catalog.done')
  AND unused_batch.`DI_ID` IS NULL
  AND TRIM(p.`PARAMETER_VALUE`) LIKE '/global/film/00/film/bevaring/%'
  AND TRIM(p.`PARAMETER_VALUE`) <> '/global/film/00/film/bevaring/'
  AND TRIM(p.`PARAMETER_VALUE`) NOT REGEXP '(^|/)[.][.]?(/|$)'
  AND TRIM(p.`PARAMETER_VALUE`) NOT REGEXP '[[:cntrl:]]'
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

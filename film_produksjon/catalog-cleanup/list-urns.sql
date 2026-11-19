-- Read-only inventory of the production queue. Export as CSV with the urn header.
-- Include every ready row, so a malformed description is caught by the scanner.
SELECT CASE
    WHEN LEFT(`DESCRIPTION`, 9) = 'transfer_'
    THEN CONCAT('URN:NBN:no-nb_', SUBSTRING(`DESCRIPTION`, 10))
    ELSE `DESCRIPTION`
END AS urn
FROM `DIGITIZED_ITEM`
WHERE `Pline_id` = 82
  AND `STATUS` = 'Transfer.ready'
ORDER BY `ID`;

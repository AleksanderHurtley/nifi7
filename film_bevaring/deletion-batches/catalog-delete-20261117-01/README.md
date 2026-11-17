# Catalog deletion batch 2026-11-17-01

The read-only preview on 17 November 2026 found 384 newly completed
`Catalog.done` packages on production line 79 in `digitalt_prod`. The batch ID
`catalog-delete-20261117-01` had zero existing assignments. All 384 paths
are unique and under `/global/film/00/film/bevaring/`. There is no path or
object-ID overlap with the September, October 5, or October 23 batches.

`candidates.csv` is the original read-only phpMyAdmin export.
`package-paths.txt` contains all 384 exported paths in order, one per line.
Every basename matches exactly one `PRESERVED` record with an archive ID in
the November 17 DPS responses for contract `91c5`.
`validation-summary.json` captures the counts, page evidence, and overlap
checks. `SHA256SUMS` pins the CSV, manifest, report, and original DPS responses.

The supplied responses cover pages 20–27. The API reported 2,717 contract
records on pages 20–23 and 2,718 on pages 24–27. This check uses exact
matches for this batch against the supplied pages. The responses form a
partial contract export, with 718 distinct submissions.

## Completing the batch

Database assignment is pending.

1. Run `02-assign-batch.sql` in `digitalt_prod` to assign the verified paths.
   The query is restricted to the 384 literal paths listed in the manifest
   and rechecks that each package is complete and unassigned.
2. Run `03-export-paths.sql` and save the assigned export as
   `DIGITIZED_ITEM.csv`. Verify that it matches the 384 paths in
   `package-paths.txt` before platform handoff.

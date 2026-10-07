# Catalog deletion batch 2026-10-05-01

Status: all 353 manifest objects match submissions last recorded as `PRESERVED`
in the consolidated index. 318 use October evidence and 35 use September
evidence. This is a check of combined recorded evidence, not a complete fresh
October status check. All 353 paths remain in the CSV and manifest.
The database assignment affected-row count has not been supplied independently.

`preservation-index-check.json` records the successful combined-evidence check,
the SHA-256 of `../../dps-submissions/preservation-index.json`, the unchanged
manifest hash, and evidence counts by snapshot. The earlier `path-checks.json`
and `coverage-check.json` document the checks performed before consolidation.

Batch identifier: `catalog-delete-20261005-01`.
Scope: film → bevaring, production line 79, source paths beneath
`/global/film/00/film/bevaring/`, DPS contract `91c5` as in the previous batch.

## Path export checks

The original `DIGITIZED_ITEM.csv` contains a `path` header and 353 data rows.
`package-paths.txt` contains exactly those 353 path values in the same order,
one per line, without CSV quoting or a header. The original CSV is retained.

- 353 unique paths and 353 unique object-ID basenames
- No malformed rows, blank paths, duplicate paths, or duplicate basenames
- All paths are beneath `/global/film/00/film/bevaring/`, with no traversal,
  control characters, empty path components, or surrounding whitespace
- No path or object-ID overlap with the 1,130 entries in the September batch

`path-checks.json` records these checks and source/manifest SHA-256 hashes.
These path checks alone do not establish DPS preservation. The later
`preservation-index-check.json` supplies the combined preservation check.
`dps-snapshot.json` references the partial October snapshot for any future
full-snapshot validation.

## Preparation

### Partial DPS coverage

`response_15.json` through `response_18.json` contain 330 submissions for
contract `91c5`, with consistent pagination totals of 1,830 submissions and
19 pages (size 100). Pages 15–17 contain 100 entries each; page 18 has 30.
318 exactly match manifest basenames and all 318 are `PRESERVED`.
There are no duplicate object IDs, submission IDs, or ambiguous matches in
these four files. 35 manifest paths are absent from these fresh responses.

`coverage-check.json` records the partial coverage and response hashes;
`missing-paths.txt` lists the 35 paths absent from October pages 15–18. All 35
were subsequently found as `PRESERVED` on September page 14 and are included
in the consolidated index. Retain this list as the historical partial-coverage
result; these paths were never removed from the manifest.
The fresh responses were initially saved
inside the legacy directory and have been moved, unchanged, to the October
snapshot. The combined index now uses historical responses for these 35 paths
and records their older source explicitly.

### Original database preparation and full-snapshot validation procedure

The following procedure describes validation against a complete fresh snapshot.
For this batch, the consolidated-index check above was used after the partial
October fetch. The strict snapshot validator still requires every page.

Preview observed in the user's phpMyAdmin screenshot on 2026-10-05, schema
`digitalt_prod`: 0 existing batch rows; 353 candidate rows, 353 packages,
353 distinct paths, 0 blank paths, and 0 unsafe paths. The batch-ID check
took 30.4699 seconds; the candidate query took 0.0146 seconds.
These are preview counts, not an assigned batch or preservation result.

1. Run `01-preview.sql` in phpMyAdmin against the same production schema as
   the previous batch. It only reads data. The proposed identifier must have
   zero existing rows. Candidate rows, packages and distinct paths should
   agree, with zero blank or unsafe paths. Resolve discrepancies before
   assigning a batch.
2. Run `02-assign-batch.sql` once to assign eligible packages to
   `source.deletion.batch`. This writes database parameters only. Record the
   affected-row count. The query excludes items already in any batch and
   refuses to extend this batch on a sequential rerun once it contains items.
   Do not run concurrent batch assignments. Export from the assigned batch;
   the preview count can change while the pipeline is running. If the client
   times out, inspect batch membership before retrying.
3. Run `03-export-paths.sql` and export through **Query results operations →
   Export → Custom → CSV**, including all result rows. Save the export here
   as `package-paths.txt`; the existing validator accepts a CSV `path` header
   and quoted fields. Keep an internal ID/path audit export as well.
4. Retrieve fresh, complete DPS submission pages for the contract and save
   them in `../../dps-submissions/2026-10-05-01/`. Do not reuse the September
   snapshot for this new list. Keep pagination metadata and submission
   identifiers/statuses.
5. Run the existing validator from the repository root:

   ```bash
   python3 film_bevaring/deletion-batches/validate_preservation.py \
     film_bevaring/deletion-batches/catalog-delete-20261005-01 \
     --report film_bevaring/deletion-batches/catalog-delete-20261005-01/validation-summary.json
   ```

6. Check that the report identifies contract `91c5`, that every manifest
   basename exactly matches a `PRESERVED` DPS object, and that the new paths
   do not overlap any earlier batch manifest. Review the literal paths,
   save evidence checksums, and update this document with actual counts.
7. Provide the verified literal path manifest to the platform team. Record
   `source.deletion.exported_at` after sending and
   `source.deletion.confirmed_at` only after confirmation of deletion.

The previous batch is `../catalog-delete-20260904-01/` and contains 1,130
validated paths. Batch membership is used to avoid repeating those items;
no completion-date cutoff is needed.

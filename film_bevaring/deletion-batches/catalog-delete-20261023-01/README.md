# Catalog deletion batch 2026-10-23-01

Status: **validated and ready for platform handoff**. The manifest contains
224 source package paths. All 224 match submissions recorded as `PRESERVED`
in the October 23 DPS responses. Handoff and filesystem deletion have not
been confirmed in this chat.

Batch identifier: `catalog-delete-20261023-01`.
Scope: `digitalt_prod`, film → bevaring, production line 79, source paths below
`/global/film/00/film/bevaring/`, DPS contract `91c5`.

## Validation result

Preview: 0 existing batch rows; 224 candidates, packages, and distinct paths;
0 blank or unsafe paths. The user confirmed running the assignment query and
provided the path export, which contains exactly 224 rows.

- 224 unique paths and object-ID basenames, all in the expected SAM-FS root
- 224 unambiguous matches for contract `91c5`, all `PRESERVED`
- Every match has October 23 evidence; no older observations are needed for this batch
- No missing, non-preserved, blank, malformed, or unsafe entries
- No path or object-ID overlap with the September batch (1,130 paths) or
  October 5 batch (353 paths)

Provide `package-paths.txt` to the platform team. It contains every exported
path, in the original order, one per line, without a header or CSV quotes.
`DIGITIZED_ITEM.csv` is retained unchanged as the original export.

`preservation-index-check.json` records the result, counts, prior-batch checks,
and evidence hashes. It references the frozen
`../../dps-submissions/preservation-index-2026-10-23.json`, so refreshing the
current index will not change this batch's evidence. `SHA256SUMS` covers the
CSV, manifest, report, frozen index, and the three October 23 response files.

## Database queries used in phpMyAdmin

1. Run `01-preview.sql`. This is read-only. `existing_batch_rows` must be 0.
   `candidate_rows`, `packages`, and `distinct_paths` should agree and be
   greater than 0; `blank_paths` and `unsafe_paths` must be 0. If they differ,
   investigate before assigning the batch. If there are no candidates, there
   is nothing to assign or export yet. The batch-ID lookup uses plain equality
   filters as well as exact comparisons so available indexes can narrow it.
2. If the preview checks pass, run `02-assign-batch.sql` once. It adds
   `source.deletion.batch` parameters; it does not delete files or mark them
   deleted. It excludes every item already assigned to any earlier batch,
   including September 4 and October 5. Record the inserted-row count. This
   may exceed the preview if the pipeline completed more packages meanwhile.
   Do not run concurrent batch assignments. If the client times out, inspect
   this batch's membership before retrying. The query refuses to extend an
   already populated batch on a sequential rerun.
3. Run `03-export-paths.sql`. Use **Query results operations → Export →
   Custom → CSV**, all rows. Save the result here as `DIGITIZED_ITEM.csv`.
   A `path` header and CSV quoting are accepted. Keep the original export.

## Preservation validation after export

Use `../../dps-submissions/preservation-index.json`, updated with October 23
pages 18–20. At preparation it contains 2,063 submissions: 2,058 last recorded
as `PRESERVED` and 5 as `PROCESSING`. It combines historical and newer evidence;
each record identifies the snapshot of its last observation.

Check the exported paths for blanks, duplicates, unsafe components, and
overlap with every previous batch manifest. Match each complete path basename
exactly to an object ID for contract `91c5`; every match must be unambiguous
and `PRESERVED`. Missing or `PROCESSING` records remain in the original list
and are reported separately. Do not silently remove them or declare the whole
batch ready while such records remain unresolved.

After the checks pass, create `package-paths.txt` with all exported path values
in their original order, one per line. Save the validation report, evidence
counts by snapshot, and SHA-256 hashes. Pin a frozen copy of the index used for
this check so future index updates do not change the batch's evidence.

Provide the final path list to the platform team only after these checks.
Record `source.deletion.exported_at` after handoff and
`source.deletion.confirmed_at` only after confirmation of actual deletion.

# Catalog deletion batch 2026-09-04-01

## Purpose

This batch contains source package paths that were selected for deletion after
the corresponding packages completed production line 79 and were preserved in
DPS.

Batch identifier: `catalog-delete-20260904-01`

## Database selection

The packages were assigned to this batch only when all of these conditions
were true:

- `DIGITIZED_ITEM.Pline_id = 79`
- `DIGITIZED_ITEM.STATUS` exactly matched `Catalog.done`
- a `DI_EVENT` row existed with `TYPE = Catalog`, `STATUS = done`, and a
  non-null completion time
- `DI_PARAMETER.package_path` was non-empty and located below
  `/global/film/00/film/bevaring/`
- the item had not already been assigned a `source.deletion.batch`

The batch assignment inserted `source.deletion.batch =
catalog-delete-20260904-01` for 1,130 items. `package-paths.txt` was then
exported from that frozen batch.

## DPS validation

The DPS evidence contains pages 0 through 15 for contract `91c5`:

- 1,519 submission records
- 1,519 unique submission IDs
- 1,519 unique object IDs
- 1,517 submissions with status `PRESERVED`
- 2 submissions with status `PROCESSING`, neither included in this deletion
  batch

For the deletion manifest:

- 1,130 unique paths
- 1,130 unique path basenames
- 1,130 exact basename-to-`objectId` matches
- 1,130 matching submissions with status `PRESERVED`
- no missing, duplicated, blank, or out-of-root paths

Result: **PASS**. Every package in this deletion manifest was represented by a
`PRESERVED` DPS submission in the saved evidence.

## Evidence files

- `package-paths.txt` is the path-only deletion manifest supplied to the
  platform team.
- `dps-submissions/submissions-page-00-of-15.json` through
  `submissions-page-15-of-15.json` contain the DPS submission records.
- `validation-summary.json` contains the machine-readable validation result.
- `SHA256SUMS` records the evidence checksums.

The DPS response `files` arrays were removed before committing because
file-level metadata and generated upload URLs were not used by this check.
Submission identifiers, object identifiers, archive identifiers, statuses,
sizes, contract/client identifiers, priorities, and pagination metadata were
retained.

## Re-run the check

From the repository root:

```bash
python3 film_bevaring/deletion-batches/validate_preservation.py \
  film_bevaring/deletion-batches/catalog-delete-20260904-01 \
  --report film_bevaring/deletion-batches/catalog-delete-20260904-01/validation-summary.json
```

## Follow-up

The path list may be sent to the platform team after this validation passes.
Record `source.deletion.exported_at` after handoff. Record
`source.deletion.confirmed_at` only after the platform team confirms deletion.

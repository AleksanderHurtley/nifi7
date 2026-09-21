# film → bevaring deletion batches

This directory contains the evidence used to prepare filesystem deletion lists
after packages have completed the film → bevaring transfer production line
and been preserved in DPS.

Each batch has an immutable identifier that is also stored in
`DI_PARAMETER` as `source.deletion.batch`. A batch directory contains:

- `package-paths.txt`: the literal filesystem paths supplied to the platform
  team, one path per line.
- `dps-submissions/`: paginated submission responses used to verify that the
  path basename equals the DPS `objectId` and that the submission status is
  `PRESERVED`.
- `validation-summary.json`: the saved result of the reproducible validation.
- `README.md`: the batch scope, checks, result, and operational follow-up.
- `SHA256SUMS`: checksums for the manifest and DPS evidence.

## Validation

Run the validator from the repository root:

```bash
python3 film_bevaring/deletion-batches/validate_preservation.py \
  film_bevaring/deletion-batches/<batch-id> \
  --report film_bevaring/deletion-batches/<batch-id>/validation-summary.json
```

The command exits with a nonzero status if a manifest path is unsafe, an
expected DPS page is missing, identifiers are duplicated, a manifest object is
missing from DPS, or a manifest submission is not `PRESERVED`.

## Operational states

Batch assignment, export, and confirmed deletion are different events:

1. `source.deletion.batch` freezes membership in a deletion batch.
2. `source.deletion.exported_at` is written after the list is sent to the
   platform team.
3. `source.deletion.confirmed_at` is written only after the platform team
   confirms that the listed paths were deleted.

The preservation validation does not by itself prove that filesystem deletion
has happened.

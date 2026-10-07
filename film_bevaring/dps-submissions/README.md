# DPS submission index and snapshots

`preservation-index.json` is the consolidated local database of last observed
submission states for contract `91c5`. Its `content` array contains one entry
per submission, sorted by object ID, with the submission metadata and evidence
references. File-level `files` arrays are omitted from the index; original
responses are retained unchanged.

Current index: 1,830 unique submissions, all last recorded as `PRESERVED`.
19 overlapping observations were merged. The newer observations update two
previously `PROCESSING` submissions to `PRESERVED`.
330 records have October evidence; 1,500 retain September evidence. The count
matches October's declared contract total, but the index is a combination of
snapshots, not a fresh full-contract response. It does not have synthetic API
pagination metadata.

Rebuild from the repository root, listing snapshots **oldest to newest**:

```bash
python3 film_bevaring/dps-submissions/build_preservation_index.py \
  film_bevaring/dps-submissions/legacy-20260904-01 \
  film_bevaring/dps-submissions/2026-10-05-01 \
  --output film_bevaring/dps-submissions/preservation-index.json
```

Newer responses replace older states, including a newer non-preserved state.
Every observation retains its source file, snapshot, page, and status. The
index records source-file SHA-256 hashes and which pages were fetched.
Conflicting object/submission identities, duplicate entries within a snapshot,
inconsistent pagination totals, and unexpected contracts stop the build.
Keep records for all statuses so later changes cannot silently leave an old
`PRESERVED` result in place. Save new exports in a new snapshot directory and
include it last when rebuilding. Historical evidence omitted from a partial
new export remains in the index and retains its older source reference.

## Source snapshots

These exports list DPS contract submissions and can be used by multiple deletion
batches. Keep each export, including partial fetches, in its own snapshot directory. Do not mix
pages from different exports or overwrite a snapshot already used as evidence.

```text
film_bevaring/
├── dps-submissions/
│   ├── legacy-20260904-01/  # Original September evidence
│   ├── 2026-10-05-01/      # October pages 15–18
│   └── preservation-index.json
└── deletion-batches/
    └── <batch-id>/
        ├── package-paths.txt
        ├── dps-snapshot.json
        └── validation-summary.json
```

Each batch's `dps-snapshot.json` selects a snapshot path relative to the batch
directory and the expected contract ID. The validator reads that reference,
checks the contract and complete page sequence, and records the reference in
new validation reports. It does not automatically select the latest export.
That validator requires a complete single snapshot. The October batch's
`preservation-index-check.json` separately records its check against the
combined index, including the index hash and counts by evidence snapshot.

Use a new `YYYY-MM-DD-NN` directory for each fresh export. The legacy
directory's name comes from the original batch ID, not a verified capture
date. Its 16 response files were moved without changing their contents.
The historical batch's `SHA256SUMS` now points to their shared location;
its original validation report and timestamp are retained.

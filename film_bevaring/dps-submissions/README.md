# DPS submission index and snapshots

`preservation-index.json` is the consolidated local database of last observed
submission states for contract `91c5`. Its `content` array contains one entry
per submission, sorted by object ID, with the submission metadata and evidence
references. File-level `files` arrays are omitted from the index; original
responses are retained unchanged.

Current index (updated 2026-10-23): 2,063 unique submissions, with 2,058 last
recorded as `PRESERVED` and 5 as `PROCESSING`. The latest export added 233
submissions and refreshed 30 existing records without changing their statuses.
49 overlapping observations have been merged across all snapshots.
263 records have October 23 evidence, 300 retain October 5 evidence, and 1,500
retain September evidence. The count matches the latest declared contract
total, but the index is a combination of
snapshots, not a fresh full-contract response. It does not have synthetic API
pagination metadata.

Rebuild from the repository root, listing snapshots **oldest to newest**:

```bash
python3 film_bevaring/dps-submissions/build_preservation_index.py \
  film_bevaring/dps-submissions/legacy-20260904-01 \
  film_bevaring/dps-submissions/2026-10-05-01 \
  film_bevaring/dps-submissions/2026-10-23-01 \
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
│   ├── 2026-10-23-01/      # October 23 pages 18–20
│   ├── preservation-index-2026-10-05.json  # Frozen batch evidence
│   ├── preservation-index-2026-10-23.json  # Frozen October 23 batch evidence
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
It now references `preservation-index-2026-10-05.json`, a byte-identical frozen
copy of the index used for that check. Preserve any index referenced by a batch
report before refreshing the current index, and point the report to that frozen
copy without changing its recorded hash or results.
The October 23 batch similarly references `preservation-index-2026-10-23.json`;
all 224 of its paths have October 23 `PRESERVED` observations.

Use a new `YYYY-MM-DD-NN` directory for each fresh export. The legacy
directory's name comes from the original batch ID, not a verified capture
date. Its 16 response files were moved without changing their contents.
The historical batch's `SHA256SUMS` now points to their shared location;
its original validation report and timestamp are retained.

# Catalog cleanup before film production

Run the cleanup separately before starting the NiFi film production flow.
Use the same catalog environment for lookup and deletion, and run `apply`
shortly after `scan`: `apply` deletes what the scan found without looking the
records up again.

## Export the URNs

Run [list-urns.sql](list-urns.sql) in the production database. It selects every
`Transfer.ready` row for production line 82. Save the query result as
`film_produksjon/catalog-cleanup/urns.csv` with the column header `urn`.
Each input row must contain one production URN. Duplicate URNs or malformed
rows stop the scan so the queue can be corrected.

The URN list, plans, dumps and audit logs are working files and are not
committed (see [.gitignore](.gitignore)). Keep them on your own machine, outside
synchronised folders, for as long as a restore might be needed, and then delete
them: the dumped records include the names of the cataloguers.

## Scan the catalog

Run from the repository root with Python 3.10 or later. These are the stage
endpoints. Use the corresponding deployed endpoints when working in production.

```bash
lookup_url='https://api.collections.stage.nb.no/webapi_levendebilder/wwwopac.ashx'
delete_url='https://api.dev.produksjon.nb.no/collections-rest'
work=film_produksjon/catalog-cleanup/work && mkdir -p "$work"
python3 film_produksjon/catalog-cleanup/cleanup.py --environment stage \
  --catalog-url "$lookup_url" --delete-base-url "$delete_url" \
  scan film_produksjon/catalog-cleanup/urns.csv "$work/catalog-plan.json"
```

`--environment` is `stage` or `prod` and is required. Both URLs must be known
endpoints of that environment, and `apply` and `parents` refuse a plan or audit
log from another environment or other endpoints.

`--request-interval` (default 1 second) is the minimum time between two catalog
requests, lookups and deletes alike, to avoid loading the catalog. A scan of
2,000 URNs therefore takes at least half an hour. Raise the interval when the
catalog is under load; `--pause-seconds` adds a further wait after each delete.

The scan only reads the catalog. Duplicates are found by URN: every record
with the URN is a match. The file size then decides which record is the
primary and which may be deleted:

| Status | Meaning |
|---|---|
| `READY` | One active ITEM_PART with a positive size |
| `CLEANUP` | One positive-size ITEM_PART and one or more explicit zero-byte ITEM_PART duplicates |
| `MISSING` | No record matched the URN |
| `REVIEW_PRIMARY` | There is no positive-size ITEM_PART or there are multiple positive-size ITEM_PARTs |
| `REVIEW_SIZE` | At least one additional record has a missing or invalid size |
| `REVIEW_TYPE` | The URN also identifies a record of another type |
| `REVIEW_SHARED_URN` | A zero-byte record also identifies another URN |
| `REVIEW_LINKED` | A zero-byte record has item parts, copies (`copied_to`) or related objects, or one of these could not be read |
| `LOOKUP_FAILED` | The HTTP request failed or the response could not establish a complete set of matches |

Zero-byte records may keep their own `copied_from` links: duplicates are
usually copied from the same source, and the delete only removes the reverse
`copied_to` from that source.

For each `CLEANUP` URN the scan also looks up the ITEMs that will be empty
once the duplicates are gone: ITEMs whose only item parts are the duplicates,
under the same manifestation as the primary's ITEM, with no copies or related
objects. The primary's own ITEM is never one of them. Other ITEMs are listed
under `itemsKept` with the reason.

The command prints counts and returns zero when every URN is `READY`. Use the
plan to inspect the remaining cases. Resolve disputed sizes or primaries in
the catalog, then scan again with a new plan filename.

### The dump

Before a `CLEANUP` URN is written to the plan, the scan saves the catalog's
responses next to the plan, in `<plan name>.dump/`, then reads them back and
checks them. Only full records of what the cleanup changes are saved; other
records appear as links.

```text
work/
  catalog-plan.json
  catalog-plan.dump/
    digifilm_815855_20131106/   one folder per CLEANUP URN
      index.json                summary, links and checksums
      urn.xml                   the URN lookup, byte for byte: primary and duplicates
      item_219355199.xml        the lookup of each ITEM that will be deleted
```

`index.json` names the primary, the primary's ITEM and its manifestation, the
duplicates, the ITEMs, the links of every dumped record (`partOf`, `parts`,
`copiedFrom`, `copiedTo`, `relatedObjects`) and a SHA-256 checksum for each
file. The plan holds the checksum of `index.json`.

## Apply the plan

Set up the existing Collections credentials in your local environment.
For bearer tokens, use `COLLECTIONS_DELETE_TOKEN` and, if lookup requires
authentication, `COLLECTIONS_LOOKUP_TOKEN`. For automatic token renewal when
both APIs accept the same OAuth client, use `COLLECTIONS_TOKEN_URL`,
`COLLECTIONS_CLIENT_ID` and `COLLECTIONS_CLIENT_SECRET`. Bearer tokens are sent
only over HTTPS. Credentials are kept out of the plan, dump and audit files.

```bash
python3 film_produksjon/catalog-cleanup/cleanup.py --environment stage \
  --catalog-url "$lookup_url" --delete-base-url "$delete_url" \
  apply "$work/catalog-plan.json" "$work/catalog-cleanup.ndjson" \
  --remove-empty-parents --limit 10
```

`apply` only processes `CLEANUP` URNs; the others need no change or a manual
review. For each URN it:

1. Reads the dump back and checks every checksum and record. If the dump is
   missing or altered, nothing is deleted for that URN (`DUMP_INVALID`).
2. Deletes the zero-byte duplicates, one at a time.
3. With `--remove-empty-parents`, deletes the ITEMs found by the scan.
4. Looks the URN up once and confirms that the unchanged primary is the only
   match (`VERIFIED`), or reports `REVIEW_AFTER_CLEANUP`.

Start with a few URNs: `--only <urn>` (repeatable) and `--limit N` restrict the
run to part of the plan. Use a new audit filename for every run.
`--pause-seconds` (default 2) waits after each DELETE, because dead links are
most often seen after many fast deletes. Each DELETE must return HTTP 204;
Collections REST reads the record itself and only deletes ITEM and ITEM_PART
records.

An ITEM can still list its deleted item parts afterwards (dead links).
Collections REST refuses to delete an ITEM that lists item parts (HTTP 409), so
the tool keeps it and reports it at the end with the priref range for the
catalogue team. The URN is still `VERIFIED`, because the item parts are gone.
Once the catalogue team has removed the dead links, retry the kept ITEMs from
the audit logs:

```bash
python3 film_produksjon/catalog-cleanup/cleanup.py --environment stage \
  --catalog-url "$lookup_url" --delete-base-url "$delete_url" \
  parents "$work/catalog-cleanup.ndjson" "$work/catalog-parents.ndjson"
```

The last file is the new audit log. Each ITEM is read again and deleted only
when it has the same identity and manifestation and lists no item parts.

Instead of waiting for the catalogue team, `unlink` can remove the dead links
itself, before `parents`:

```bash
python3 film_produksjon/catalog-cleanup/cleanup.py --environment stage \
  --catalog-url "$lookup_url" --delete-base-url "$delete_url" \
  unlink "$work/catalog-cleanup.ndjson" "$work/catalog-unlink.ndjson"
python3 film_produksjon/catalog-cleanup/cleanup.py --environment stage \
  --catalog-url "$lookup_url" --delete-base-url "$delete_url" \
  parents "$work/catalog-cleanup.ndjson" "$work/catalog-unlink.ndjson" "$work/catalog-parents.ndjson"
```

For each kept ITEM, `unlink` checks that it is still the same active ITEM under
the same manifestation, that it lists only the item parts that were deleted,
and that none of them exists. It saves the ITEM to `<audit name>.dump/`, sends
one `updaterecord` to the catalog URL that empties `parts.reference.lref`, and
reads the ITEM back. If `parts` is not empty or any other checked field has
changed, the run stops. This writes to the catalog directly, not through
Collections REST: a bearer token is sent only to an HTTPS catalog URL. It was
tested in stage, where the ITEM kept all other fields.

The audit log records each requested and confirmed deletion. A
`DELETE_REQUESTED` row holds the record's priref, object number, identity,
size, links and the dump file with the full record. On an HTTP or
malformed-response error the run stops and reports how many URNs remain. If a
DELETE response was interrupted, check the catalog and the audit log before
scanning again with a new plan filename.

Start the production flow once every URN in the export is either `READY` in the
plan or `VERIFIED` in an audit log. A fresh scan with every URN `READY` also
confirms this. Connect catalog dump failures to the existing failure output in
NiFi for any package that needs further review.

## Restoring a deleted record

Collections REST cannot recreate a record with its old priref; Axiell assigns
new prirefs and reuses old ones. Use the dump:

1. Ask the catalogue team to restore the record. Give them the priref, object
   number, guid and deletion time from `index.json`, `urn.xml` or
   `item_<priref>.xml` and the audit log. A restore by the catalogue team keeps
   the record's history.
2. If that is not possible, create the record again through Collections REST
   from the dumped fields, under the ITEM in `partOf` and with its `copiedFrom`
   source. The links on the other records come back with it, but the record
   gets a new priref, loses its edit history and only keeps the fields the API
   supports. The film production flow finds the record by URN, so a new priref
   does not affect it.

## Local tests

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s film_produksjon/tests -p test_catalog_cleanup.py
```

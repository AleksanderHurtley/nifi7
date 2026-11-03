# Catalog attributes and submission body

The four catalog extractors in this directory are also reused by
`film_produksjon`. Produksjon uses its own
[05_Make submission body.groovy](../../film_produksjon/07_Submission%20body/05_Make%20submission%20body.groovy)
for the final body with source references.

## Attribute reset on every run

NiFi retains incoming FlowFile attributes unless a processor replaces or removes
them. An `if (values)` around `putAttribute` does not clear a previous value
when the new catalog record has no corresponding field.

Each extractor removes all of its generated metadata attributes before input
validation and parsing, then sets the values found in the current exports.
This applies to normal processing, retries, missing/empty exports, and failures.
Missing optional fields therefore remain absent; the body builders turn them
into empty arrays or omit optional source entries. The existing behavior for
missing export files and the catalog field mappings are unchanged.

| Processor script | Metadata attributes rebuilt |
|---|---|
| `01_Get Work.groovy` | `metadata.mainTitle`, `metadata.alternativeTitles`, `metadata.creators`, `metadata.contributors`, `metadata.publishers`, `metadata.workRelations`, `metadata.countries`, `metadata.dates`, `metadata.descriptions` |
| `02_Get DIGITAL ITEM.groovy` | `metadata.languages` |
| `03_Get Analog item part bar_code.groovy` | `metadata.barcode` |
| `04_Get digital item part.groovy` | `metadata.pidDataUrns`, `metadata.digitalItemPartRelations`, `metadata.provenance` |

Each stage also clears previous `submission.payload.status/error` attributes and
sets fresh failure information if that run fails. Package identity, directory
attributes, checksums, and other stages' metadata outputs are retained. Run the
four extractors in order, routing failures out before building the submission.
The digital item-part extractor reads only that record's direct `CopiedFrom`
and `CopiedTo` links; copy history embedded in parent records is excluded.
Get Work deduplicates countries as well as its other list outputs.

For each export type, all four extractors use the matching files in work when
present, otherwise those in the retained SIP. They do not combine those two
snapshots. This prevents both duplicate values and older SIP metadata leaking
into a refreshed work export. Field-level deduplication also handles repeated
values inside the selected exports. This choice is per export type; it does
not verify that the catalog itself is correct or that work contains a complete
snapshot of every linked record.

Update all four live processors' Script Body properties from these files in
both flows where they are used. Repository edits do not update NiFi automatically.
For a package that already has stale catalog attributes, rerun the extractors
and body builder and save the new `submission.json`; changing scripts alone
does not rewrite a payload already on disk.

## Verification

From the repository root with Groovy 4:

```sh
groovy film_bevaring/tests/catalog-attributes.groovy
groovy film_produksjon/tests/submission-body.groovy
```

The reset test reuses populated FlowFile attributes with sparse or missing
exports, exercises failure routes, checks that unrelated attributes survive,
and verifies the final produksjon payload against the captured WORK record for
`digifilm_815655_20130726`. That record has no alternative titles or work
relations, so stale `COLUMBI EGG SOM SKOLEMAT` and SAS `SegmentOf` entries must
disappear while current carrier `CopiedFrom` relations remain. It also stages
the same WORK export in work and the retained SIP and verifies that countries
and the complete generated metadata are unchanged by the second copy. For all
four extractors, it tests identical and differing work/SIP copies and the SIP
fallback after the work export is removed.

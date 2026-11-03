# Produksjon submission body

Use [05_Make submission body.groovy](05_Make%20submission%20body.groovy) in the
existing payload-building ExecuteGroovyScript processor after the catalog
attribute extractors. It starts from the bevaring builder and adds
`metadata.source` from the current digital item part's direct `CopiedFrom`
relationships. Copy history stays at `ITEM_PART` (carrier) level. The existing
`metadata.digitalItemPartRelations` attribute supplies the relation list and
source references when the current item-part export is unavailable.
The script makes no API calls.
It writes the submission JSON to the FlowFile content; the existing downstream
step remains responsible for saving `submission.json`.

## Catalog attribute reset

Use the production-local [01_Get Work](01_Get%20Work.groovy),
[02_Get DIGITAL ITEM](02_Get%20DIGITAL%20ITEM.groovy),
[03_Get Analog item part bar_code](03_Get%20Analog%20item%20part%20bar_code.groovy),
and [04_Get digital item part](04_Get%20digital%20item%20part.groovy) scripts here.
They start from the existing extractors with the tested attribute resets and
work-over-SIP selection. Updating these copies requires no new bevaring changes.
Each extractor removes its generated metadata attributes before each run and
repopulates them from the current catalog exports. A missing alternative title,
relationship, language, barcode, or other mapped field cannot retain a previous
FlowFile value. Identity, paths, checksum results and other extractors' outputs
are retained.
For each export type, work files take precedence over the retained SIP; the
extractors use the SIP only when matching work exports are absent. Current and
older copies are not merged. Identical values within the selected exports are
deduplicated.

Deploy all four extractor scripts to the live processors. For existing test
FlowFiles, rerun the extraction stages and this body builder, then save the new
`submission.json`. A payload already on disk is not changed automatically.

## Source mapping

The [DPS metadata guidance](https://digitalpreservation.no/nb/docs/dps/api/submission/metadata/#source)
allows physical or digital source objects, with one source object per source
and typed identifiers. The source level for produksjon is the source `ITEM_PART`
(carrier), matching the current item part's copy history.

The builder locates the current `ITEM_PART` by `metadata.pidDataUrns` (or the
package URN when that attribute is absent) and reads its direct
`CopiedFrom/copied_from.lref` references. The parent's `ITEM` copy history is not
followed. IDs are never inferred from numeric offsets, file order, or matching
titles. A matched current item part without `CopiedFrom` produces no sources.

If the current item-part export is unavailable, the builder retains the source
IDs from the existing `metadata.digitalItemPartRelations` attribute. The
upstream catalog lookup must select the correct package records. The local
`04_Get digital item part` extractor also reads only direct `CopiedFrom` and
`CopiedTo` links, excluding embedded parent records' copy history. Existing
`relation` entries are retained.

| Input | Output in each `metadata.source` entry |
|---|---|
| Current item part's `copied_from.lref` (or extracted `CopiedFrom` relationship `id`) | `identifier: [{"type":"Collections priref","value":"<id>"}]` |
| Same Collections priref | `URI: https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/<id>` |
| Nonempty relationship `title` | `description` |
| Missing relationship title, but matching source record in a catalog dump | Source record's title as `description` |
| Matching source record's `file.format` | Append to `description` as `<title> — <format>`; use just the format if no source title is available |
| Hierarchy `Parts` with `parts.reference.lref` matching the source ID | `parts.title` and `parts.fileformat` supply the source title/format |

`Collections priref` is an explicit identifier type for the catalog record ID;
it is not a barcode or URN. URL construction uses the Collections record-link
pattern from the DPS examples; the builder does not fetch the target page.
IDs must contain only digits. Duplicate references to one source ID
are combined, retaining the first nonempty title. Multiple source objects
remain separate entries, even if their titles are identical.

The builder searches the XML files directly under `metadata.descriptive.dir`,
then `transfer.dir/metadata/descriptive`. A record whose own `@priref`
(or direct `priref`) matches the source ID can supply its title or file format.
For missing titles it prefers a `Title` marked `title.main=x`, then
`title.type=Originaltittel`, then the first nonempty `Title/title`, then the
matched source record's `Part_of/part_of.title`. Existing nonempty relationship
titles are retained. If those title fields are empty, use the immediate embedded
parent's `Part_of/part_of_reference/Title` with the same main/original/first
preference. This follows an explicit parent link for a shared display title;
it does not replace the source carrier's ID or format with its parent's.
Catalog format spelling is retained.

Hierarchy exports can also supply source data in a `Parts` block. Match its
`parts.reference.lref` (or `parts_reference/priref`) to the source ID, then read
`parts.title` (or `parts_reference/Title/title`) and `parts.fileformat` from
that same block. If the title is empty, the containing manifestation/component's
title supplies the shared film title for the display description. Its ID and
file format are never substituted for the child's. This supports fields such as those reported on parent
component `68902057` for source carrier `68902058`, without substituting the
parent's record ID for the source ID.

The source records or hierarchy exports containing explicit child references
must already be included in the local catalog dumps for this lookup to work.
A `CopiedFrom` link alone does not contain the complete source
record. If neither a title nor a format is available, `description` is omitted.
If the existing hierarchy exports supply those fields, no additional source
record fetch is needed. Otherwise, source records can be added to the Catalog
PG output. The builder does not invent a title/type or fetch a web page.
The production-only [CopiedFrom export group](../03_Catalog/README.md) now
provides that acquisition step using InvokeHTTP and saves each source as
`<package.name>_COPIED_FROM_<priref>.xml` beside the other dumps. Run it after
the existing catalog fragment join and before submission/E-ARK.

`CopiedTo` links and work-level relationships do not create source entries.
If neither the current item-part links nor the attribute supplies usable
`CopiedFrom` references, `metadata.source` is omitted.
The existing `relation` list and all other payload fields remain unchanged.
Malformed JSON or nonnumeric `CopiedFrom` IDs route to failure using the
existing `submission.payload.status/error` attributes.

This is a minimal catalog-reference mapping: it does not retrieve source records
over the network. It can use titles and file formats once the source records
are in the dumps; source barcodes/URNs can be added later once explicitly
linked to each source object. The package's own
`metadata.pidDataUrns` and the aggregated `metadata.barcode` values are not
automatically copied into `source`.

## Examples and verification

The [earlier Per på landet capture](../examples/submission/digifilm_815655_20130726.submission.json)
shows the user's corrected live output after rerunning Catalog/submission on
2026-11-03: no stale alternative title or SAS relations, one country entry, and
item-part sources `69119828` and `69119830`. Its
[capture notes](../examples/submission/README.md) link to the matching events
and distinguish observed output from independent source verification.
The subsequent [five-export capture](../examples/catalog_dump/digifilm_815655_20130726/README.md)
reproduces those source entries locally. It has component-level formats but no
source-carrier title/format metadata, so it cannot supply descriptions without
additional exports explicitly identifying the source item parts.
The subsequently captured `COPIED_FROM` responses now provide those records.
Both lack a direct `Title`, but their embedded immediate parents supply the
film title. The [enriched live payload](../examples/submission/digifilm_815655_20130726.enriched-submission.json)
was subsequently supplied from NiFi and matches the locally tested output from
all seven real exports, with descriptions `Per på landet — Image/x-dpx` and
`Per på landet — Audio/wav` and formats from the source carriers themselves.

The [four-file dump](../examples/catalog_dump/digifilm_727480_20130730/README.md)
for `digifilm_727480_20130730` produces the
[source example](../examples/submission/digifilm_727480_20130730.source.json)
directly, without extra catalog calls:

- Current carrier `70398205` has `CopiedFrom` references to source carriers
  `69194879` and `69194881`. Those IDs supply both `source` and the existing
  carrier-level `CopiedFrom` relations.
- Its parent component's references `69194878` and `69194880` do not supply sources.
- The manifestation lists DPX/WAV formats for those components, without an
  explicit link to the source carriers. Those component formats cannot be
  assigned to the carriers by adjacent IDs or ordering.
- The carrier relationship titles are empty and no matching carrier details are
  exported, so this example omits `description`. Exporting the source carrier
  records or hierarchy entries explicitly linked to them will enable it.

This example is verified against the captured XML by the local test, not a
live API-acceptance result.

The [2013 source fragment](../examples/submission/digifilm_1300107_20130729.source.json)
uses the two `CopiedFrom` IDs (`69185513`, `69185515`) supplied in the user's
live FlowFile attributes. Both relationship titles were empty, and the linked
source records have not been supplied locally for this package, so the example
has no invented descriptions. The builder can populate them once those records
are included in the catalog dumps.
The [captured 2013 produksjon dump](../examples/catalog_dump/README.md) for
`digifilm_801190_20130711` confirms the same situation: source IDs `68902056`
and `68902058` are present, but their titles are empty and their records are
not embedded. The production record's own main title is available separately.
The user's earlier Collections screenshots supplied on 2026-11-02 separately confirm
that `68902056` is `Image/x-dpx` and `68902058` is `Audio/wav`, with title
`Over Nordpolen : med Hjalmar Riiser-Larsen`. The
[expected source fragment](../examples/submission/digifilm_801190_20130711.source.json)
shows the carrier-level title/format descriptions based on that UI evidence.
It is not a live generated payload: the captured production-record XML alone
cannot supply the two source formats. Catalog retrieval must supply those
source records/fields or an explicitly linked `Parts` block before the builder
can populate them automatically.
This file illustrates the `metadata.source` fragment, not a complete API
request or a confirmed accepted submission.

With Groovy 4, from the repository root:

```sh
groovy film_produksjon/tests/submission-body.groovy
groovy film_produksjon/tests/catalog-attributes.groovy
groovy film_produksjon/tests/copied-from.groovy
```

The tests compare existing fields against the unchanged bevaring builder,
exercise captured catalog dumps, current item-part selection via resource URN,
no substitution from parent ITEM links, unchanged carrier relations, a retained-SIP retry, deduplication, multiple sources, optional
titles and formats, source-record/Parts lookup in work and SIP, exact ID binding,
captured manifestation hierarchy fields, format-only descriptions, no parent
or current-MOV format leakage, UTF-8/quotes,
omitted sources, and failure routing. No live API writes
are performed. Update the live processor's Script Body with this file and
inspect the generated source entries before submission.

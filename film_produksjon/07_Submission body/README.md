# Produksjon submission body

Use [05_Make submission body.groovy](05_Make%20submission%20body.groovy) in the
existing payload-building ExecuteGroovyScript processor after the catalog
attribute extractors. It starts from the bevaring builder and adds
`metadata.source` using locally staged catalog exports. It prefers the source
components explicitly referenced by the current digital item's `CopiedFrom`
relationships. The existing `metadata.digitalItemPartRelations` attribute is
retained for the existing relation list and as a carrier-level source fallback.
The script makes no API calls.
It writes the submission JSON to the FlowFile content; the existing downstream
step remains responsible for saving `submission.json`.

## Source mapping

The [DPS metadata guidance](https://digitalpreservation.no/nb/docs/dps/api/submission/metadata/#source)
allows physical or digital source objects, with one source object per source
and typed identifiers. The agreed source level for produksjon is the source
component, allowing title/format descriptions from the existing four-file dump.

The builder locates the current `ITEM_PART` by `metadata.pidDataUrns` (or the
package URN when that attribute is absent), follows its direct
`Part_of/part_of_reference.lref` to the current `ITEM`, then reads that item's
`CopiedFrom/copied_from.lref` source-component references. These are explicit
catalog links; IDs are never inferred from numeric offsets, file order, or
matching titles. All referenced parent items must have exported `CopiedFrom`
links before the component sources replace the carrier-level fallback.

If the parent linkage/exports are unavailable, the builder retains the source
IDs from the existing `metadata.digitalItemPartRelations` attribute. The
upstream catalog lookup must select the correct package records. No existing
`relation` entries are replaced when component-level source objects are used.

| Input | Output in each `metadata.source` entry |
|---|---|
| Source component's `copied_from.lref` (or fallback carrier relationship `id`) | `identifier: [{"type":"Collections priref","value":"<id>"}]` |
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
titles are retained. Catalog format spelling is retained.

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

`CopiedTo` links and work-level relationships do not create source entries.
If neither the current parent-item links nor the fallback supplies usable
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

The [four-file dump](../examples/catalog_dump/digifilm_727480_20130730/README.md)
for `digifilm_727480_20130730` produces the
[source example](../examples/submission/digifilm_727480_20130730.source.json)
directly, without extra catalog calls:

- Current carrier `70398205` links to parent component `70398204`.
- That parent's `CopiedFrom` points to source components `69194878` and `69194880`.
- Manifestation `25727480` explicitly lists their formats as `Image/x-dpx` and
  `Audio/wav`. Its title supplies `Kontakten knyttes` for each description.
- Carrier-level `relation` IDs `69194879` and `69194881` remain unchanged.

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
shows the earlier carrier-level title/format descriptions based on that UI evidence.
It is not a live generated payload: the captured production-record XML alone
cannot supply the two source formats. Catalog retrieval must supply those
source records/fields or an explicitly linked `Parts` block before the builder
can populate them automatically.
This file illustrates the `metadata.source` fragment, not a complete API
request or a confirmed accepted submission.

With Groovy 4, from the repository root:

```sh
groovy film_produksjon/tests/submission-body.groovy
```

The tests compare existing fields against the unchanged bevaring builder,
exercise captured catalog dumps, component selection via resource URN and parent
links, unchanged carrier relations, a retained-SIP retry, deduplication, multiple sources, optional
titles and formats, source-record/Parts lookup in work and SIP, exact ID binding,
captured manifestation hierarchy fields, format-only descriptions, no parent
or current-MOV format leakage, UTF-8/quotes,
omitted sources, and failure routing. No live API writes
are performed. Update the live processor's Script Body with this file and
inspect the generated source entries before submission.

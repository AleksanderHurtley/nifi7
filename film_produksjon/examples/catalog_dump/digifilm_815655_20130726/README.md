# Per på landet: catalog and source captures

The user first supplied five XML exports as pasted server output on 2026-11-03
from `transfer/digifilm_815655_20130726/metadata/descriptive/`. Only the XML was
saved, excluding command output labels. This is user-provided evidence, not a
direct API fetch or a checksum-verified server copy. The WORK record was also
supplied separately earlier and remains available in the parent directory.

| Export | Records | Relevant evidence |
|---|---|---|
| `DIGITAL_ITEM_162311521.xml` | `162311521` | MOV component; component-level sources `69119827`, `69119829` |
| `DIGITAL_ITEM_70398019.xml` | `70398019` | Earlier MOV component; same component-level source references |
| `DIGITAL_ITEM_PART_70398020_162311522.xml` | `70398020`, `162311522` | Both share the package URN and link directly to source item parts `69119828`, `69119830` |
| `MANIFESTATION_25815655.xml` | `25815655` | `Parts` identifies component `69119827` as `Image/x-dpx` and component `69119829` as `Audio/wav` |
| `WORK_815655.xml` | `815655` | Film title, credits, description, country and production year |

Filenames in the table have the common `digifilm_815655_20130726_` prefix.

In these original five exports, `CopiedFrom/copied_from.title` is empty. Neither a
source record nor a `Parts` block identifying `69119828` or `69119830` supplies
its title/format. The manifestation's DPX/WAV fields identify different records
at ITEM/component level. These exports do not explicitly map those components
to the source carriers; adjacent IDs and file order are not a mapping rule.

The [captured submission](../../submission/digifilm_815655_20130726.submission.json)
therefore has the correct item-part identifiers and URIs without descriptions.
The film title is present elsewhere, but assigning the component formats to
the carriers would require an unsupported inference. To populate source
descriptions, the catalog stage must also export source item parts `69119828`
and `69119830` or hierarchy metadata explicitly identifying those same carriers.

## Additional CopiedFrom responses

After testing the new Catalog group, the user supplied these two real XML
responses from `work/digifilm_815655_20130726/metadata/descriptive/`. They are
saved with the same `<package.name>_COPIED_FROM_<priref>.xml` names, retaining
the supplied XML content rather than reformatting it.

| Source ITEM_PART | Own `file.format` | Explicit parent ITEM | Embedded parent title |
|---|---|---|---|
| `69119828` | `Image/x-dpx` | `69119827` | `Per på landet` |
| `69119830` | `Audio/wav` | `69119829` | `Per på landet` |

Both source responses lack a direct `Title` and have an empty `part_of.title`.
The nearest `Part_of/part_of_reference/Title/title` supplies the display title.
The builder therefore supports this explicit title inheritance while keeping
source IDs at ITEM_PART level and reading formats only from the source records'
own `file.format`. These responses now document the carrier/component links;
they are not inferred from adjacent numeric IDs.

The [enriched submission](../../submission/digifilm_815655_20130726.enriched-submission.json)
was first verified locally using all seven exports and the production
extractors/body builder. The user then supplied matching live NiFi output on
2026-11-03, confirming the source enrichment. DPS acceptance is not recorded.
The earlier live submission capture is unchanged. The new source
descriptions are `Per på landet — Image/x-dpx` and `Per på landet — Audio/wav`.

The local source-builder test reproduces the captured source entries from all
five exports without descriptions, then the seven-export result with inherited
titles and each carrier's own format. Duplicate references from the two current
MOV item parts still produce only two source objects. The live event's 2018 creation
timestamp is not independently verified by these current catalog exports;
original source METS is still needed for that check.

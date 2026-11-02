# Four-file produksjon catalog capture

The user supplied these four XML exports as pasted `cat` output on 2026-11-02,
from `transfer/digifilm_727480_20130730/metadata/descriptive/` on nifi7.
Only the XML was retained, excluding leading commands/trailing prompts.
No direct API fetch or source-checksum validation was performed locally.

| Record | Role | Source references |
|---|---|---|
| `70398205` | Current digital item part (MOV carrier) | `CopiedFrom` carriers `69194879`, `69194881` |
| `70398204` | Current digital item (MOV component) | `CopiedFrom` components `69194878`, `69194880` |
| `25727480` | Manifestation | `Parts` identifies component `69194878` as `Image/x-dpx`, `69194880` as `Audio/wav` |
| `727480` | Work | Main title `Kontakten knyttes` |

Source-carrier titles are empty in `CopiedFrom`; source component titles are
also empty in the manifestation's `Parts`. The film title is available on the
work/manifestation and current MOV carrier.

These exports do not directly link component `69194878` to carrier `69194879`,
or component `69194880` to carrier `69194881`. Similar numeric IDs/order are
not sufficient evidence for assigning formats between these levels.

The [component-level example](../../submission/digifilm_727480_20130730.source.json)
uses explicit `CopiedFrom` component references, their explicitly linked
manifestation formats, and the common film title. The user approved component
sources on 2026-11-02. Local tests confirm that the current builder produces
this source fragment from these four exports, including when read from a
retained SIP after work cleanup. No additional catalog calls are required.
The rest of the metadata, including carrier-level `relation`, remains unchanged.

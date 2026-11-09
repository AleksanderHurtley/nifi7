# Produksjon submission examples

## Earlier complete capture before source enrichment

[digifilm_815655_20130726.submission.json](digifilm_815655_20130726.submission.json)
records the user's final corrected `submission.json` from nifi7, supplied on
2026-11-03 after rerunning Catalog/submission for the same package. The values
are retained as supplied; whitespace is normalized, including empty arrays.
It came from:

```text
/fc1/film_produksjon/payloads/digifilm_815655_20130726/submission.json
```

The corresponding [event capture](../events/README.md#digifilm_815655_20130726)
contains the three unchanged events from the package's original processing.

This example records the corrected behavior:

- Main title `Per på landet`, director `Gran, Sverre`, production company
  `Norsk Dokumentarfilm`, production year `1964`, and the farm-visit description
  match the [supplied WORK export](../catalog_dump/digifilm_815655_20130726_WORK_815655.xml).
- The stale alternative title `COLUMBI EGG SOM SKOLEMAT` and SAS `SegmentOf`
  relationships are absent. Country `Norge` appears only once.
- `source` and the carrier `CopiedFrom` relations use item parts `69119828`
  and `69119830`. Source entries contain typed identifiers and matching URIs.
- Source descriptions are omitted because matching source-carrier details were
  not available to the builder. The subsequent
  [complete five-export capture](../catalog_dump/digifilm_815655_20130726/README.md)
  confirms that the formats identify components, with no explicit mapping to
  these source carriers. Component-level formats are not substituted.
- Empty `alternative`, `publisher`, and `language` lists are retained as output
  by the flow; the capture does not add fields that the script did not produce.

This is user-provided live output, not confirmation of DPS API acceptance or
completed preservation. The original MOV has not been independently checked locally.

## Latest complete capture with source descriptions

[digifilm_815655_20130726.enriched-submission.json](digifilm_815655_20130726.enriched-submission.json)
records the user's subsequent live NiFi output supplied on 2026-11-03 after
adding the two `COPIED_FROM` responses and supporting their embedded parent
titles. It matches the previously generated expected payload and is verified
by the production-local submission tests against all
[seven exports](../catalog_dump/digifilm_815655_20130726/README.md). Whitespace
is normalized. This confirms the generated payload, not DPS acceptance; the
earlier capture above is retained separately.

The source records directly confirm `Image/x-dpx` for `69119828` and `Audio/wav`
for `69119830`. Their own `Title` fields are absent; the explicit immediate
parent references each embed `Per på landet`. Source IDs stay at item-part
level. Every other payload field is retained from the earlier live submission.

## Expected output with relation titles

[digifilm_815655_20130726.titled-relations.submission.json](digifilm_815655_20130726.titled-relations.submission.json)
shows the next builder revision's expected output. Its two blank `CopiedFrom`
titles are filled with `Per på landet` using their exact source IDs. The source
descriptions still include each source's format; all other fields match the
latest live capture above. This file is verified by the production-local
integration test, not yet supplied as a new live NiFi result. The earlier live
files are not rewritten to imply that this revision has been deployed.

## Source fragments

These show only `metadata.source`, not complete submission requests:

- [digifilm_815655_20130726.source.json](digifilm_815655_20130726.source.json)
  extracts the sources from the complete capture above.
- [digifilm_815655_20130726.enriched-source.json](digifilm_815655_20130726.enriched-source.json)
  extracts the source descriptions confirmed in the latest live output.
- [digifilm_727480_20130730.source.json](digifilm_727480_20130730.source.json)
  is the expected item-part source output tested against the four catalog dumps.
- [digifilm_1300107_20130729.source.json](digifilm_1300107_20130729.source.json)
  uses item-part references from the user's earlier FlowFile attributes.
- [digifilm_801190_20130711.source.json](digifilm_801190_20130711.source.json)
  illustrates item-part descriptions supported by the user's Collections
  screenshots, not a complete generated live payload.

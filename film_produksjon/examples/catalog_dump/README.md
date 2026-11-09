# Produksjon catalog-dump evidence

[digifilm_801190_20130711_DIGITAL_ITEM_PART_70398711.xml](digifilm_801190_20130711_DIGITAL_ITEM_PART_70398711.xml)
was supplied as pasted `cat` output on 2026-11-02. The leading shell command
was removed; the supplied XML bytes were retained. This is a user-provided
catalog capture, not a direct API fetch or a checksum-verified copy from nifi7.

The user recorded the source location as:

```text
/fc1/film_produksjon/work/digifilm_801190_20130711/metadata/descriptive/
```

Observed fields:

- Current production record: `70398711`.
- Current record's main title: `Over Nordpolen : med Hjalmar Riiser-Larsen`.
- `CopiedFrom` source IDs: `68902056` and `68902058`.
- Both `copied_from.title` fields are empty. Neither source record is embedded
  in the XML. Other nested titles refer to the current record's parent item,
  manifestation, and work, not the two `CopiedFrom` source records.

The local source lookup cannot extract those source records' own titles or
file formats from this capture. The user clarified that `description` should
show the title and source file format. Collections screenshots supplied on
2026-11-02 confirm the intended mapping:

| Source priref | Title | Source file format |
|---|---|---|
| `68902056` | Over Nordpolen : med Hjalmar Riiser-Larsen | Image/x-dpx |
| `68902058` | Over Nordpolen : med Hjalmar Riiser-Larsen | Audio/wav |

The [expected source fragment](../submission/digifilm_801190_20130711.source.json)
uses that screenshot evidence. The runtime script must receive source records
with these fields to produce the descriptions automatically; the screenshot
mapping is not hard-coded into the script. The production record's own
`file.format=Video/quicktime` must not be assigned to its source objects.

## WORK capture for Per på landet

[digifilm_815655_20130726_WORK_815655.xml](digifilm_815655_20130726_WORK_815655.xml)
was supplied as pasted server output on 2026-11-03 from
`transfer/digifilm_815655_20130726/metadata/descriptive/`. Only the XML was saved;
the shell commands and preceding submission JSON were removed. This is a
user-provided capture, not a direct API fetch or a checksum-verified server copy.

The record has title `Per på landet`, director `Gran, Sverre`, production company
`Norsk Dokumentarfilm`, production year `1964`, country `Norge`, and a description
of Per visiting his aunt and uncle's farm. It has no alternative title or
work-level relationship fields. An earlier supplied submission incorrectly retained
`COLUMBI EGG SOM SKOLEMAT` and two SAS `SegmentOf` relationships from a previous
test. The submission regression tests use this capture to verify that the unified
builder ignores stale metadata attributes when it builds a fresh payload.
The [corrected live submission](../submission/digifilm_815655_20130726.submission.json)
records the user's subsequent rerun without those stale fields or duplicate countries.

The subsequent [complete five-export capture](digifilm_815655_20130726/README.md)
includes both digital items, their combined item-part export, the manifestation
and WORK. It confirms that source-carrier titles are empty and that the listed
DPX/WAV formats identify ITEM/components, not the source item parts.
The same directory now also holds the two subsequently captured `COPIED_FROM`
responses. They document each source carrier's own format and its explicit
parent link/title. The source-builder tests cover both the original five-file
capture without descriptions and the enriched seven-file data set.

A subsequent user-provided field report for parent component `68902057`
confirmed explicit child metadata: `parts.reference.lref=68902058`,
`parts.title=Over Nordpolen : med Hjalmar Riiser-Larsen`, and
`parts.fileformat=Audio/wav`. This is another supported lookup route when the
corresponding `Parts` block is present in an exported XML file. The text report
does not mean that the original production-record XML contains those fields.

## New export names and primary selection

The [selected-primary layout](selected-primary-layout.txt) illustrates the new
short names and one-primary traversal. The package's own record uses
`PACKAGE_CATALOG_RECORD_<priref>.xml`; its type is read from the XML. Existing
XML captures keep their actual old names, including the combined duplicate response. They remain evidence of
what the catalog returned; they do not imply cleanup has occurred. The new
[local traversal test](../../tests/catalog-dump.groovy) uses those captures and
constructs a separate synthetic one-record response to test post-cleanup URN
validation. No live record was removed or rewritten.

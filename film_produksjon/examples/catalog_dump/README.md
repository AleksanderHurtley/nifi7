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

A subsequent user-provided field report for parent component `68902057`
confirmed explicit child metadata: `parts.reference.lref=68902058`,
`parts.title=Over Nordpolen : med Hjalmar Riiser-Larsen`, and
`parts.fileformat=Audio/wav`. This is another supported lookup route when the
corresponding `Parts` block is present in an exported XML file. The text report
does not mean that the original production-record XML contains those fields.

# Produksjon submission body

Use [01_Build submission body.groovy](01_Build%20submission%20body.groovy) after
[Catalog](../03_Catalog/README.md). One ExecuteGroovyScript reads the package's
catalog exports and builds the complete UTF-8 JSON body. PutFile saves that
body as `submission.json`. This replaces the four metadata extractors, the
old body builder and the filename UpdateAttribute.

```text
Input → Build submission body → Save submission.json → Output
                 failure ↘        failure ↙
                            Failure
```

## NiFi setup

1. **ExecuteGroovyScript — Build submission body**: paste the entire
   `01_Build submission body.groovy` into **Script Body**. Connect `success` to
   PutFile and `failure` to the PG's Failure port.
2. **PutFile — Save submission.json**: reuse the current file writer with:

   | Property | Value |
   |---|---|
   | Directory | `${payloads.dir}` |
   | Conflict Resolution Strategy | `replace` |

   PutFile takes the name from `filename`, already set to `submission.json`
   by the script. `replace` ensures a rerun overwrites the initialized or
   earlier file. Connect `success` to Output and `failure` to Failure.
   See the [Apache NiFi PutFile reference](https://nifi.apache.org/components/org.apache.nifi.processors.standard.PutFile/).

Connect the completed Catalog output directly to this PG's input. Output goes
on to E-ARK. In the live PG, remove the four old Get processors and Set filename
once the two-step path is connected. Use only this production script; the
bevaring submission group is separate. Use one active run per package.

The script makes no API calls and does not write the payload file itself.
`submission.payload.status=OK` means the body was built; PutFile success is
what confirms it was saved. An existing payload on disk is not updated until
PutFile runs.

## Input and output

| Input | Meaning |
|---|---|
| `package.name` | Package name and DPS `objectId` |
| `catalog.dump.status` | Must be `COMPLETE` |
| `catalog.primary.priref` | Selected `ITEM_PART` from Catalog |
| `metadata.descriptive.dir` | Absolute work directory containing the catalog exports |
| `payloads.dir` | Absolute output directory for PutFile |
| `transfer.dir` | Optional retained SIP root; used only when work metadata is absent |

The selected record must match the package URN. Existing `catalog.primary.urn`,
`catalog.urn`, `media.urn` and `objectId`, if present, must agree with the
package. Historical export names remain readable when the selected primary
and completed catalog status are supplied. The script reads one catalog
snapshot: work when present, otherwise the retained SIP. Missing fields in
work are never filled from a previous SIP. A missing or wrong primary, invalid
catalog XML or an incomplete catalog run goes to Failure.

Each XML export is parsed once. The script removes the former extractors'
metadata attributes, including stale `metadata.pidDataUrns` and
`metadata.digitalItemPartRelations`. It does not use those attributes as
fallbacks. Package paths, identity, verification results and unrelated
metadata attributes are kept. Success sets:

- `filename=submission.json`
- `mime.type=application/json`
- `submission.payload.path=<payloads.dir>/submission.json`
- `submission.payload.status=OK`

Failure sets `submission.payload.status=FAIL`, `submission.payload.error`,
`error.stage=submission.body` and `error.message`. It does not route a previous
payload to PutFile.

## Metadata mapping

The submission shape, `priority=70`, film type, provenance text and existing
field mapping are preserved. Metadata comes from the selected primary and its
explicit parent line, so other same-URN candidates and unrelated WORK exports
are excluded.

| Catalog data | Submission fields |
|---|---|
| Primary `ITEM_PART` | URN identifiers, direct `CopiedFrom` and `CopiedTo` relations |
| WORK on its parent line | Main/alternative titles, credits, countries, dates, descriptions and work relations |
| ITEM on its parent line | Language and usage |
| Optional legacy `ANALOG_ITEM_PART` exports | Existing barcode-to-`HYLLESIGNATUR` mapping |
| Direct `CopiedFrom` references and matching records | `metadata.source` |

WORK is optional. If no WORK title is available, use a title from the primary
or its parent line. Missing optional fields produce empty lists; absence of
any usable title fails with a clear error. An absent or blank source title or
format is allowed. The [planned digital-file date mapping](../docs/METADATA_MAPPING.md#planned-submission-dates-and-optional-work)
remains separate: this script maps the existing WORK dates to `metadata.date`.

Each distinct direct `CopiedFrom` carrier becomes one source with a typed
Collections priref and its record URI. The parent ITEM's and source records'
copy histories are not followed. A source's own title and `file.format` are
preferred after any nonempty relationship title. An explicit immediate
parent title or a `Parts` block with the exact source priref may fill missing
details. A component's own format is never substituted for its child.

Descriptions combine `title — format` when both exist, or use the field that
is available. They are omitted if neither is available. Blank `CopiedFrom`
relation titles get the matched source title without the format. Nonempty
relation titles and other relation types are retained. Source URNs and barcodes
are not used as the production file's identifiers. With zero direct sources,
`metadata.source` is omitted.

See the [submission examples](../examples/submission/README.md) for captured
payloads and the locally verified expected output with enriched relation titles.

## Local verification

```sh
groovy film_produksjon/tests/submission-body.groovy
groovy film_produksjon/tests/catalog-dump.groovy
```

The tests compare the full payload against the existing Per på landet example
and exercise the complete Catalog-to-submission handoff with mocked HTTP. They
cover current and historical filenames, stale attributes, optional WORK, one
snapshot at a time, source descriptions, UTF-8/escaping, filename/MIME and
failure routing. The tests run without live NiFi or DPS; PutFile wiring should
be checked with one package before proceeding to E-ARK.

# Catalog dump: one primary record, direct parents and sources

Use the two scripts in this directory, in this order:

1. **01_Prepare catalog dump.groovy** — initialize the package URN request.
2. **02_Save catalog response.groovy** — validate/save each response and advance the request queue.

Run the [standalone catalog cleanup](../catalog-cleanup/README.md) before
starting Film Produksjon. This NiFi group reads from the catalog, validates the
package's primary record, and exports its metadata to work.

## Files and names

The package directory already supplies the package identity. Its own record is
named `PACKAGE_CATALOG_RECORD_<priref>.xml`, regardless of its catalog type.
Parent records use their type and priref; source records use their CopiedFrom
role and priref. No filename needs the package-name prefix:

```text
metadata/descriptive/
├── deprecated_mets/              # existing legacy XML, unchanged
├── PACKAGE_CATALOG_RECORD_162311522.xml  # selected package record
├── ITEM_162311521.xml             # its direct parent
├── MANIFESTATION_25815655.xml
├── WORK_815655.xml
├── COPIED_FROM_69119828.xml       # directly referenced source item part
└── COPIED_FROM_69119830.xml
```

`PACKAGE_CATALOG_RECORD` identifies the package's own primary record. Its
actual type (`ITEM_PART`, `ITEM`, or another supported type) remains in the XML
and run manifest. Parents retain their catalog types in their filenames. A
source's role is shown by `COPIED_FROM`.
One record has one export even if multiple references lead to it. If a source
also belongs to the parent chain, the hierarchy filename serves both roles.

Original METS/MAVIS filenames under `deprecated_mets/` are unchanged. Historical
[captured examples](../examples/catalog_dump/README.md) retain their actual
old filenames; the new [layout example](../examples/catalog_dump/selected-primary-layout.txt)
is an expected layout, not evidence of a live catalog cleanup.
The production submission readers accept both old and new export names.

## NiFi processor group

Use this group before the submission body and E-ARK generation.

```text
Input / Set URN
  → Prepare catalog dump
  → Route catalog status ──fetch──→ InvokeHTTP
        ↑                             │ Response
        └────success──── Save catalog response
        │
        └──complete──→ Output

Script failure / HTTP Failure, No Retry, Retry / router unmatched → Failure
```

1. **ExecuteGroovyScript — Prepare catalog dump**: use
   [01_Prepare catalog dump.groovy](01_Prepare%20catalog%20dump.groovy).
2. **RouteOnAttribute — Catalog status**: Route to Property name:

   | Property | Expression |
   |---|---|
   | `fetch` | `${catalog.dump.status:equals('FETCH')}` |
   | `complete` | `${catalog.dump.status:equals('COMPLETE')}` |

3. **InvokeHTTP — Fetch catalog record**: reuse existing catalog authentication,
   parameter context, SSL and proxy settings. HTTP Method `GET`, URL:

   ```text
   #{collection_url}search=${catalog.dump.query}&output=xml&database=catalogue
   ```

   `catalog.dump.query` is already URL-encoded. Do not encode it again. The first
   request searches `PID_data_URN = '<urn>'`; subsequent requests use
   `priref = <id>`. Keep Response Body Attribute Name empty, Response Body
   Ignored `false`, Response Generation Required `false`, caching disabled.
   Connect **Response only** to Save; auto-terminate **Original**.
   HTTP failures go to the package failure path, including socket timeouts.
   An incomplete response (`diagnostic.hits` greater than returned records)
   fails rather than accepting a partial set. Configure the catalog endpoint to
   return the complete result or investigate its result limit if this occurs.
4. **ExecuteGroovyScript — Save catalog response**: use
   [02_Save catalog response.groovy](02_Save%20catalog%20response.groovy).
   Connect success back to **Route catalog status**, never Prepare.

Use one concurrent task per processor and one active run per package. Queue,
visited records and current request are FlowFile attributes, not processor
state. Two packages use separate directories/state. Concurrent retries of the
same package are not supported. The output body is empty; downstream readers
use the completed exports.

### Remove the former stage deletion branch

1. Stop the processors in the stage deletion branch and inspect any queued
   FlowFiles. For a package with a deletion already in progress, check the
   catalog before reprocessing it from **Prepare catalog dump**.
2. Connect **Save catalog response** `failure` directly to **Failure**. Keep
   `success` connected to **Route catalog status**.
3. Disconnect and remove **Route stage duplicate**, **Read duplicate report**,
   **Advance duplicate deletion**, **Route deletion status**,
   **Fetch duplicate parent**, and **Delete stage record** wherever present.
   This includes the former `complete` connection back to **Prepare catalog dump**.
4. Load the two updated Groovy scripts and remove `catalog.duplicates.mode`
   from **Set catalog attributes**. For Film Produksjon, the required primary
   type defaults to `ITEM_PART`.

### Inputs

| Attribute | Meaning |
|---|---|
| `package.name` | This package's directory name |
| `metadata.descriptive.dir` | Existing/new absolute descriptive directory in work |
| `payloads.dir` | Absolute package payload directory, outside descriptive metadata |
| `catalog.urn` | URN to look up; falls back to `media.urn`, then `urn` |
| `catalog.required.primary.type` | Optional expected root type, defaults to `ITEM_PART` |
| `catalog.expected.primary.priref` | Optional expected priref for this package |

For this flow, `media.urn` is already set by Fetch. Alternatively retain
`Set urn` with `URN:NBN:no-nb_${package.name}`. Set an expected priref only when it is known for the individual package.

### Outputs

| Attribute | Meaning |
|---|---|
| `catalog.primary.priref` | Exactly one validated package record; retain for catalog archive-ID updates |
| `catalog.primary.type` / `.urn` | Primary record identity |
| `catalog.dump.status` | `FETCH`, `COMPLETE`, or `FAIL` |
| `catalog.dump.count` | Number of distinct exported records |
| `catalog.dump.query` / `.priref` | Next request; absent when complete |
| `catalog.dump.records` | Export inventory: priref, type, role, filename, direct parents |
| `catalog.dump.run.dir` | Unique evidence directory under `<payloads.dir>/catalog-runs/`; catalog exports themselves are saved directly in work |
| `catalog.dump.manifest.path` | Completed inventory JSON |
| `catalog.dump.report.path` | Report on multiple URN matches, if generated |

Prepare resets previous transient `catalog.dump.*`, `catalog.primary.*` and old
`catalog.copied_from.*` state. Package, media verification and other stages are
preserved. Errors from this catalog stage are reset on retry. The submission builder removes the former extractors' derived metadata
attributes and builds the payload directly from the completed catalog exports.

## Lookup, hierarchy and copy history

An initial URN response must contain exactly one active matching record before
traversal starts. No matches, multiple matches, or a sole `ITEM_PART` with
explicit zero file size go to **Failure**. The primary must match the required
type and any explicitly supplied expected priref. For multiple matches,
`DUPLICATES.json` lists each priref, type and reported size alongside the raw
`URN_RESPONSE.xml`. Resolve the catalog issue separately, then restart the
package at **Prepare catalog dump** for a fresh URN lookup.

From the primary, follow direct `Part_of/part_of_reference.lref`, falling back
to the priref of its explicitly expanded immediate parent. Fetch each parent
and repeat until no parent remains or WORK is reached. Missing WORK, missing
manifestation, and a root ITEM without parents are valid for the dumper.
A populated but unresolved reference fails. Parent cycles fail. A shared
ancestor is fetched once. A 100-record limit catches unexpected expansion.

Only the **primary record's direct** `CopiedFrom/copied_from.lref` links are
followed. Do not follow the parent's or a source's copy history, source parents,
siblings, Parts children or embedded nested records as new requests. Zero, one,
two or more distinct direct sources are valid. Sources need not contain title
or format. A requested related record must return exactly one active matching
priref; HTML, wrong IDs/types, malformed XML and HTTP errors fail.

The dumper supports `ITEM_PART`, `ITEM`, `MANIFESTATION`, `WORK`. Other catalog
schemas/types require an explicit extension, not a guessed mapping. Generic
dump support does not make the film-specific submission/event scripts generic.
The submission builder combines WORK dates and the original MOV creation date.
[Submission date mapping](../docs/METADATA_MAPPING.md#submission-dates-and-optional-work)

## Direct saving and retrying

Each validated catalog response is saved byte-for-byte directly in
`<metadata.descriptive.dir>/`, using its final filename. The first successful
lookup therefore immediately creates `PACKAGE_CATALOG_RECORD_<priref>.xml` in
work; parent and source exports appear as their responses arrive. Each file is
written through a temporary sibling and replaced atomically where supported,
so an HTTP/XML error does not replace an existing export with invalid content.

The separate run directory under `<payloads.dir>/catalog-runs/` contains only
lookup evidence and control information: `URN_RESPONSE.xml`, `DUPLICATES.json`
when duplicates are found, and `CATALOG.json` when the run completes. Parent and
source exports are not duplicated there. These support files stay outside the
SIP and remain until final package cleanup. Copy them elsewhere first if longer
retention is needed.

Only the router's `complete` output may reach submission/E-ARK. A failed run
can leave partial new exports alongside old files in work, and must stay on
Failure. Restart from Prepare to rebuild the request state and refetch the
records. When every request succeeds, the script checks that all expected
exports exist, removes recognizable old catalog exports not belonging to this
run, writes the completion inventory and sets `COMPLETE`. Legacy files under
`deprecated_mets/`, other unrecognized XML, source media and a retained SIP are
untouched. One active run per package remains required.

When updating an in-progress test from the former payload-staging version,
restart the package at Prepare. Earlier responses may only exist in the old
payload run; the new script requires every completed export to exist in work.
Existing old payload runs can remain until final package cleanup. Rebuild the
SIP if its metadata changes. Event text and historical payload examples are
unchanged by this storage adjustment.

## Local verification

```sh
groovy film_produksjon/tests/catalog-dump.groovy
groovy film_produksjon/tests/submission-body.groovy
```

The traversal test uses captured catalog responses and a simulated response
after external cleanup. It verifies rejection of duplicate matches and a sole
zero-byte part, type and expected-ID checks, hierarchy traversal, direct
sources, shared references, cycles, byte-for-byte exports, failure/restart,
stale-export cleanup on completion, and submission output. HTTP delivery is mocked.

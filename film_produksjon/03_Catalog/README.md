# Catalog dump: one primary record, direct parents and sources

Use the two scripts in this directory, in this order:

1. **01_Prepare catalog dump.groovy** — initialize the package URN request.
2. **02_Save catalog response.groovy** — validate/save each response and advance the request queue.

These handle the complete agreed catalog dump together with InvokeHTTP and
RouteOnAttribute. The former standalone CopiedFrom scripts have been removed.

The implementation is ready for local NiFi testing; no live catalog changes
have been made. Deploy only in `film_produksjon` for now.
The request/traversal logic can later be reused by other flows. The optional
zero-byte duplicate proposal is specific to film production.

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

## NiFi replacement group

Replace the old Catalog **dump** chain (initial item-part request, parent split,
parent requests/join, and CopiedFrom group) with this sequential group. Keep
the submission body group after it, and run everything before E-ARK generation.
Do not append this group after the old traversal or run both.

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

### Inputs

| Attribute | Meaning |
|---|---|
| `package.name` | This package's directory name |
| `metadata.descriptive.dir` | Existing/new absolute descriptive directory in work |
| `payloads.dir` | Absolute package payload directory, outside descriptive metadata |
| `catalog.urn` | URN to look up; falls back to `media.urn`, then `urn` |
| `catalog.duplicates.mode` | Set `film_produksjon` here; default `stop` for other flows |
| `catalog.required.primary.type` | Optional expected root type; defaults to `ITEM_PART` in production mode |
| `catalog.expected.primary.priref` | Expected surviving record after external cleanup, when restarting on a new FlowFile |

For this flow, `media.urn` is already set by Fetch. Alternatively retain
`Set urn` with `URN:NBN:no-nb_${package.name}`. Do not set an expected priref as a
constant for all packages. A retry on the same failed FlowFile carries forward
its proposed primary only when both package and URN still match.

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
| `catalog.dump.report.path` | Duplicate comparison report, if generated |
| `catalog.dump.proposed.primary.priref` | Production duplicate candidate, never deletion authorization |

Prepare resets previous transient `catalog.dump.*`, `catalog.primary.*` and old
`catalog.copied_from.*` state. Package, media verification and other stages are
preserved. Errors from this catalog stage are reset on retry. The submission builder removes the former extractors' derived metadata
attributes and builds the payload directly from the completed catalog exports.

## Lookup, hierarchy and copy history

An initial URN response must contain exactly one active matching record before
traversal starts. Zero matches fail. Multiple matches always fail with retained
original response and a report. Required primary type and an expected priref,
when supplied, must match.

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

## Production duplicates and cleanup

In `film_produksjon` mode, exactly one ITEM_PART with positive `file.size` plus
one or more zero-byte ITEM_PARTs yields a **proposed** surviving priref. Missing
sizes, multiple positive records or different types produce no automatic
proposal. A sole production primary with explicit zero size also fails for
inspection. Catalog MD5/size is not compared to the verified MOV to select it.

The complete original URN response is saved as `URN_RESPONSE.xml`, and
`DUPLICATES.json` lists candidates and all differing top-level XML field groups
against the proposal, including repeated-field context and XML attributes.
The report deliberately retains differences in identifiers, parent references
and administrative history. Empty versus missing fields can appear as a
difference; no semantic equivalence or safe deletion is asserted.

The package goes to **Failure**. Review relevant values, unique information,
identifiers and incoming references before approved external cleanup. The
scripts do not merge, change or delete catalog records. No catalog mutation API
has been configured here. For the real `Per på landet` capture, `162311522` is
proposed, but parent and legacy identifiers also differ: this is not a clean
"zero-byte copy with no unique information" case.

After cleanup, rerun Prepare. On the same FlowFile it retains the proposal as
the expected surviving priref; on a new one set `catalog.expected.primary.priref`.
This makes a **new URN request** and requires exactly one matching record with
that ID. Zero/multiple results or an unexpected ID fail. The new response is
preserved in a new run directory. Once validated, the selected primary's own
parents/copy links drive traversal and submission; candidate metadata is not
unioned together.

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

The new traversal test uses the real duplicate response and parent/source
captures, with a clearly simulated post-cleanup response. It verifies failure
reports, expected-ID checks, complete and partial hierarchies, direct sources,
shared references, cycles, immediate byte-for-byte exports in work, partial
failure/restart, stale-export cleanup on completion, evidence-only payload
storage, new filenames and the submission output. HTTP delivery is mocked; there are no live catalog writes.

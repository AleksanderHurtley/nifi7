# Initialize produksjon

Run these two ExecuteGroovyScript processors in order:

```text
Manual trigger or DB pickup
  → 01_Initialize Flowfile
  → 02_Initialize Directories
  → next PG
```

Set each processor's Script Body from the corresponding `.groovy` file, or
deploy the file on nifi7 and use Script File. Use one script source property
per processor. Connect each `failure` relationship to the review/failure path.
For the manual test, keep the STARTED updater and later DB/DPS writers stopped.

## Input and output attributes

The only required inputs are `package.name` and `package.path`. The supported
name is `digifilm_<numeric-id>_<YYYYMMDD>` with a valid calendar date. The source
path must be absolute, have the layout
`/global/film/00/film/produksjon/<year-month>/<package.name>`, and name that same
package. Initialization validates these strings without inspecting SAM-FS.

Existing `package.uuid`, `prod_db_id`, `prod_db_description`, and FlowFile
content are retained. The scripts use attributes, so an empty body is fine.
They perform no database writes or media transfers.

For the current manual test, the main output attributes are:

| Attribute | Value |
|---|---|
| `flow.id` | `film_produksjon` |
| `package.name` | `digifilm_1300107_20130729` |
| `source.dir` | `/global/film/00/film/produksjon/201307/digifilm_1300107_20130729` |
| `source.video.path` | `<source.dir>/lossless/digifilm_1300107_20130729.mov` |
| `source.mets.path` | `<source.dir>/digifilm_1300107_20130729.xml` |
| `source.metadata.dir` | `<source.dir>/meta` |
| `source.logs.dir` | `<source.dir>/logs` |
| `work.dir` | `/fc1/film_produksjon/work/digifilm_1300107_20130729` |
| `rep.dir` | `<work.dir>/representations/primary_20130729` |
| `rep.data.dir` | `<rep.dir>/data` |
| `video.filename` | `digifilm_1300107_20130729.mov` |
| `video.output.path` | `<rep.data.dir>/digifilm_1300107_20130729.mov` |
| `payloads.dir` | `/fc1/film_produksjon/payloads/digifilm_1300107_20130729` |
| `events.payload.path` | `<payloads.dir>/events.ndjson` |
| `submission.payload.path` | `<payloads.dir>/submission.json` |
| `transfer.dir` | `/fc1/film_produksjon/transfer/digifilm_1300107_20130729` |
| `metadata.descriptive.depr_mets.dir` | `<work.dir>/metadata/descriptive/deprecated_mets` |
| `metadata.technical.dir` | `<rep.dir>/metadata/technical` |
| `metadata.preservation.dir` | `<rep.dir>/metadata/preservation` |

The first script also sets the metadata directory attributes used by the
[staging map](../docs/METADATA_MAPPING.md). Source METS and the four known legacy
MAVIS XML snapshots share `metadata.descriptive.depr_mets.dir`, grouping legacy
metadata under `deprecated_mets` in line with bevaring's structure. MAVIS
snapshots are not METS documents, but belong to this legacy group rather than
the current catalog exports. All files retain their names and complete contents;
the historical folder name does not mean the information is without value.
The script sets `total.pipeline.start` on
the first run and retains an existing value when the same FlowFile is retried.
No DPX, batch, separate-audio, or RAWcooked paths are generated.

You can override `work.base.dir`, `payloads.base.dir`, and `transfer.base.dir`
on the incoming FlowFile. The defaults are `/fc1/film_produksjon/work`,
`/fc1/film_produksjon/payloads`, and `/fc1/film_produksjon/transfer`.
Overrides must be absolute output roots outside the source tree.

When deploying these roots and the bevaring-style METS placement, replace both
Initialize scripts and the metadata Fetch script. Generate a fresh manual
FlowFile and run Initialize again; old FlowFiles retain their old path
attributes, and explicit base-directory overrides are still honored. Existing
staging directories and payload files are not moved or deleted automatically.
The separate `metadata.descriptive.legacy_mavis.dir` attribute is no longer
emitted or required. If a previous test already populated `legacy_mavis/`,
compare and move its four known XML files into `deprecated_mets/`, without
overwriting different files, then remove the old directory only when empty.
Do not package both the old and new layouts. A new metadata fetch copies the
source files to the new location but does not clean up the old location.

After the staging review reported `NBSIPSTR20`, representation-level
`metadata.other.dir` is retired and ignored by Fetch. Deploy both Initialize
scripts plus the metadata Fetch script together and rerun Initialize to set
`metadata.technical.dir`. Fetch writes ffprobe under `technical/ffprobe/`,
JHOVE under `technical/jhove/`, and conversion logs under `preservation/logs/`.
Use fresh package staging or reconcile old copies first: an existing
representation `metadata/other` causes Fetch to fail, not silently package it.
Old JHOVE copies in preservation are not automatically deleted. Regenerate
the SIP after all paths are corrected; don't rename files inside a completed
SIP without rebuilding its METS and checksums. Package-level
`metadata/other/unclassified` remains separate from representation metadata.

## Directory and payload behavior

The second script creates work, representation data, descriptive metadata,
the shared legacy metadata directory, preservation metadata, technical metadata, and
payload directories. It creates `events.ndjson` and `submission.json` as
empty files if absent, preserving existing files and their content on reruns.
Unlike the bevaring initializer, it does not reset existing payloads.
Paths are based on the package name, so generating a new `package.uuid` alone
does not create fresh directories. Use separate local base-directory overrides
when an independent test needs fresh staging and payload files.

`transfer.dir` is prepared as an attribute for E-ARK to create its output.
The unclassified directory is prepared as an attribute and created later
only if unknown files are found. Source existence, media availability, source
checksums, and the older package's metadata contents are checked at fetch,
not during initialization.

On success, expect `setup.status=OK`. On failure, expect `setup.status=FAIL`
from directory initialization and `error.stage` of `initialize.flowfile` or
`initialize.directories`, with `error.message` and `error.details`.

## Local verification

From the repository root, with Groovy 4 available:

```sh
groovy film_produksjon/tests/initialize.groovy
```

The test uses mock NiFi sessions and temporary local directories. It checks
both package examples, invalid inputs, stable dates, custom output roots,
directory creation, failure routing, and preservation of existing payloads.
It does not need SAM-FS or a database.

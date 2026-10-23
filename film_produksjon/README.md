# film → produksjon (NiFi)

This directory holds the source examples and design for the film → produksjon
flow on `nifi7`, with its own top-level NiFi process group. It transfers the
existing viewing/production files from SAM-FS into E-ARK and DPS.

Status: package discovery, initialization, and fetch/metadata scripts are
available. The existing produksjon DB pickup has been tested with production
line 82, and initialization has completed successfully on nifi7. The fetch
scripts have passed local tests and the user-reported 2013 live copy/verification.
Catalog/submission-body preparation is complete in the live flow and E-ARK is
configured; that configuration is not fully exported into the repo. The
[post-E-ARK event builder](04_Events/README.md) is locally tested on the 2023
example, and its [2013 live output](examples/events/README.md) is recorded from
the successful nifi7 run after work cleanup. Confirmed DPS completion remains pending.

## Prepare package FlowFiles

Use [00_Package discovery](00_Package%20discovery/README.md) to list
`/global/film/00/film/produksjon/<year-month>/<package>` and emit one FlowFile
per package, with `package.name` and `package.path`. These are inputs to the
database preparation steps; the script does not insert database rows.

## Flow design

The agreed approach is a simple transfer flow: copy the existing media
byte-for-byte, verify it, organize its metadata, generate an E-ARK SIP, and
submit it to DPS. Reuse the existing NiFi groups and conventions with small,
produksjon-specific adaptations.

Local output uses `/fc1/film_produksjon/` with `work/`, `payloads/`, and
`transfer/` beneath it. Source METS is preserved under package-level
`metadata/descriptive/deprecated_mets/`, matching bevaring. The four legacy
MAVIS XML snapshots are grouped there unchanged, separate from current catalog
exports. Regular `.done` files are ignored; JHOVE, ffprobe, and logs keep their
representation-level locations.

The [flow overview and step-by-step walkthrough](docs/FLOW_OVERVIEW.md) use
five stages:

1. Pick up the package from the DB and initialize paths.
2. Copy the media and source metadata, then verify source checksums.
3. Fetch current catalog metadata and prepare the submission.
4. Generate the E-ARK SIP (including checksums), then build the three events.
5. Submit to DPS, record completion, and clean local staging.

The first implementation covers the captured loose-MOV package. DPX batching,
RAWcooked, compression statistics, and scanner-event extraction are outside
this flow. Keep changes local to produksjon; shared-code refactoring and a new
Grafana dashboard are deferred until there is a concrete need.

The [metadata mapping](docs/METADATA_MAPPING.md) records the sample's existing
checksums, viewing-file identity, provenance, and proposed E-ARK staging paths.
The current manual test uses `digifilm_1300107_20130729`, DB item `49062190`,
bypassing the STARTED update. See [Initialize](01_Initialize/README.md) for
its scripts and output attributes, then [Fetch](02_Fetch%20files/README.md)
for the metadata-first test and MOV copy/verification.
After successful E-ARK generation, capture its completion timestamp, then
[clean up work staging](05_Cleanup%20work/README.md) while retaining the
completed SIP and external payloads for DPS. Use [Events](04_Events/README.md)
to read the retained SIP metadata and write historical FFmpeg creation,
transfer, and SIP-creation records together. The builder also supports retries
after work deletion; no restaging is required for event creation.
Leave the E-ARK **Checksum File** property unset; there is no separate
produksjon checksum-manifest step.

## Source package examples

Copy small metadata files and record the source layout in
[examples/sam-fs/](examples/sam-fs/README.md), one directory per package.
Media can remain on SAM-FS while we develop the E-ARK mapping from these
examples. Record its paths and sizes, plus archive members where applicable.

The [digifilm_796646_20230321 example](examples/sam-fs/digifilm_796646_20230321.notes.md)
now includes its metadata and inventory. This package has a loose MOV file.

The [digifilm_1300107_20130729 layout](examples/sam-fs/digifilm_1300107_20130729.notes.md)
also records a production log and five `.done` files found in an older package.
Its original XML/log contents are not copied locally; the separately captured
[event output](examples/events/README.md) documents the successful live event build.

Optional complete packages or partial downloads belong in
[local-packages/](local-packages/README.md), which is ignored by Git except
for its README.

## Implementation

Work through the five stages in the [flow overview](docs/FLOW_OVERVIEW.md),
reviewing the relevant existing processors before adapting them. Get one
representative package through to confirmed DPS completion before expanding
to other layouts or adding monitoring features. Add numbered stage directories
as their scripts are implemented, and record the live process-group ID,
parameters, DB queries, and DPS/E-ARK properties as they are confirmed.

Keep reporting and operational evidence for this flow in this directory.
Use [film → bevaring](../film_bevaring/README.md) as a reference for conventions,
and confirm this flow's configuration before reusing scripts.

See the [repository overview](../README.md) for directory conventions.

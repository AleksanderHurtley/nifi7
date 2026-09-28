# film → produksjon (NiFi)

This directory holds the source examples and design for the film → produksjon
flow on `nifi7`, with its own top-level NiFi process group. It transfers the
existing viewing/production files from SAM-FS into E-ARK and DPS.

Status: the package-discovery script, design, and metadata examples are
available. The remaining processing stages are not implemented yet.

## Prepare package FlowFiles

Use [00_Package discovery](00_Package%20discovery/README.md) to list
`/global/film/00/film/produksjon/<year-month>/<package>` and emit one FlowFile
per package, with `package.name` and `package.path`. These are inputs to the
database preparation steps; the script does not insert database rows.

## Flow design

The [proposed flow](docs/FLOW_OVERVIEW.md) follows film → bevaring's DB pickup,
initialization, staging, fixity, catalog, E-ARK, and DPS stages, with a small
finalization/cleanup step. It omits DPX batching, RAWcooked, and compression
statistics, and keeps a minimal DB/Grafana monitoring model.

The [metadata mapping](docs/METADATA_MAPPING.md) records the sample's existing
checksums, viewing-file identity, provenance, and proposed E-ARK staging paths.
The first integration step is to capture the existing DB bootstrap and pickup
queries and define which rows belong to this flow.

## Source package examples

Copy small metadata files and record the source layout in
[examples/sam-fs/](examples/sam-fs/README.md), one directory per package.
Media can remain on SAM-FS while we develop the E-ARK mapping from these
examples. Record its paths and sizes, plus archive members where applicable.

The [digifilm_796646_20230321 example](examples/sam-fs/digifilm_796646_20230321.notes.md)
now includes its metadata and inventory. This package has a loose MOV file.

The [digifilm_1300107_20130729 layout](examples/sam-fs/digifilm_1300107_20130729.notes.md)
also records a production log and five `.done` files found in an older package.
Only its directory listing is available so far.

Optional complete packages or partial downloads belong in
[local-packages/](local-packages/README.md), which is ignored by Git except
for its README.

## Implementation

Follow the implementation order and open configuration items in the
[flow overview](docs/FLOW_OVERVIEW.md). Add numbered stage directories as their
scripts are implemented. Record the live process-group ID, parameter contexts,
DB queries, and DPS/E-ARK processor properties as they are confirmed.

Keep reporting and operational evidence for this flow in this directory.
Use [film → bevaring](../film_bevaring/README.md) as a reference for conventions,
and confirm this flow's configuration before reusing scripts.

See the [repository overview](../README.md) for directory conventions.

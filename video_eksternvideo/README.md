# video → eksternvideo (NiFi)

This directory is reserved for the video → eksternvideo flow on `nifi7`,
with its own top-level NiFi process group.

Status: ready to start; no processors or scripts have been added here yet.

## Source package examples

Copy small metadata files and record the source layout in
[examples/sam-fs/](examples/sam-fs/README.md), one directory per package.
Media can remain on SAM-FS while we develop the E-ARK mapping from these
examples. Record its paths and sizes, plus archive members where applicable.

The [AV1000026258 example](examples/sam-fs/no-nb_eksternvideo_AV1000026258_01.notes.md)
records the supplied MKV and TIFF tar listings. The intended fetch stage will
unpack the archives and map their content into E-ARK without media conversion.
Metadata collection and the final E-ARK layout are still pending.

Optional complete packages or partial downloads belong in
[local-packages/](local-packages/README.md), which is ignored by Git except
for its README.

## Starting the flow

1. Record the top-level process-group name/ID and outline the stages here.
2. Document the source and staging paths, package/metadata requirements,
   parameter contexts, and any database/DPS configuration in `docs/`.
3. Add numbered stage directories matching this flow's process groups and
   representative inputs in `examples/` as the implementation develops.

Keep reporting and operational evidence for this flow in this directory.
Use [film → bevaring](../film_bevaring/README.md) as a reference for conventions,
and confirm this flow's configuration before reusing scripts.

See the [repository overview](../README.md) for directory conventions.

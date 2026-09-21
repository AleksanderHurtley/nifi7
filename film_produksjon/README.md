# film → produksjon (NiFi)

This directory is reserved for the film → produksjon flow on `nifi7`,
with its own top-level NiFi process group.

Status: ready to start; no processors or scripts have been added here yet.

## Source package examples

Put complete SAM-FS packages in
[local-packages/](local-packages/README.md). Its contents are ignored by Git
except for the README.

Extract small reference files and structure listings into
[examples/sam-fs/](examples/sam-fs/README.md), one directory per package.
These tracked examples establish the input layout for the fetch stage.

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

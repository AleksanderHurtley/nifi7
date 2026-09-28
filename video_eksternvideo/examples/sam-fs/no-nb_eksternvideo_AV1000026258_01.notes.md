# Eksternvideo source example: no-nb_eksternvideo_AV1000026258_01

Known source-relative path: `2/no-nb_eksternvideo_AV1000026258_01` beneath
the SAM-FS `eksternvideo` directory. The absolute source root is pending.

The user supplied two `tar -tvf` listings on 2026-09-24. The
[partial inventory](no-nb_eksternvideo_AV1000026258_01.files.txt) records their
member paths and sizes. It is not a complete package inventory yet.

| Source archive | Content after unpacking | Size in bytes |
|---|---|---:|
| `original/AV1000026258_01_original.tar` | `AV1000026258_01_0001.mkv` | 46,186,028,687 |
| `reference/AV1000026258_01_reference.tar` | `AV1000026258_01_ref_0001.tif` | 6,852,786 |

No metadata, archives, or media have been copied for this package yet.
The media payloads can remain on SAM-FS while the small metadata files and
inventory are collected for development.

## Flow development

The intended fetch stage will unpack the media archives, following the
film → bevaring approach, and map the extracted files into E-ARK without
media conversion. Keep track of which files came from `original/` and
`reference/` when defining the E-ARK representations.

The metadata files are needed to define the descriptive and preservation
metadata mapping and final E-ARK layout. That mapping remains to be decided.
These listings establish the content of this example; further package
variants should have their own inventories. A later run with real media
will be needed to validate extraction, fixity, and packaging end to end.

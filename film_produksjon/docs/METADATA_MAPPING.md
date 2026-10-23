# Film produksjon: source metadata and proposed staging map

This is a proposed mapping for the captured `digifilm_796646_20230321` example,
based on its source files and the bevaring staging conventions. The E-ARK
processor's metadata properties and the local SIP profile must be checked
before treating these locations as the final package contract.

The [Fetch scripts](../02_Fetch%20files/README.md) implement this staging map.
Local tests cover the captured XML and synthetic media; live fetch of the
current 2013 test package is the next verification step.

The [simple transfer flow](FLOW_OVERVIEW.md) copies the existing MOV and source
XML byte-for-byte, verifies the copied media, and packages them with current
catalog metadata. The mapping describes that placement and identity work;
the media needs no conversion.

## What the source establishes

Source path:
`/global/film/00/film/produksjon/202303/digifilm_796646_20230321`

| Fact | Observed value |
|---|---|
| Media | `lossless/digifilm_796646_20230321.mov` |
| Media bytes | `220812884653` |
| MD5 | `872e9deca085f6f5ad419d6de0dcc615` |
| SHA-1 | `77d78bb89d7dfa65ce68b968b60c3c556a6e1423` |
| Viewing-file URN | `URN:NBN:no-nb_digifilm_796646_20230321` |
| Legacy MAVIS carrier key | `796646-14-1` |
| PREMIS preservation level | `production` |
| Historical event | `derivation`, successful, `2023-05-04T09:02:30+02:00` |
| Other source content | Seven XML files; empty `logs/`; no tar archive in this example |

The [legacy METS](../examples/sam-fs/digifilm_796646_20230321/digifilm_796646_20230321.xml)
contains one `mets:file` (`DO_0001`) whose `ADMID` refers to `OBJ_001`.
That PREMIS object gives `originalName=digifilm_796646_20230321.mov`, size,
fixity, and provenance. `FLocat` contains a URN, not a filesystem path.
Resolve this documented linkage when creating the source inventory; do not
treat the URN or historical absolute paths as paths to copy.

The [JHOVE report](../examples/sam-fs/digifilm_796646_20230321/meta/JHOVE_digifilm_796646_20230321.mov.xml)
names the same MOV under its old `/stornext/...` location and agrees with METS
on MD5, SHA-1, and size. The
[MAVIS carrier update](../examples/sam-fs/digifilm_796646_20230321/meta/MAVIS_mezzaninCarrierUpdate.xml)
also agrees. These are consistent historical references, not three independent
checks of today's bytes. Runtime fixity must still read the staged MOV.
The repository's `.metadata.sha256` file verifies the copied XML examples only.

The metadata contains format-label differences: METS declares `video/mp4`,
JHOVE reports a generic bytestream, and ffprobe reports a MOV/QuickTime-family
container with H.264 video and PCM audio. Retain the source reports unchanged;
do not rename or transcode the file based on one inherited label. Confirm the
MIME/type metadata that the new SIP processor should emit.

## Proposed file placement

Paths below are relative to `work.dir` before SIP generation.
`<rep>` means `representations/primary_20230321` for this example. This stable
name follows the bevaring convention; it does not claim that the MOV was
created on the date encoded in the package name.

| Source file | Proposed destination |
|---|---|
| `lossless/digifilm_796646_20230321.mov` | `<rep>/data/digifilm_796646_20230321.mov` |
| `digifilm_796646_20230321.xml` | `metadata/descriptive/deprecated_mets/digifilm_796646_20230321.xml` |
| `meta/mavisMezzaninCarrier.xml` | `metadata/descriptive/deprecated_mets/mavisMezzaninCarrier.xml` |
| `meta/mavisMezzaninComponent.xml` | `metadata/descriptive/deprecated_mets/mavisMezzaninComponent.xml` |
| `meta/mavisMezzaninTitle.xml` | `metadata/descriptive/deprecated_mets/mavisMezzaninTitle.xml` |
| `meta/MAVIS_mezzaninCarrierUpdate.xml` | `metadata/descriptive/deprecated_mets/MAVIS_mezzaninCarrierUpdate.xml` |
| `meta/ffprobe.xml` | `<rep>/metadata/other/ffprobe.xml` |
| `meta/JHOVE_digifilm_796646_20230321.mov.xml` | `<rep>/metadata/preservation/JHOVE_digifilm_796646_20230321.mov.xml` |
| `logs/<filename>` where present, except `*.done` | `<rep>/metadata/other/logs/<filename>` |
| Regular `*.done` files | Ignored; remain only in the source |
| Current catalog XML fetched later | `metadata/descriptive/<catalog-export-filename>.xml` |

Keep every source XML byte-for-byte, including its declared encoding and old
paths. Parse copies to produce new attributes, inventory, and submission JSON.
Preserve historical references as evidence while the newly generated SIP METS
describes the new layout. Empty source `logs/` needs no packaged file.
Capture unexpected files for review rather than silently discarding them.

The source METS combines MODS description, PREMIS technical/fixity information,
and historical events. Its complete content is retained unchanged under
`metadata/descriptive/deprecated_mets/`, matching bevaring's existing convention.
The historical folder name does not mean all embedded information is obsolete
or purely descriptive. The four legacy MAVIS XML snapshots share this directory,
including any valuable technical fields within those records. These are not
METS documents themselves; this is a legacy-metadata grouping, not a format
conversion. It follows the preservation-team-approved separation of legacy
metadata from current catalog exports shown in bevaring. Other XML files are
placed according to their role; there is no general rule that every XML file
is descriptive metadata. The separate `legacy_mavis/` directory is no longer
created; existing staging requires manual reconciliation before SIP generation.

Keep current catalog exports separate from the legacy MAVIS snapshot. The
existing submission scripts expect Axiell-style `recordList/record` exports
named with `_WORK_`, `_DIGITAL_ITEM_`, `_DIGITAL_ITEM_PART_`, and, where used,
`_ANALOG_ITEM_PART_`. The seven source XML files do not supply those exports.
Renaming legacy MAVIS files would not make them compatible.

## Older package variant: logs and workflow markers

The user-provided [2013 package inventory](../examples/sam-fs/digifilm_1300107_20130729.files.txt)
shows the same loose MOV and seven-XML layout, plus one ffmpeg log under
`logs/` and five root-level `.done` files. Only the listing is available;
metadata contents, sizes, and checksums have not been checked for this package.

Retain `logs/<filename>` unchanged under
`<rep>/metadata/other/logs/<filename>` as production history, preserving any
subdirectories, except for `.done` files. Regular `.done` files are explicitly
ignored, whether empty or nonempty, and are not copied or sent to unclassified
review. They are not evidence of current fixity or DPS status. These extra
files do not change discovery: this directory still produces exactly one
package FlowFile. Final metadata registration remains subject to the E-ARK
profile check, as for the other staged paths.

## Catalog identity and provenance

Use the viewing-file URN above to find its current digital item part, then
follow the catalog references to digital item, manifestation, and work, as
shown in the bevaring Catalog screenshot. Verify that the returned record
actually belongs to this viewing file before building the submission body.

The legacy METS records derivation from the following core-file URNs:

- `URN:NBN:no-nb_digifilm_796646_20230321_FSIG00000076`
- `URN:NBN:no-nb_digifilm_796646_20230321_FSIG00000077`

Retain these relationships in the source metadata and reconcile them with
the current catalog's relationship fields. They are provenance links, not
additional media files to fetch for this package. Do not replace the viewing
file's identity with either source URN.

## Small adaptations for this transfer

- Use a small produksjon organizer for these seven loose XML files, retaining
  JHOVE as fixity evidence and applying the placement table above. Bevaring's
  metadata tar and Scanity handling remain specific to that flow.
- Verify the staged MOV using the METS file-to-object binding, expected size,
  and digest. Conflicting evidence, ambiguous mappings, or a missing expected
  checksum must prevent a successful verification result.
- Preserve the historical FFmpeg derivation in the source METS. After E-ARK,
  the [event builder](../04_Events/README.md) also represents it as a historical
  creation event, followed by this run's transfer and SIP-creation records.
- Reuse the catalog and submission-body builders with matching current
  exports, checking the required identity and title fields before submission.
- Let EarkSIPGenerator generate package checksums with Checksum File unset,
  and reuse DPS processing with the configured produksjon paths. Shared-code
  refactoring is outside the initial scope.

No real media verification, catalog lookup, SIP generation, or DPS submission
has been performed for this mapping. Parsing and small-media copy/verification
have passed local tests; the next step is the live metadata-first test on nifi7.

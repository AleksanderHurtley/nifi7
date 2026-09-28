# Film produksjon: source metadata and proposed staging map

This is a proposed mapping for the captured `digifilm_796646_20230321` example,
based on its source files and the bevaring staging conventions. The E-ARK
processor's metadata properties and the local SIP profile must be checked
before treating these locations as the final package contract.

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
| `meta/mavisMezzaninCarrier.xml` | `metadata/descriptive/legacy_mavis/mavisMezzaninCarrier.xml` |
| `meta/mavisMezzaninComponent.xml` | `metadata/descriptive/legacy_mavis/mavisMezzaninComponent.xml` |
| `meta/mavisMezzaninTitle.xml` | `metadata/descriptive/legacy_mavis/mavisMezzaninTitle.xml` |
| `meta/MAVIS_mezzaninCarrierUpdate.xml` | `metadata/descriptive/legacy_mavis/MAVIS_mezzaninCarrierUpdate.xml` |
| `meta/ffprobe.xml` | `<rep>/metadata/other/ffprobe.xml` |
| `meta/JHOVE_digifilm_796646_20230321.mov.xml` | `<rep>/metadata/preservation/JHOVE_digifilm_796646_20230321.mov.xml` |
| Current catalog XML fetched later | `metadata/descriptive/<catalog-export-filename>.xml` |

Keep every source XML byte-for-byte, including its declared encoding and old
paths. Parse copies to produce new attributes, inventory, and submission JSON.
Preserve historical references as evidence while the newly generated SIP METS
describes the new layout. Empty source `logs/` needs no packaged file.
Capture unexpected files for review rather than silently discarding them.

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

Propose retaining `logs/<filename>` unchanged under
`<rep>/metadata/other/logs/<filename>` as production history, subject to the
same SIP-profile check as the other proposed paths. A non-empty `logs/`
directory must be included when collecting metadata for staging and checksums.

The `.done` filenames suggest historical workflow markers, but the listing
does not establish that they are empty. Inventory and inspect them before
defining their SIP placement or exclusion. Do not use them as a substitute
for current fixity verification or DPS status. These extra files do not
change discovery: this directory still produces exactly one package FlowFile.

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

## What must change from bevaring

- The metadata organizer currently requires one `*_meta_xml.tar`, removes
  extracted JHOVE files, and expects Scanity/reel metadata. Replace that logic
  for these seven loose XML files and retain JHOVE as fixity evidence.
- The checksum extractor/verifier uses a DPX manifest and batches. Replace
  that with the METS file-to-object binding and staged MOV verification.
  Fail on conflicting evidence or ambiguous file mappings; never invent a
  successful fixity result when the expected checksum is missing.
- The historical event is FFmpeg derivation, not the scanner event extracted
  from bevaring's MIX metadata. Preserve the original event and decide its DPS
  representation independently from this transfer's new events.
- The submission-body builders become reusable once the matching current
  catalog exports exist. Add a readiness check for the required identity and
  title fields; an empty export search must not become a successful empty body.

No media verification, catalog lookup, SIP generation, or DPS submission has
been performed for this proposed mapping. The source XML is enough to develop
and check the metadata parser; real media is needed for the later end-to-end run.

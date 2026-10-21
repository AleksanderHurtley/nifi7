# Fetch produksjon files

Use two ExecuteGroovyScript processors, connected in order:

```text
Initialize
  → 01_Fetch and Organize metadata
  → 02_Copy and Verify MOV
  → Catalog
```

Load each `.groovy` file into its processor's Script Body, or deploy the file
and use Script File. Connect `failure` to review/error handling. Use one
concurrent task per processor for the initial test and keep DB/DPS writers
stopped. The FlowFile body is not read or modified. No database writes,
source changes, conversion, or source archive extraction are performed.

## 1. Fetch and organize metadata

Run this first, with the MOV processor stopped. It reads `package.name`,
`source.dir`, `work.dir`, and the metadata directory attributes set by
[Initialize](../01_Initialize/README.md). Source file locations follow the
documented loose-MOV layout under `source.dir`.

| Source | Staged location |
|---|---|
| `<package.name>.xml` | `metadata.descriptive.depr_mets.dir` |
| The four known MAVIS XML files under `meta/` | `metadata.descriptive.legacy_mavis.dir` |
| `meta/ffprobe.xml` | `metadata.other.dir` |
| `meta/JHOVE_<package.name>.mov.xml` | `metadata.preservation.dir` |
| Files under `logs/`, including subdirectories | `<metadata.other.dir>/logs/` |
| Root-level `*.done` files, including nonempty files | `<metadata.other.dir>/workflow_markers/` |

Files are copied byte-for-byte with their original names. The `.done` files
are retained as historical evidence; they do not establish current transfer
success or fixity. Known metadata copies are refreshed on a rerun.

The script parses the staged METS. Its one `mets:file` must bind through
`ADMID` to one PREMIS object with the expected MOV `originalName`, a valid MD5,
and a positive byte size. Its `FLocat` must identify this package's viewing
URN. Available METS MD5/size attributes, JHOVE MD5/size, and MAVIS carrier
update MD5/size must agree with PREMIS. The source METS and `meta/`
directory are required. Other known reports are retained and checked when
present; the script does not fabricate a missing MD5 or size.

Unknown regular files in the package root or `meta/` are copied into
`metadata.other.unclassified.dir`, retaining their source-relative paths.
Unexpected directories, symlinks, and additional `lossless/` entries are
flagged for review without recursively copying their contents or recalling
unexpected media. Directory inspection is limited to the package root and
the known `meta/`, `lossless/`, and `logs/` directories; only logs are walked
recursively.

Unknown entries set `metadata.org.review.required=true`. The MOV processor
refuses to copy while that flag is true. An optional RouteOnAttribute between
the two processors can direct review cases to a dedicated queue. Existing
unclassified copies continue to require review on a rerun; resolve their
placement before removing any redundant staged review copy. The script never
deletes source files or silently clears review material.

Expected attributes after this processor:

| Attribute | Meaning |
|---|---|
| `metadata.org.status` | `OK` when organization and parsing succeeded; `FAIL` otherwise |
| `metadata.org.count` | Number of metadata/log/marker files copied, including unknown review copies |
| `metadata.org.marker.count` | Number of retained root-level `.done` files |
| `metadata.org.review.required` | `false` when nothing remains unclassified |
| `metadata.org.unclassified.count` | Number of distinct entries requiring review |
| `metadata.org.review.paths` | Review paths, capped at 2048 characters |
| `media.expected.filename` | `<package.name>.mov` |
| `media.expected.size` | Expected bytes from PREMIS |
| `media.expected.md5` | Expected MD5 from PREMIS |
| `media.urn` | `URN:NBN:no-nb_<package.name>` |
| `media.mets.path` | The staged legacy METS used for these values |
| `fetch.start` | Epoch milliseconds, retained when the same FlowFile is retried |

`metadata.org.status=OK` does not mean the MOV is present or verified. Inspect
the expected-media attributes and review flag before continuing.

## 2. Copy and verify the MOV

This processor uses `source.video.path`, `video.output.path`, `rep.data.dir`,
`work.dir`, `package.name`, and the metadata processor's expected-media and
status attributes. It checks the source size, copies to a unique temporary
file under `work.dir`, then reads the staged bytes to verify both size and
MD5. Only a verified copy is moved into `video.output.path`.

If the final staged MOV already exists, the processor verifies its bytes
again and reuses it. A different or corrupt staged file produces failure and
is left for inspection. Failed new copies remove their own temporary files;
an abrupt process termination may leave a `.fetch-video-*.tmp` file under
`work.dir`. Such scratch files are outside `metadata/` and `representations/`.
They are not treated as completed media on retry.

On success, inspect:

```text
fetch.status         = OK
checksum.status      = OK
media.verified.md5   = <expected MD5>
media.verified.size  = <expected bytes>
fetch.video.reused   = false (new copy) or true (existing verified copy)
```

The processor also sets `fetch.end`, `fetch.duration`, `checksum.start`,
`checksum.end`, `checksum.durationMs`, the expected/checked media count of
one, `checksum.source=legacy_mets`, and `transfer.event.datetime` in UTC.
`fetch.duration` includes any manual pause between the metadata and MOV steps.
It does not append an event or generate the final E-ARK checksum inventory;
those happen later, after catalog metadata is present.

On failure, inspect `error.stage` (`fetch.metadata.organize` or `fetch.video`),
`error.message`, and `error.details`. A failed MOV step sets `fetch.status=FAIL`
and `checksum.status=FAIL` or `NOT_CHECKED` according to whether staged-byte
verification had begun.

## Test the current package

The 2023 example's actual XML was used to validate parsing. The current manual
test package, `digifilm_1300107_20130729`, has only a captured directory listing
in this repository; its XML contents, size, and expected digest remain
unverified until this first live metadata run.

1. Run only the metadata processor for the initialized FlowFile.
2. Check its `media.expected.*`, `media.urn`, and review attributes. If it
   fails or flags review, inspect that result before copying the MOV.
3. Run the MOV processor once and check `fetch.status` and `checksum.status`.
4. Keep the output queued for the next walkthrough stage, Catalog.

The MOV copy and subsequent full-file hash may take time for a large source.
The memory buffer is 1 MiB; the movie is not loaded into memory.

## Local verification

From the repository root, with Groovy 4 and its XML module available:

```sh
groovy film_produksjon/tests/fetch.groovy
```

The tests parse the captured XML and use small synthetic media with matching
fixture checksums for copy/verification tests. They cover raw metadata bytes,
logs and nonempty markers, source-report conflicts, broken METS bindings,
missing media, unknown-file review, existing-file corruption, repeated runs,
and failed-copy cleanup. They do not access SAM-FS or a database.

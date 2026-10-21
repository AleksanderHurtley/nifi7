# Film → produksjon: simple transfer flow

Status: step-by-step implementation in progress. Package discovery,
initialization, and the two fetch scripts are available. The existing
produksjon DB pickup has selected a test package on production line 82, and
initialization has succeeded on nifi7. Fetch has passed local tests and is
ready for a live manual run. Catalog, E-ARK, and DPS adaptation are still pending.

Transfer the existing viewing/production media byte-for-byte from SAM-FS into
an E-ARK SIP and DPS. Reuse the existing NiFi groups and conventions, making
small adaptations for produksjon paths, source metadata, and catalog identity.
Keep the implementation local to this flow. Shared-code refactoring and new
reporting infrastructure are deferred until there is a concrete need.

The first target is the
[captured MOV package](../examples/sam-fs/digifilm_796646_20230321.notes.md).
Its audio is already inside the MOV. DPX extraction, batching, RAWcooked,
compression statistics, and scanner-event extraction are outside this flow.
Review other source layouts after the first package completes successfully.

The [bevaring implementation](../../film_bevaring/README.md) and supplied NiFi
screenshots are references for reuse. Live DB queries, processor properties,
controller services, and DPS configuration still need to be captured as we
reach each step.

## Main path

```mermaid
flowchart TD
    A[1. Pick up package and initialize] --> B[2. Copy media and metadata, then verify]
    B --> C[3. Fetch catalog metadata and prepare submission]
    C --> D[4. Complete checksums and generate E-ARK SIP]
    D --> E[5. Submit to DPS, record completion, clean staging]
```

These are five walkthrough stages; existing NiFi process groups can keep
their current boundaries. Checksum verification after copying and package
checksum generation before E-ARK remain separate responsibilities within
this path.

## Step-by-step walkthrough

Work through one stage at a time using the existing processors. Record the
necessary changes and confirm each stage's output before moving to the next.
The first goal is one package reaching confirmed DPS completion.

### 1. Pick up the package and initialize

Start with **Find new package in DB** and its input preparation. The existing
[package-discovery helper](../00_Package%20discovery/README.md) emits one
FlowFile per `produksjon/<year-month>/<package>` directory, carrying
`package.name` and `package.path`. Database insertion and subsequent pickup
are separate steps.

The current produksjon pickup uses:

```sql
SELECT `ID`, `DESCRIPTION`
FROM `DIGITIZED_ITEM`
WHERE `STATUS` = 'Transfer.ready'
  AND `Pline_id` = 82
LIMIT 1;
```

The ID becomes `prod_db_id` and the description becomes `prod_db_description`.
The path lookup then uses:

```sql
SELECT `PARAMETER_VALUE`
FROM `DI_PARAMETER`
WHERE `DI_ID` = ${prod_db_id}
  AND `PARAMETER_NAME` LIKE 'package%path'
LIMIT 1;
```

These are the existing produksjon PG's queries. Selection is read-only; the
PG's STARTED update comes later. Trigger overlap and empty-result handling
still need review before unattended pickup. The input-row preparation queries
have not been captured.

For testing, a manual GenerateFlowFile bypasses the STARTED update and passes
`package.name=digifilm_1300107_20130729`, the corresponding source path,
`prod_db_id=49062190`, `prod_db_description`, and `package.uuid` directly to
Initialize. It can have an empty body. Keep DB status writers and DPS submission
stopped while testing initialization.

Run the two [produksjon initialization scripts](../01_Initialize/README.md)
in order. The default local roots are:

| Attribute | Value |
|---|---|
| `work.base.dir` | `/fc1/work/film_produksjon` |
| `payloads.base.dir` | `/fc1/payloads/film_produksjon` |
| `transfer.base.dir` | `/fc1/transfer/film_produksjon` |
| `source.dir` | Validated `package.path` under `/global/film/00/film/produksjon/` |
| `rep.dir` | `<work.dir>/representations/primary_20130729` for the manual test |
| `rep.data.dir` | `<rep.dir>/data` |

The initializer appends the validated package name under each local root and
uses its terminal calendar date for a stable representation name. This date
is a naming token, not evidence of the historical derivation date. Incoming
base-directory attributes can override the defaults. The MOV source and
destination paths use `<package.name>.mov`.

Directory initialization creates the local staging/metadata directories and
missing payload files, retaining existing event and submission content on
reruns. It prepares `transfer.dir` as an attribute for E-ARK to create later.
Neither script reads the FlowFile body or writes to the database.

**Check before continuing:** the manual test retains its package/DB identity,
has the expected source and local paths, and reports `setup.status=OK` after
directory initialization. Source file existence and checksums are checked
in the next stage.

### 2. Copy media and source metadata, then verify

Run the two [Fetch scripts](../02_Fetch%20files/README.md) in order:

1. Fetch and organize the source XML, logs, and workflow markers. Bind the
   METS file entry through `ADMID` to its PREMIS object and extract expected
   MOV identity, size, and MD5. Check agreement with available METS attributes,
   JHOVE, and MAVIS carrier update evidence. Retain `.done` files under
   representation other metadata as historical workflow markers. Unknown
   entries require review before media copying.
2. Copy `lossless/<package.name>.mov` unchanged into a temporary local file,
   read the staged bytes to verify size and MD5, then publish the verified MOV
   under `rep.data.dir`. A previously staged MOV is verified again before
   reuse. A mismatching existing file is retained and the run fails.

The first processor uses the [metadata mapping](METADATA_MAPPING.md) and can
be tested without copying the large MOV. The current 2013 package's XML has
not yet been captured, so start with that metadata-only live run. Local tests
use the captured 2023 XML and small synthetic media; no real MOV transfer has
been performed by this implementation yet.

Missing, ambiguous, conflicting, or mismatching required evidence routes to
failure. Unknown metadata is retained in unclassified review; unexpected
media is flagged without being copied. Neither script changes source files
or writes to the DB. This step performs source fixity verification, so a
separate DPX-style Checksum PG is unnecessary for this path.

**Check before continuing:** the media and source metadata are staged in their
assigned locations, no unclassified entries remain, and `fetch.status=OK`
and `checksum.status=OK` confirm the staged MOV matches its expected size
and MD5. Queue the result for Catalog while that stage is adapted.

### 3. Fetch catalog metadata and prepare the submission

Reuse the catalog retrieval and
[submission-body components](../../film_bevaring/07_Submission%20body/),
adapting the lookup to the viewing-file URN. Confirm that the returned current
catalog records belong to this media file and contain the required identity
and title fields. The legacy MAVIS snapshot remains source metadata alongside
the current catalog exports.

Use the package name as the candidate DPS `objectId`, subject to the selected
contract's rules. The related preservation/core-file URNs document provenance;
the viewing-file URN supplies this package's catalog identity.

**Check before continuing:** current catalog XML is staged and a populated
submission body identifies the correct viewing/production object.

### 4. Complete checksums and generate the E-ARK SIP

Reuse the [checksum generator](../../film_bevaring/06_Generate%20checksums/)
and live `EarkSIPGenerator` configuration with produksjon paths. Generate the
final package checksum manifest after the source and catalog metadata are
in place. E-ARK produces the new SIP structure and METS around the existing
media and prepared metadata.

Bevaring writes `checksums.md5` as `<md5> *<relative-path>` lines and exposes
`checksums.md5.path`, covering `metadata/` and `representations/`. Confirm the
live processor's checksum format, path base, algorithm, and metadata mapping
when wiring this stage. The file placements in the metadata mapping remain
proposed until that check is complete.

Start with the existing generator even if it reads the MOV again. Reusing the
earlier verified digest is a later optimization that requires tracking whether
the staged file has changed.

**Check before continuing:** the generated SIP includes the unchanged MOV and
all intended metadata, with checksums and file references matching its layout.

### 5. Submit to DPS, record completion, and clean staging

Reuse the existing DPS-2 submission, upload, response handling, and retry
groups with produksjon configuration. Retain the submission ID so retries can
resume the same submission. Preserve the distinction between submission
acceptance, completed uploads, and confirmed DPS preservation completion.
The final DB status and cleanup trigger follow the confirmed completion
condition established by the live response-handling configuration.

Adapt [finalization](../../film_bevaring/09_Finalize%20stats/) to the small set
of transfer fields below, and configure
[cleanup](../../film_bevaring/10_Package%20cleanup/) for this flow's local
roots. SAM-FS source deletion is a separate operation.

**Check before completing the first run:** DPS confirms completion, the DB
records the submission and outcome, and eligible local staging is cleaned.

## Events and basic operations

Reuse the [event appender](../../film_bevaring/Add%20event.groovy) for
`transfer` and `information package creation`. Adapt transfer text to MOV
checksum verification and use the actual deployed agent versions. Complete
event writes before the DPS stage uploads them. Preserve the historical
`derivation` event in the legacy METS; mapping that history into separate DPS
events can be decided later. This flow performs no new media conversion.

Keep the existing DB model (`DIGITIZED_ITEM`, `DI_EVENT`, `DI_PARAMETER`) and
NiFi queue/error monitoring. Record package identity, source path, state,
fixity result, DPS submission ID/status, overall start/end times, and final
package bytes. Detailed stage timings, compression metrics, and a new Grafana
dashboard are outside the first implementation.

Use the existing error conventions (`prod_db_id`, package identity,
`error.stage`, and `error.message`) and preserve staged files needed for retry.
Use queue backpressure and concurrency appropriate to local capacity; the
sample MOV alone is about 206 GiB. A bounded review queue is sufficient to
start; add more elaborate buffering only if operation shows a need.

# Create produksjon events after E-ARK

Run [01_Create events payload.groovy](01_Create%20events%20payload.groovy) in
ExecuteGroovyScript after successful EarkSIPGenerator execution. It creates
the complete `events.ndjson` in the existing payload directory, outside the
finished SIP. It does not alter the FlowFile body, source metadata, MOV, SIP,
or submission JSON, and makes no API or database calls. Bevaring is unchanged.
It reads the retained legacy METS and MOV from `transfer.dir`, so it also works
after work staging has been deleted.

## Wiring and configuration

1. Connect EarkSIPGenerator's **success** to an UpdateAttribute processor.
2. Capture these attributes there:

   | Attribute | Value |
   |---|---|
   | `eark.end` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}` |
   | `eark.status` | `OK` |
   | `nifi.agent.version` | `2.2.0` if this is the deployed NiFi version |
   | `eark.agent.version` | `1.0.10` if this is still the deployed processor version |

3. Connect UpdateAttribute success to [E-ARK - Cleanup](../05_Cleanup%20work/README.md),
   then cleanup success to the new ExecuteGroovyScript event processor.
   Load the script via Script Body or Script File, not both. Use one concurrent
   task and avoid running the same package in parallel.
4. Route script `failure` to Failure and hold `success` for inspection during
   the first 2013 test. The external event file remains available for the existing event upload
   path at the appropriate submission stage.

Use actual deployed versions, not the versions from bevaring's example JSON.
`eark.end` records when the successful SIP becomes available, with a small
processor-scheduling delay. Capture it before any deliberately held queue.
The script accepts ISO-8601 with timezone (the expression above, e.g.
`2026-10-23T11:27:15.132+02:00`) or epoch milliseconds (`${now():toNumber()}`).
Existing timestamp configuration does not need changing. Offset values are
converted to the same instant in UTC; the event output uses seconds precision,
as in bevaring. A timestamp without a timezone is rejected rather than guessed.
The builder never substitutes its own current time. On an event-build retry,
rerun the builder with the saved attributes; do not reset `eark.end` unless
the SIP was actually regenerated.
The builder can run before or after work cleanup; the recommended order now
cleans work first. Event-build retries need only the retained SIP, external
payload file, and saved attributes, not restaging.

The script consumes the existing package/payload/transfer paths,
`media.urn`, `media.expected.*`,
`media.verified.*`, `fetch.status`, `checksum.status`, the expected/checked
checksum counts, `metadata.org.status`, `metadata.org.review.required`, and
`transfer.event.datetime`. No initialization or fetch script changes are needed.
The source METS is resolved as
`<transfer.dir>/metadata/descriptive/deprecated_mets/<package.name>.xml`, not
the generated root `METS.xml`. The MOV is resolved under the SIP's existing
`representations/primary_<package-date>/data/` path. Old `media.mets.path` and
`video.output.path` attributes may still point into work; they are ignored,
as are `work.dir` and `work.base.dir`. There is no fallback to old work files.

## Three records

| Type | Evidence and timestamp | Agent and scope |
|---|---|---|
| `creation` | Historical PREMIS creatingApplication date, corroborated by a linked successful creation/derivation event | FFmpeg; `fileRef.relativePath` identifies the MOV in the new SIP |
| `transfer` | Existing `transfer.event.datetime`, captured after MOV verification | Apache NiFi; package-level, including verification of the MOV only |
| `information package creation` | `eark.end`, captured on generator success | nifi-nb-eark-nar.EarkSIPGenerator; package-level |

For the [captured 2023 METS](../examples/sam-fs/digifilm_796646_20230321/digifilm_796646_20230321.xml),
creation is `2023-05-04T07:02:30Z`, the same instant as its recorded
`2023-05-04T09:02:30+02:00`. The original `derivation` is represented as a
reconstructed historical `creation` of the existing MOV, not a claim of a
new conversion in this flow. Its source type is retained in eventDetail.
The retained original XML remains unchanged in the SIP's `deprecated_mets/`.

Following the stage review, eventDetail begins `Produksjonsfilen <filename>
ble opprettet med FFmpeg.` rather than `Historisk opprettelse`. It explicitly
states `Denne hendelsen er dokumentert i ettertid på grunnlag av de opprinnelige
METS-metadataene.` This refers to documenting the event afterwards, not
reconstructing the MOV from METS. The original source type is written in Norwegian
as `Opprinnelig hendelsestype: derivat` or `Opprinnelig hendelsestype: opprettelse`, without a parenthetical English
label or trailing full stop. The original label remains in the retained METS. The
controlled API `eventType` remains `creation`, not a translated value.
The creation event omits `outcomeDetail`, retaining `outcome=success` and fileRef.

The recorded `Lavf58.62.100` becomes agentVersion
`libavformat 58.62.100 (FFmpeg versjon ikke dokumentert)`. For the 2013 example
the value is `libavformat 54.63.104 (FFmpeg versjon ikke dokumentert)`.
This identifies the library version without claiming the complete FFmpeg
release/build is known; see the
[FFmpeg libavformat identifier](https://www.ffmpeg.org/doxygen/trunk/libavformat_2version_8h.html).
The version is no longer repeated in eventDetail. A non-Lavf creating
application version is used as recorded; an absent version is omitted. An
unrecognized Lavf identifier is kept verbatim with the same unknown-FFmpeg
qualification instead of guessing a numeric version. Agent notes remain stable.
No FFmpeg command, source format, or lossless result is inferred.

The MOV is bound to one PREMIS object via the source METS `ADMID`, then to one
historical event through both event and object identifiers. Creation dates
must include a timezone and agree between creatingApplication and event.
Creation, transfer, and SIP timestamps must be chronologically consistent.
Missing, unsuccessful, ambiguous, or unfamiliar evidence fails the builder;
it does not fabricate a third event or use the package date, METS header date,
file mtime, or log filename. The 2013 package's original XML/log contents are
not in the repo, but its [live event output](../examples/events/README.md) is
now recorded from the user's successful test after work cleanup. The log is retained as evidence
but this script does not parse logs or use them as a timestamp fallback.

## Safety and retry behavior

- Require successful metadata organization, MOV verification, and SIP creation.
  Check that the retained METS still agrees with the verified MD5/size, and that
  the packaged MOV exists with that size. This is a gate on
  prior verification, not a new hash or SIP validation. It cannot detect a
  later same-size content change; keep verified media immutable.
- Require `<transfer.dir>/METS.xml` and the expected MOV path in that folder.
  If the generator changes the representation path, fail rather than emit a
  file reference to the wrong file.
- Validate and serialize all records before atomically replacing the initialized
  `events.ndjson`. A rerun with unchanged inputs yields the same three records,
  not six. Clean up only the temporary file created by this invocation.
- Preserve existing output on validation failure. Refuse to overwrite malformed
  NDJSON, another package's records, unexpected event types, or duplicate types.
  This builder owns only these three event types; don't also run Add event.groovy
  on the same file or repeatedly route it through an append loop.
- Local replacement does not deduplicate API uploads. Do not blindly repost
  events already accepted by DPS when resuming a failed submission.

Success sets `events.payload.status=OK`, `events.payload.count=3`,
`creation.event.datetime`, `sip.event.datetime`, and `events.source.mets.path`
(the retained source XML path in the SIP). Failure sets
`events.payload.status=FAIL`, `events.payload.error`, `error.stage=events.payload.build`,
`error.message`, and `error.details`. Always gate upload on the success relationship.

## Upload shape

Each local line has `packageId`, `agent`, and flat `eventDateTime`, `eventType`,
`eventDetail`, `outcome`, and optional `outcomeDetail`/`fileRef`. Notes use
singular `agentNote`. Reuse the existing `FetchFile → SplitText → Jolt → InvokeHTTP`
path, removing only the local `packageId` before posting one JSON record.
Preserve `fileRef` on the creation record. Do not wrap the event fields or
rename agentNote during upload.

Files are written as UTF-8 with literal Norwegian letters (`æ`, `ø`, `å`),
using JsonGenerator's `disableUnicodeEscaping()` option. Earlier output used
valid JSON Unicode escapes such as `\u00f8` for `ø`; that was a presentation
difference, not lost characters. Quotes, backslashes, and control characters
remain JSON-escaped so NDJSON still has exactly one event per line. Rerun only
the event builder with its saved timestamps to rewrite an existing local payload;
do not repost previously accepted API events just for this readability change.

The preservation [guidance page](https://digitalpreservation.no/nb/docs/dps/api/submission/events/)
describes the event semantics but currently shows a different JSON example.
The [published API schema](https://api.nb.no/dps-submission/v1/api-docs), checked
2026-10-23 (`SubmitEventDto`, `SubmitAgentDto`, `SubmitFileRefDto`), specifies
the flat shape and singular agentNote used here and by bevaring. Verify the
contract endpoint's schema before live upload if it uses a different deployment.
No event POST has been performed by this implementation.

## Local test

From the repository root with Groovy 4, groovy-xml, and groovy-json:

```sh
groovy film_produksjon/tests/events.groovy
```

Tests use the captured 2023 XML with small synthetic MOV bytes and a minimal
SIP existence fixture. They execute the real cleanup script before event
creation to cover deleted work and stale work-path attributes. They also
cover absent work attributes, missing retained METS (without fallback),
reviewed creation wording/no outcomeDetail, qualified Lavf versions,
epoch/ISO timestamps with timezone offsets, readable UTF-8 Norwegian letters
and safe JSON control-character escaping, exact dates/agents/fileRef, linkage failures,
status/size/checksum gates, missing SIP output, immutable input XML, retry-safe
replacement, and preservation of previous payloads on failure. They do not
claim E-ARK schema validation, actual large-media verification, or API upload.

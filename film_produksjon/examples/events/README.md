# Produksjon event output examples

These are generated outputs, separate from the original source examples under
[sam-fs/](../sam-fs/README.md). Keep one NDJSON file per package.

## digifilm_1300107_20130729

[digifilm_1300107_20130729.events.ndjson](digifilm_1300107_20130729.events.ndjson)
records the user's pasted output from nifi7, supplied on 2026-10-23:

```text
/fc1/film_produksjon/payloads/digifilm_1300107_20130729/events.ndjson
```

The three lines are retained as supplied, including Unicode escapes (valid
JSON, decoded to the original letters by JSON readers). The current builder
writes literal UTF-8 letters for readability; this earlier capture is unchanged:

- Historical FFmpeg creation: `2013-08-12T11:14:34Z`; source library identifier
  `Lavf54.63.104`, not claimed as an FFmpeg release number.
- SAM-FS transfer including MOV verification: `2026-10-21T10:47:01Z`;
  MD5 `3a878d5b259824621bd99d5d8b82aa3a`, size `19907607344` bytes.
- SIP creation: `2026-10-23T09:27:15Z`; EarkSIPGenerator `1.0.10`.

This records a live 2013 event-build test after work cleanup, not proof that
the events have been accepted by the DPS API or preservation completed.
`packageId` is local bookkeeping; remove it before posting each event as
described in [Events](../../04_Events/README.md).

## Creation-event review, updated 2026-11-02

[digifilm_1300107_20130729.reviewed.events.ndjson](digifilm_1300107_20130729.reviewed.events.ndjson)
is an expected-output example derived from that capture, not another live run
or accepted API payload. It applies the agreed reviewer/user changes:

- Describe the final FFmpeg conversion after digital post-production, including
  `blant annet` and `faste trinn` for color correction, cropping, and sound
  synchronization. These steps are user-confirmed workflow context; other work
  may also have occurred.
- State that the event was registered afterwards using original METS and
  knowledge of the production workflow. It is the event documentation that
  was created afterwards, not the MOV.
- Explain `derivation` as `METS-metadataene beskriver opprettelsen av et derivat.`
  The original label remains in METS. Keep API eventType `creation`.
- Use `Digital filmlab Mo i Rana, Nasjonalbiblioteket` as the agreed responsible
  organization, with no agentVersion. This is agreed flow context, while the
  source METS identifies the FFmpeg software used.
- Preserve version evidence in eventDetail as `Registrert bibliotekversjon:
  libavformat 54.63.104. Versjonen av FFmpeg fremgår ikke av METS-metadataene.`
  Use separate sentences without parentheses; omit creation outcomeDetail.

Other event contents and all timestamps are retained for comparison. When the
SIP is actually rebuilt to fix representation metadata paths, capture its new
completion time for the new SIP-creation event. This example is not a reason
to reuse the old completion time or blindly repost events to the rejected submission.
The readable [`.events.json` example](digifilm_1300107_20130729.events.json)
contains the three updated events as one JSON document for sharing and discussion.
It has no appended submission response. Its package data and timestamps remain
those of the original example; it is a review draft, not a new live-run capture.

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

## Creation-event review, 2026-10-27

[digifilm_1300107_20130729.reviewed.events.ndjson](digifilm_1300107_20130729.reviewed.events.ndjson)
is an expected-output example derived from that capture, not another live run
or accepted API payload. It applies the agreed reviewer/user changes:

- Start with `Produksjonsfilen ... ble opprettet med FFmpeg`, without Historisk.
- Clarify `Denne hendelsen er dokumentert i ettertid på grunnlag av de opprinnelige METS-metadataene.`
  It is the event documentation that was created afterwards, not the MOV.
- Use `Opprinnelig hendelsestype: derivat` without parentheses or a trailing
  full stop. The original label remains in METS. Keep API eventType `creation`.
- Move the version to `libavformat 54.63.104 (FFmpeg versjon ikke dokumentert)`
  in agentVersion; omit creation outcomeDetail and the duplicate version sentence.

Other event contents and all timestamps are retained for comparison. When the
SIP is actually rebuilt to fix representation metadata paths, capture its new
completion time for the new SIP-creation event. This example is not a reason
to reuse the old completion time or blindly repost events to the rejected submission.
The readable `.events.json` example also reflects the reviewed creation event;
the separately appended submission response is retained unchanged.

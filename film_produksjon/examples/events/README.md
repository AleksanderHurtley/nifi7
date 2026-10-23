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

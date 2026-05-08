# Preservation events (NDJSON)

Events are appended as NDJSON records:
- one JSON object per line
- fields: `packageId`, `agent`, `event`

Reference: <https://digitalpreservation.no/nb/docs/dps/api/submission/events/>

## General guidance
A good event should be understandable by someone who did not implement the system.
Prefer clarity over internal jargon. Per DPS guidance, events should be written in
Norwegian where possible — they are read by future Norwegian preservation staff.

Each event should answer:
- What happened? (eventDetail — operation + method/parameters)
- Who/what performed it? (agent)
- What was the outcome? (outcome + optional outcomeDetail with the achieved result)

`eventDetail` describes *what was done* (including parameters/flags used).
`outcomeDetail` describes *what was achieved*. Not every event needs `outcomeDetail`
— `outcome=success|failure|warning` is often enough on its own.

## Agent policy
Default agent fields (unless overridden):
- agentName: `Apache NiFi`
- agentType: `software`
- agentVersion: `2.2.0`
- agentNotes: `Programvare for automatisering av dataflyter som muliggjør utforming og administrasjon av komplekse datapipelines.`

`Add event.groovy` reads the optional flowfile attribute `agent.notes` and
omits the field when blank.

Exceptions:
- **RAWcooked migration**: agent is RAWcooked (include version + agentNotes).
- **Creation (digitization)**: agent is the scanner hardware (e.g. Scanity), parsed
  from the deprecated METS files; agentType is `hardware`.

## Canonical event texts

### Transfer (SAM-FS → staging, fixity, E-ARK SIP)
eventType: `transfer`
agent: default (Apache NiFi)
eventDetail:
- “Overført pakke fra Oracle HSM (SAM-FS) til lokalt arbeidsområde for videre behandling; DPX-sjekksummer verifisert mot SAM-FS-metadata med md5sum (GNU coreutils); Opprettet E-ARK SIP med commons-ip2.”

### Creation (digitization, parsed from deprecated METS)
eventType: `creation`
agent override:
- agentName: scanner model (e.g. `Scanity`), parsed from `mix:scannerModelName`
- agentType: `hardware`
- agentVersion: parsed from `mix:scannerModelName` (e.g. `V3.2.3`)
- agentNotes: `Filmskanner produsert av <scannerManufacturer>. Brukes til digitalisering av analog film til DPX-bildesekvenser.`
eventDateTime: earliest `mix:GeneralCaptureInformation/mix:dateTimeCreated` across all reels
eventDetail (template):
- “Digitalisering av analog film til DPX-bildesekvenser ved bruk av filmskanner \<scannerModelName> (\<scannerManufacturer>, serienr. \<scannerModelSerialNo>). imageProducer: \<imageProducer>; captureDevice: \<captureDevice>.”

Source XML elements (in `metadata/other/deprecated_mets/METS_*_NNNN.xml`):
- `mix:GeneralCaptureInformation` (`dateTimeCreated`, `imageProducer`, `captureDevice`)
- `mix:ScannerCapture` (`scannerManufacturer`, `ScannerModel/scannerModelName`, `ScannerModel/scannerModelSerialNo`)

Notes:
- Only final-reel METS (`METS_*_NNNN.xml`, e.g. `_0001`, `_0002`, …) are
  considered. The pre-production `METS_*_pre.xml` is excluded — its
  `dateTimeCreated` reflects a pre-conformance pass, not the final digitization.
- `mix:SourceXDimension` (film gauge) is intentionally not used — the
  recorded value is unreliable in source METS (e.g. recorded as 16 mm when
  the original is actually 32 mm).

This event is best-effort: when no METS files contain the required mix data
(e.g. born-digital packages), no creation event is emitted.

### RAWcooked migration (DPX → FFV1/MKV)
eventType: `migration`
agent override:
- agentName: `RAWcooked`
- agentType: `software`
- agentVersion: e.g. `24.11`
- agentNotes: `Verktøy for tapsfri konvertering av DPX-bildesekvenser til FFV1-video i Matroska-container, med bit-eksakt rekonstruksjon.`
eventDetail (parameters belong here, per DPS guidance):
- “Konvertering av DPX-bildesekvenser til FFV1-video i Matroska (MKV) container for langtidsbevaring. Brukte parametere: command=rawcooked --all --check -y; tool.ffmpeg=\<version>; tool.ffprobe=\<version>.”
outcomeDetail (result-only keys):
- `container`
- `videoCodec`
- `videoProfile`

## Example RAWcooked event
{
  "packageId": "<packageId>",
  "agent": {
    "agentName": "RAWcooked",
    "agentType": "software",
    "agentVersion": "24.11",
    "agentNotes": "Verktøy for tapsfri konvertering av DPX-bildesekvenser til FFV1-video i Matroska-container, med bit-eksakt rekonstruksjon."
  },
  "event": {
    "eventDateTime": "2026-02-11T14:01:41Z",
    "eventType": "migration",
    "eventDetail": "Konvertering av DPX-bildesekvenser til FFV1-video i Matroska (MKV) container for langtidsbevaring. Brukte parametere: command=rawcooked --all --check -y; tool.ffmpeg=ffmpeg version N-113331-...; tool.ffprobe=ffprobe version N-113331-....",
    "outcome": "success",
    "outcomeDetail": "container=Matroska / WebM;videoCodec=ffv1;videoProfile=FFV1 version 3"
  }
}

## Example creation event
{
  "packageId": "digifilm_22433263_20230908_FYAL00000247",
  "agent": {
    "agentName": "Scanity",
    "agentType": "hardware",
    "agentVersion": "V3.2.3",
    "agentNotes": "Filmskanner produsert av Digital Film Technology GmbH. Brukes til digitalisering av analog film til DPX-bildesekvenser."
  },
  "event": {
    "eventDateTime": "2023-09-21T14:28:44+02:00",
    "eventType": "creation",
    "eventDetail": "Digitalisering av analog film til DPX-bildesekvenser ved bruk av filmskanner Scanity V3.2.3 (Digital Film Technology GmbH, serienr. 124). imageProducer: Nasjonalbiblioteket; captureDevice: still from video.",
    "outcome": "success"
  }
}

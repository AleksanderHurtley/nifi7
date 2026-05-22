# Preservation events (NDJSON)

Events are appended as NDJSON records, one JSON object per line, with these top-level fields:
- `packageId` — local-only; identifies which package the event belongs to
- `agent` — nested object (`agentName`, `agentType`, `agentVersion`, optional `agentNote`)
- `eventDateTime`, `eventType`, optional `eventDetail`, `outcome`, optional `outcomeDetail`

The flat shape matches the DPS events API body when `packageId` is removed.
Upload pipeline: `FetchFile` → `SplitText` (one event per FlowFile) →
`JoltTransformJSON` with spec `[{"operation":"remove","spec":{"packageId":""}}]`
→ `InvokeHTTP` POST to
`#{submission_service_base_url}/v1/contracts/${contract.id}/submissions/${submission.id}/events`.

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
- agentNote: `Programvare for automatisering av dataflyter som muliggjør utforming og administrasjon av komplekse datapipelines.`

`Add event.groovy` reads the optional flowfile attribute `agent.note` and
omits the field when blank.

Exceptions:
- **RAWcooked migration**: agent is RAWcooked (include version + agentNote).
- **Creation (digitization)**: agent is the scanner hardware (e.g. Scanity), parsed
  from the deprecated METS files; agentType is `hardware`.

## Canonical event texts

### Transfer (SAM-FS → staging, fixity)
eventType: `transfer`
agent: `Apache NiFi` — set explicitly on the UpdateAttribute (values match the
defaults in `Add event.groovy`; setting them explicitly is belt-and-suspenders
against agent-attribute leakage from upstream processors like RAWcooked).
eventDetail:
- “Overført pakke fra Oracle HSM (SAM-FS) til lokalt arbeidsområde for videre behandling; DPX-sjekksummer verifisert mot SAM-FS-metadata med md5sum (GNU coreutils).”

Wiring: see [examples/nifi-updateattribute-transfer.json](../examples/nifi-updateattribute-transfer.json)
for the full property list of the NiFi `UpdateAttribute` processor that
emits this event.

### Information package creation (E-ARK SIP)
eventType: `information package creation`
agent override:
- agentName: `Commons IP`
- agentType: `software`
- agentVersion: e.g. `2.3.0` (the bundled `commons-ip2` library version inside the `nifi-nb-eark-nar` NAR — verify against the NAR currently deployed)
- agentNote: `Verktøy brukt for å opprette E-ARK SIP-pakker`
eventDetail:
- “Opprettelse av E-ARK SIP i henhold til E-ARK Common Specification (CSIP) V.2.2.0, E-ARK SIP V.2.2.0 og Nasjonalbibliotekets spesifikasjoner SIP 1.0 (E-ARK).”

Wiring: a NiFi `UpdateAttribute` processor placed right after `EarkSIPGenerator`,
routed to `Add event.groovy`. Because `agent.name` is set here, all four
`agent.*` properties must be set on the same UpdateAttribute (the defaults in
the appender only apply when `agent.name` is unset). See
[examples/nifi-updateattribute-information-package-creation.json](../examples/nifi-updateattribute-information-package-creation.json)
for the full property list.

### Creation (digitization, parsed from deprecated METS)
eventType: `creation`
agent override:
- agentName: scanner model (e.g. `Scanity`), parsed from `mix:scannerModelName`
- agentType: `hardware`
- agentVersion: firmware version + physical-unit serial, e.g. `V3.2.3 (serienr. 124)`
  (combines `mix:scannerModelName` revision with `mix:scannerModelSerialNo` so the
  agent block uniquely identifies the specific physical scanner)
- agentNote: `Filmskanner produsert av <scannerManufacturer>. Brukes til digitalisering av analog film til DPX-bildesekvenser.`
eventDateTime: earliest `mix:GeneralCaptureInformation/mix:dateTimeCreated` across all reels
eventDetail (template — scanner identity lives entirely in `agent`):
- “Digitalisering av analog film til DPX-bildesekvenser. Produsent: \<imageProducer>.”
outcomeDetail (template):
- “Resulterte i \<total> DPX-filer.” (sum of `er:elementCount` across all reels)

Source XML elements (in `metadata.descriptive.depr_mets.dir`, usually `metadata/descriptive/deprecated_mets/METS_*_NNNN.xml`):
- `mix:GeneralCaptureInformation` (`dateTimeCreated`, `imageProducer`)
- `mix:ScannerCapture` (`scannerManufacturer`, `ScannerModel/scannerModelName`, `ScannerModel/scannerModelSerialNo`)
- `er:elementRange/er:elementCount` (per-reel DPX count; summed for the package)

Notes:
- Only final-reel METS (`METS_*_NNNN.xml`, e.g. `_0001`, `_0002`, …) are
  considered. The pre-production `METS_*_pre.xml` is excluded — its
  `dateTimeCreated` reflects a pre-conformance pass, not the final digitization.
- `mix:SourceXDimension` (film gauge) is intentionally not used — the
  recorded value is unreliable in source METS (e.g. recorded as 16 mm when
  the original is actually 32 mm).
- `mix:captureDevice` is intentionally not used — its MIX value
  ("still from video") is not meaningful to Norwegian readers and duplicates
  what the scanner agent already implies.

This event is best-effort: when no METS files contain the required mix data
(e.g. born-digital packages), no creation event is emitted.

### RAWcooked migration (DPX → FFV1/MKV)
eventType: `migration`
agent override:
- agentName: `RAWcooked`
- agentType: `software`
- agentVersion: e.g. `24.11`
- agentNote: `Verktøy for tapsfri konvertering av DPX-bildesekvenser til FFV1-video i Matroska-container, med bit-eksakt rekonstruksjon.`
eventDetail (parameters belong here, per DPS guidance):
- “Konvertering av alle DPX-bildesekvenser i pakken til FFV1-video i Matroska (MKV) container for langtidsbevaring. Kommando kjørt: rawcooked --all --check -y. Verktøy: ffmpeg=\<token>; ffprobe=\<token>.”
  Tool versions are trimmed to the build identifier (e.g. `N-113331-g202a35ecdb`), not the full first line of `--version`.
outcomeDetail (result-only keys):
- `container`
- `videoCodec`
- `videoProfile`
- `mkvCount` (number of MKV outputs produced)

## Example RAWcooked event
{
  "packageId": "<packageId>",
  "agent": {
    "agentName": "RAWcooked",
    "agentType": "software",
    "agentVersion": "24.11",
    "agentNote": "Verktøy for tapsfri konvertering av DPX-bildesekvenser til FFV1-video i Matroska-container, med bit-eksakt rekonstruksjon."
  },
  "eventDateTime": "2026-02-11T14:01:41Z",
  "eventType": "migration",
  "eventDetail": "Konvertering av alle DPX-bildesekvenser i pakken til FFV1-video i Matroska (MKV) container for langtidsbevaring. Kommando kjørt: rawcooked --all --check -y. Verktøy: ffmpeg=N-113331-g202a35ecdb; ffprobe=N-113331-g202a35ecdb.",
  "outcome": "success",
  "outcomeDetail": "container=Matroska / WebM;videoCodec=ffv1;videoProfile=FFV1 version 3;mkvCount=3"
}

## Example creation event
{
  "packageId": "digifilm_22433263_20230908_FYAL00000247",
  "agent": {
    "agentName": "Scanity",
    "agentType": "hardware",
    "agentVersion": "V3.2.3 (serienr. 124)",
    "agentNote": "Filmskanner produsert av Digital Film Technology GmbH. Brukes til digitalisering av analog film til DPX-bildesekvenser."
  },
  "eventDateTime": "2023-09-21T14:28:44+02:00",
  "eventType": "creation",
  "eventDetail": "Digitalisering av analog film til DPX-bildesekvenser. Produsent: Nasjonalbiblioteket.",
  "outcome": "success",
  "outcomeDetail": "Resulterte i 1423 DPX-filer."
}

# Preservation events (NDJSON)

Events are appended as NDJSON records:
- one JSON object per line
- fields: `packageId`, `agent`, `event`

## General guidance
A good event should be understandable by someone who did not implement the system.
Prefer clarity over internal jargon.

Each event should answer:
- What happened?
- Why did it happen?
- What changed?
- Who/what performed it?
- What was the outcome?

## Agent policy
Default agent fields (unless overridden):
- agentName: `NiFi preservation ingest flow`
- agentType: `software`
- agentVersion: optional (omit unless explicitly set)

Exception:
- RAWcooked migration: agent is RAWcooked (include version)

## Canonical event texts

### Transfer (from SAM-FS to staging)
eventType: `transfer`
eventDetail:
- “Package transferred from SAM-FS archival storage to local staging storage for preservation processing.”

### Fixity check (DPX/MD5)
eventType: `fixity check`
eventDetail:
- “Fixity check after transfer: computed MD5 checksums for DPX files in the staging area and compared them to MD5 values recorded in SAM-FS archival storage metadata to confirm bit-level integrity.”
outcomeDetail:
- optional; include counts/mismatches if available

### RAWcooked migration (DPX -> FFV1/MKV)
eventType: `migration`
eventDetail:
- “DPX image sequences converted to FFV1 video wrapped in a Matroska (MKV) container for preservation storage.”
agent override:
- agentName: RAWcooked
- agentType: software
- agentVersion: e.g. `24.11`
outcomeDetail:
- recommended keys:
  - command
  - tool.ffmpeg
  - tool.ffprobe
  - container
  - videoCodec
  - videoProfile

## Example RAWcooked event
{
  "packageId": "<packageId>",
  "agent": { "agentName": "RAWcooked", "agentType": "software", "agentVersion": "24.11" },
  "event": {
    "eventDateTime": "2026-02-11T14:01:41Z",
    "eventType": "migration",
    "eventDetail": "DPX image sequences converted to FFV1 video wrapped in a Matroska (MKV) container for preservation storage.",
    "outcome": "success",
    "outcomeDetail": "command=rawcooked --all --check -y;tool.ffmpeg=...;container=Matroska / WebM;videoCodec=ffv1"
  }
}

# Error handling conventions

## Goals
- Fail fast when integrity is at risk (fixity mismatch, missing inputs)
- Provide actionable error messages
- Make failures traceable in logs and in event output (when appropriate)

## Standard failure attributes (recommended)
- `error.message` (short)
- `error.details` (optional, longer)
- `event.outcome` = `failure`
- `event.detail` updated to include the reason (when the event itself should exist)
- Route FlowFile to REL_FAILURE

## Typical failure cases
- Missing required attributes (`package.name`, `events.payload.path`, `event.type`)
- Source paths not found in SAM-FS or staging
- Fixity mismatch (computed vs recorded checksums)
- External tool errors (rawcooked/ffmpeg non-zero exit, invalid output)
- Permissions issues (cannot write events file, cannot create directories)

## Logging
Prefer:
- concise `error.message` on FlowFile
- full stderr or stack trace in NiFi logs or `error.details` (if needed)

## Event emission on failure
- For steps where an event is meaningful even on failure (fixity check, migration), emit an event with:
  - `event.outcome=failure`
  - `event.detail` describing what failed and what was being validated/converted
  - `event.outcomeDetail` containing a short machine-readable hint (optional)

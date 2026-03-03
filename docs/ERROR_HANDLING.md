# Error handling conventions

## Goals
- Fail fast when integrity is at risk (fixity mismatch, missing inputs)
- Provide actionable error messages
- Make failures traceable in logs and in event output (when appropriate)

## Standard failure attributes (recommended)
- `error.message` (short)
- `error.details` (optional, longer, cap at 2048 characters)
- `error.stage` (dot-notation source, e.g. `fetch.tar.untar`, `checksum.verify`)
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

## Delivery margin buffer (DPS-2)
- Retain up to 2 failed packages in a manual review queue (configurable via `error.buffer.capacity`)
- Use one shared buffer-manager processor instance for both actions:
  - `error.buffer.op=acquire` (reserve/decide retain vs autocleanup)
  - `error.buffer.op=release` (free slot on manual retry/discard)
- When the margin is full, mark next failure for auto-cleanup:
  - `error.buffer.action=autocleanup`
  - `error.autocleaned=true`
- Keep standard failure attributes (`error.stage`, `error.message`, `error.details`) for both retained and auto-cleaned failures
- Ensure logs and DB messages clearly indicate when auto-cleanup happened due to full margin

# Delete produksjon work staging after SIP generation

Use [E-ARK - Cleanup.groovy](E-ARK%20-%20Cleanup.groovy) in
ExecuteGroovyScript to remove only the current package's work directory after
SIP generation has succeeded. The [event builder](../04_Events/README.md) reads
the retained metadata/media in the SIP and can run after cleanup.
The script is intentionally separate from produksjon's
[final all-directory cleanup](../10_Package%20cleanup/README.md).
Its single responsibility is deletion: the NiFi connections determine
when it runs. It does not check events, SIP contents, checksums, or upstream
status flags, and it does not depend on their attributes.

## Order

1. EarkSIPGenerator succeeds.
2. Capture the SIP timestamp/status and agent versions with UpdateAttribute.
3. Run this work-only cleanup script.
4. Create the three-record `events.ndjson` from the retained SIP using the event builder.
5. Inspect the SIP and event output during the first manual test, then continue to DPS processing.

Capture `eark.end` before cleanup so its timestamp represents SIP completion,
not the later event-build time. Use one concurrent task and never mutate,
regenerate, or clean the same package concurrently.
Connect script `failure` to Failure and `success` to the next processing step.

For the current test, the only deletion target is:

```text
/fc1/film_produksjon/work/digifilm_1300107_20130729
```

These locations remain intact:

- `/fc1/film_produksjon/work` itself and other packages under it.
- `/fc1/film_produksjon/transfer/digifilm_1300107_20130729` (completed SIP).
- `/fc1/film_produksjon/payloads/digifilm_1300107_20130729` (events and submission JSON).
- The original package in SAM-FS.

The whole target work directory is removed, including scratch/leftover files
at its root. There is no automatic backup, trash, or rollback. A partial I/O
failure can leave a partially removed work tree; inspect it before retrying.
Source content and the SIP are retained, but the exact deleted staging cannot
be assumed recoverable without another copy. Preserve any work-only material
you need before enabling cleanup.

## Required attributes and guards

The only required inputs are `package.name`, `work.base.dir`, and `work.dir`.

Before any deletion the script checks:

- All path attributes exist and are absolute. `work.base.dir` cannot be the
  filesystem root, and `work.dir` must be exactly its direct `package.name`
  child, not the base itself, a nested subdirectory, or another package.
- Work/base cannot themselves be symlinks. The deletion walk does not follow
  symlinks; a scratch symlink is unlinked without deleting its target.
- When work exists, its resolved physical path must remain the direct package
  child of its resolved base, and it must be a directory.

These are deletion-target safety guards only, not workflow validation. Configure
the work base correctly and connect cleanup only after stages that need work
have finished. The script will delete staging even if the SIP/events are absent
or their status flags say failure; it deliberately does not inspect them.

No FlowFile body changes or database/API calls occur. Success sets
`cleanup.work.status=OK`, `cleanup.work.deleted=true`, and `cleanup.work.path`.
If work was already removed, success instead sets `cleanup.work.deleted=false`.
Failure sets
`cleanup.work.status=FAIL`, `error.stage=cleanup.work`, `error.message`, and
`error.details`. Inputs are validated before traversal, not after deletion.

## Retries and final cleanup

After work deletion, resume from this cleanup step, the event builder, or the
retained SIP/payload upload steps. Event creation no longer needs work staging.
Earlier work-dependent stages such as fetch still need restaging. Local
cleanup retries do not repost events and do not modify existing payloads.

Retain the SIP and payloads until the existing DPS completion condition allows
final package cleanup. The final all-directory script must not be used in
this early slot, because it would also delete files still needed for delivery.

## Local verification

With Groovy 4, from the repository root:

```sh
groovy film_produksjon/tests/cleanup-work.groovy
```

Tests only delete fixture directories created with `Files.createTempDirectory`.
They cover package-only removal, protected source/SIP/payload preservation,
already-deleted retries, missing attributes, root/outside/nested path rejection,
symlink handling, and independence from SIP/event/status validation. No server
cleanup is run.

# Final package cleanup

Use [01_Delete package directories.groovy](01_Delete%20package%20directories.groovy)
in the final **Cleanup** ExecuteGroovyScript processor. The user reports that
this script is working in the produksjon flow. It is a byte-identical copy of
the bevaring package-cleanup script, retained here so the deployed script can
be found under the flow that uses it. No behavior or NiFi configuration change
is needed when switching to this repository copy.

This final cleanup deletes the current package's payloads, transfer, and work
directories. The earlier [work-only cleanup](../05_Cleanup%20work/README.md)
remains a separate step after SIP generation; it retains the SIP and payloads
for event creation and DPS delivery.

## Inputs and targets

Required attributes: `package.name`, `payloads.base.dir`, `transfer.base.dir`,
and `work.base.dir`. With the configured produksjon roots, the targets are:

```text
/fc1/film_produksjon/payloads/<package.name>/
/fc1/film_produksjon/transfer/<package.name>/
/fc1/film_produksjon/work/<package.name>/
```

The script uses the incoming base attributes, not hard-coded produksjon paths.
Only the named package directories and their contents are removed; parent
directories and other packages remain. Missing directories, including work
already removed earlier, are skipped successfully.

Deletion is its only responsibility. It does not check submission status,
events, checksums, or SIP contents. The calling flow determines when cleanup
is appropriate after delivery, once local files are no longer needed.
There are no database/API calls or changes to the FlowFile body.

All three targets are validated before deletion starts. Package names must be
single directory names and bases must be absolute and not filesystem roots.
Symlink bases/targets and non-directory targets are rejected. Links within
package directories are deleted without following them. Cleanup does not
provide rollback if deletion later fails.

## Results and retries

- `cleanup.package.status`: `OK` or `FAIL`.
- `cleanup.package.path.payloads|transfer|work`: resolved package paths.
- `cleanup.package.exists.payloads|transfer|work`: whether the target existed
  during this run's validation.
- `cleanup.package.deleted.payloads|transfer|work`: whether the whole target
  directory was removed in this run.
- `cleanup.package.deleted.any`: whether at least one whole package directory
  was removed. Flags are reset for each attempt.
- Failure sets `error.stage=cleanup.package`, `error.message`, and `error.details`.

Connect `success` to the next finalization step and `failure` to failure handling.
A retry skips already-absent directories and attempts the remaining targets.
The earlier `cleanup.work.*` attributes alone do not indicate final cleanup ran.

## Local verification

From the repository root, run the existing fixture harness against this copy:

```sh
groovy film_bevaring/tests/package-cleanup.groovy \
  'film_produksjon/10_Package cleanup/01_Delete package directories.groovy'
```

The tests only delete temporary fixtures. They cover deletion of all three
package directories, absent work, retry flags, invalid paths, symlinks, and
preservation of source files and other packages. When changing either copy,
review the other and run the harness against both.

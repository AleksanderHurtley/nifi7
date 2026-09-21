#!/usr/bin/env python3
"""Validate a deletion manifest against paginated DPS submission responses."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


DEFAULT_ROOT = "/global/film/00/film/bevaring/"


def read_manifest(path: Path) -> list[str]:
    paths: list[str] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.reader(handle):
            if not row or not row[0].strip():
                continue
            value = row[0].strip()
            if not paths and value.lower() == "path":
                continue
            paths.append(value)
    return paths


def read_payloads(directory: Path) -> tuple[list[dict], list[dict]]:
    pages: list[dict] = []
    submissions: list[dict] = []
    for path in sorted(directory.glob("*.json")):
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        page = payload.get("page")
        content = payload.get("content")
        if not isinstance(page, dict) or not isinstance(content, list):
            raise ValueError(f"{path} is not a paginated DPS response")
        pages.append({"file": path.name, **page, "records": len(content)})
        submissions.extend(content)
    return pages, submissions


def duplicates(values: list[str]) -> list[str]:
    return sorted(value for value, count in Counter(values).items() if count > 1)


def validate(batch_dir: Path, expected_root: str) -> dict:
    manifest_path = batch_dir / "package-paths.txt"
    payload_dir = batch_dir / "dps-submissions"
    manifest_paths = read_manifest(manifest_path)
    pages, submissions = read_payloads(payload_dir)

    basenames = [PurePosixPath(path).name for path in manifest_paths]
    submission_ids = [str(item.get("submissionId", "")) for item in submissions]
    object_ids = [str(item.get("objectId", "")) for item in submissions]
    contract_ids = [str(item.get("contractId", "")) for item in submissions]
    objects = {str(item.get("objectId", "")): item for item in submissions}

    total_pages_values = {page.get("totalPages") for page in pages}
    total_elements_values = {page.get("totalElements") for page in pages}
    page_numbers = sorted(page.get("number") for page in pages)
    expected_page_numbers: list[int] = []
    if len(total_pages_values) == 1:
        total_pages = next(iter(total_pages_values))
        if isinstance(total_pages, int):
            expected_page_numbers = list(range(total_pages))

    missing = sorted(set(basenames) - set(object_ids))
    manifest_non_preserved = sorted(
        {
            object_id: objects[object_id].get("status")
            for object_id in set(basenames) & set(object_ids)
            if objects[object_id].get("status") != "PRESERVED"
        }.items()
    )
    outside_manifest_non_preserved = sorted(
        (
            str(item.get("objectId", "")),
            str(item.get("status", "")),
            str(item.get("submissionId", "")),
        )
        for item in submissions
        if item.get("objectId") not in set(basenames)
        and item.get("status") != "PRESERVED"
    )

    unsafe_paths = sorted(
        path
        for path in manifest_paths
        if not path.startswith(expected_root)
        or path == expected_root.rstrip("/")
        or ".." in PurePosixPath(path).parts
    )

    issues: list[str] = []
    duplicate_paths = duplicates(manifest_paths)
    duplicate_basenames = duplicates(basenames)
    duplicate_submission_ids = duplicates(submission_ids)
    duplicate_object_ids = duplicates(object_ids)
    if not manifest_paths:
        issues.append("Manifest is empty")
    if unsafe_paths:
        issues.append(f"Unsafe or unexpected manifest paths: {len(unsafe_paths)}")
    if duplicate_paths:
        issues.append(f"Duplicate manifest paths: {len(duplicate_paths)}")
    if duplicate_basenames:
        issues.append(f"Duplicate manifest object IDs: {len(duplicate_basenames)}")
    if duplicate_submission_ids:
        issues.append(f"Duplicate DPS submission IDs: {len(duplicate_submission_ids)}")
    if duplicate_object_ids:
        issues.append(f"Duplicate DPS object IDs: {len(duplicate_object_ids)}")
    if "" in submission_ids or "" in object_ids or "" in contract_ids:
        issues.append("One or more DPS records lack a required identifier")
    if len(set(contract_ids)) != 1:
        issues.append("DPS records contain zero or multiple contract IDs")
    if len(total_pages_values) != 1 or len(total_elements_values) != 1:
        issues.append("DPS pagination totals are inconsistent across files")
    if page_numbers != expected_page_numbers:
        issues.append("DPS page sequence is incomplete or duplicated")
    if len(total_elements_values) == 1 and len(submissions) != next(iter(total_elements_values)):
        issues.append("DPS record count does not equal totalElements")
    if missing:
        issues.append(f"Manifest object IDs missing from DPS: {len(missing)}")
    if manifest_non_preserved:
        issues.append(
            f"Manifest object IDs without PRESERVED status: {len(manifest_non_preserved)}"
        )

    status_counts = dict(sorted(Counter(str(item.get("status", "")) for item in submissions).items()))
    report = {
        "schemaVersion": 1,
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "batchId": batch_dir.name,
        "result": "PASS" if not issues else "FAIL",
        "expectedPathRoot": expected_root,
        "counts": {
            "manifestPaths": len(manifest_paths),
            "uniqueManifestPaths": len(set(manifest_paths)),
            "uniqueManifestObjectIds": len(set(basenames)),
            "dpsPages": len(pages),
            "dpsSubmissions": len(submissions),
            "uniqueDpsSubmissionIds": len(set(submission_ids)),
            "uniqueDpsObjectIds": len(set(object_ids)),
            "manifestObjectsFoundInDps": len(set(basenames) & set(object_ids)),
            "manifestObjectsPreserved": sum(
                objects[object_id].get("status") == "PRESERVED"
                for object_id in set(basenames) & set(object_ids)
            ),
            "dpsObjectsOutsideManifest": len(set(object_ids) - set(basenames)),
        },
        "dpsStatusCounts": status_counts,
        "dpsContractIds": sorted(set(contract_ids)),
        "pagination": {
            "pageNumbers": page_numbers,
            "declaredTotalPages": sorted(total_pages_values),
            "declaredTotalElements": sorted(total_elements_values),
        },
        "issues": issues,
        "exceptions": {
            "missingManifestObjectIds": missing,
            "manifestNonPreserved": manifest_non_preserved,
            "outsideManifestNonPreserved": outside_manifest_non_preserved,
            "unsafePaths": unsafe_paths,
            "duplicatePaths": duplicate_paths,
            "duplicateManifestObjectIds": duplicate_basenames,
            "duplicateDpsSubmissionIds": duplicate_submission_ids,
            "duplicateDpsObjectIds": duplicate_object_ids,
        },
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_dir", type=Path)
    parser.add_argument("--root", default=DEFAULT_ROOT)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    report = validate(args.batch_dir, args.root)
    if args.report:
        args.report.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    counts = report["counts"]
    print(f"Validation: {report['result']}")
    print(f"Batch: {report['batchId']}")
    print(f"Manifest paths: {counts['manifestPaths']}")
    print(f"DPS submissions: {counts['dpsSubmissions']}")
    print(f"Manifest object IDs found: {counts['manifestObjectsFoundInDps']}")
    print(f"Manifest object IDs preserved: {counts['manifestObjectsPreserved']}")
    if report["issues"]:
        for issue in report["issues"]:
            print(f"ERROR: {issue}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

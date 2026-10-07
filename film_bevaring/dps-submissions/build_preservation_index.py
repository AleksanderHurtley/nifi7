#!/usr/bin/env python3
"""Merge explicitly ordered DPS snapshots into an index of last observed states."""

import argparse
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(snapshot_dirs, output, contract_id):
    records = {}
    object_submissions = {}
    sources = []
    snapshot_names = [path.name for path in snapshot_dirs]
    if len(set(snapshot_names)) != len(snapshot_names):
        raise ValueError("Snapshot names must be unique and ordered oldest to newest")
    for directory in snapshot_dirs:
        files = sorted(directory.glob("*.json"))
        if not files:
            raise ValueError(f"No response pages in {directory}")
        seen_ids, page_numbers, totals = set(), set(), set()
        source_files, snapshot_count = [], 0
        for file in files:
            payload = json.loads(file.read_text())
            page, content = payload.get("page"), payload.get("content")
            if not isinstance(page, dict) or not isinstance(content, list):
                raise ValueError(f"Not a paginated response: {file}")
            number, size = page.get("number"), page.get("size")
            total, total_pages = page.get("totalElements"), page.get("totalPages")
            if not all(type(v) is int for v in (number, size, total, total_pages)):
                raise ValueError(f"Invalid pagination: {file}")
            if size <= 0 or total <= 0 or total_pages != (total + size - 1) // size:
                raise ValueError(f"Inconsistent pagination: {file}")
            if number < 0 or number >= total_pages or number in page_numbers:
                raise ValueError(f"Invalid or duplicate page number: {file}")
            if len(content) != min(size, total - number * size):
                raise ValueError(f"Unexpected page length: {file}")
            page_numbers.add(number)
            totals.add((total, total_pages, size))
            source_ref = os.path.relpath(file, output.parent)
            source_files.append({"path": source_ref, "sha256": sha256(file), "page": page})
            for item in content:
                if not isinstance(item, dict) or not all(
                    isinstance(item.get(key), str) and item[key]
                    for key in ("contractId", "submissionId", "objectId", "status")
                ):
                    raise ValueError(f"Missing submission identifiers/status in {file}")
                if item["contractId"] != contract_id:
                    raise ValueError(f"Unexpected contract in {file}")
                identifier, object_id = item["submissionId"], item["objectId"]
                if identifier in seen_ids:
                    raise ValueError(f"Duplicate submission within {directory}: {identifier}")
                seen_ids.add(identifier)
                if object_id in object_submissions and object_submissions[object_id] != identifier:
                    raise ValueError(f"Multiple submissions for object {object_id}; review required")
                previous = records.get(identifier)
                if previous and previous["objectId"] != object_id:
                    raise ValueError(f"Submission changed object ID: {identifier}")
                observation = {
                    "snapshot": directory.name, "file": source_ref,
                    "page": number, "status": item["status"],
                }
                history = previous["evidence"]["observations"] if previous else []
                records[identifier] = {
                    key: value for key, value in item.items() if key not in ("files", "evidence")
                }
                records[identifier]["evidence"] = {
                    "latestSnapshot": directory.name,
                    "observations": history + [observation],
                }
                object_submissions[object_id] = identifier
                snapshot_count += 1
        if len(totals) != 1:
            raise ValueError(f"Pagination totals changed within {directory}")
        total, total_pages, size = next(iter(totals))
        sources.append({
            "snapshot": directory.name, "records": snapshot_count,
            "declaredTotalElements": total, "declaredTotalPages": total_pages,
            "pagesPresent": sorted(page_numbers),
            "completeSnapshot": len(page_numbers) == total_pages,
            "files": source_files,
        })
    content = sorted(records.values(), key=lambda item: item["objectId"])
    return {
        "schemaVersion": 1,
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "contractId": contract_id,
        "description": "Last observed submission states; evidence spans multiple snapshots, not one live API response.",
        "mergeRule": "Explicit oldest-to-newest snapshot order; newer records replace older states, including non-PRESERVED states.",
        "sources": sources,
        "summary": {
            "uniqueSubmissions": len(content),
            "uniqueObjects": len(object_submissions),
            "duplicateObservationsMerged": sum(s["records"] for s in sources) - len(content),
            "statusCounts": dict(sorted(Counter(r["status"] for r in content).items())),
            "latestObservationBySnapshot": dict(sorted(Counter(r["evidence"]["latestSnapshot"] for r in content).items())),
            "latestDeclaredTotalElements": sources[-1]["declaredTotalElements"],
            "countMatchesLatestDeclaredTotal": len(content) == sources[-1]["declaredTotalElements"],
            "allRecordsObservedInLatestSnapshot": all(r["evidence"]["latestSnapshot"] == sources[-1]["snapshot"] for r in content),
        },
        "content": content,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot_dirs", nargs="+", type=Path, help="Oldest to newest; order is explicit")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--contract", default="91c5")
    args = parser.parse_args()
    try:
        index = build(args.snapshot_dirs, args.output, args.contract)
        # Build completely before replacing an existing index.
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n")
        temporary.replace(args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f"ERROR: {error}\n")
    print(json.dumps(index["summary"], indent=2))


if __name__ == "__main__":
    main()

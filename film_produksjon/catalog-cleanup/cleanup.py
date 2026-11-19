#!/usr/bin/env python3
"""Scan film production URNs, dump the records to change, and apply a saved Collections cleanup plan."""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


URN = re.compile(r"URN:NBN:no-nb_digifilm_[A-Za-z0-9_-]+")
PRIREF = re.compile(r"[1-9][0-9]*")
SIZE = re.compile(r"[0-9]+")
SCHEMA_VERSION = 3
KNOWN_ENVIRONMENTS = {
    "api.collections.stage.nb.no": "stage",
    "api.dev.produksjon.nb.no": "stage",
    "nb-axiell.nb.local": "prod",
    "api.produksjon.nb.no": "prod",
}
# Links that deleting a record would silently remove from other records. A deletion candidate must have none.
# Shared copied_from links are expected: duplicates are usually copied from the same source.
BLOCKING_LINKS = ("children", "copiedTo", "relatedObjects")
# Relation fields of a described record and their names in the dump index
LINK_NAMES = {"parents": "partOf", "children": "parts", "copiedFrom": "copiedFrom", "copiedTo": "copiedTo",
              "relatedObjects": "relatedObjects"}
DEAD_LINKS = "Item still lists deleted item parts (dead links)"
REFUSED_ITEM = "Collections REST refused the delete (HTTP 409): the item still lists item parts, possibly dead links"


class CatalogError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class DumpError(Exception):
    pass


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def check_url(url, token=None, *, https=False):
    parsed = urlsplit(url)
    if (
        parsed.scheme not in ("https", "http")
        or ((https or token) and parsed.scheme != "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Use a plain catalog endpoint URL, with HTTPS when sending credentials")


def direct_refs(record, group, field, expanded):
    ids = set()
    for relation in record.findall(group):
        direct = [node.text.strip() for node in relation.findall(field) if node.text and node.text.strip()]
        nested = [
            node.text.strip() for node in relation.findall(f"{expanded}/priref")
            if node.text and node.text.strip()
        ]
        refs = set(direct + nested)
        if len(refs) > 1 or any(not PRIREF.fullmatch(ref) for ref in refs):
            return None
        if not refs and any(text.strip() for text in relation.itertext()):
            return None
        ids.update(refs)
    return sorted(ids)


def record_priref(record):
    return record.get("priref") or record.findtext("priref", "")


def describe_record(record):
    priref = record_priref(record)
    if not PRIREF.fullmatch(priref):
        raise CatalogError("Catalog record has an invalid priref")
    sizes = record.findall("file.size")
    size = sizes[0].text if len(sizes) == 1 else None
    size = size.strip() if size else None
    guid = record.findtext("guid", "").strip()
    created = record.get("created", "").strip()
    return {
        "priref": priref,
        "type": record.findtext("record_type/value[@lang='neutral']", "").strip(),
        "objectNumber": record.findtext("object_number", "").strip() or None,
        "deleted": record.get("deleted", "").lower() == "true",
        "urns": sorted({node.text.strip() for node in record.findall("PID_data_URN") if node.text and node.text.strip()}),
        "size": int(size) if size and SIZE.fullmatch(size) else None,
        "identity": f"guid:{guid}" if guid else f"created:{created}" if created else None,
        "parents": direct_refs(record, "Part_of", "part_of_reference.lref", "part_of_reference"),
        "children": direct_refs(record, "Parts", "parts.reference.lref", "parts_reference"),
        "copiedFrom": direct_refs(record, "CopiedFrom", "copied_from.lref", "copied_from"),
        "copiedTo": direct_refs(record, "CopiedTo", "copied_to.lref", "copied_to"),
        "relatedObjects": direct_refs(record, "Related_object", "related_object.reference.lref",
                                      "related_object.reference"),
    }


def environment_of(url):
    return KNOWN_ENVIRONMENTS.get(urlsplit(url).hostname)


class Catalog:
    def __init__(self, catalog_url, delete_base_url, environment, lookup_token=None, delete_token=None, oauth=None,
                 request_interval=0):
        check_url(catalog_url, lookup_token)
        if delete_base_url:
            check_url(delete_base_url, https=True)
        for url in filter(None, (catalog_url, delete_base_url)):
            if environment_of(url) != environment:
                raise ValueError(f"{urlsplit(url).hostname} is not a known {environment} catalog endpoint")
        if oauth:
            check_url(oauth[0], https=True)
        self.environment = environment
        self.catalog_url = catalog_url.rstrip("?")
        self.delete_base_url = delete_base_url.rstrip("/") if delete_base_url else None
        self.lookup_token = lookup_token
        self.delete_token = delete_token
        self.oauth = oauth
        self.access_token = None
        self.expires_at = 0
        self.opener = build_opener(NoRedirect())
        self.request_interval = request_interval
        self.last_request = None

    def throttle(self):
        """Waits so that catalog lookups and deletes start at least request_interval seconds apart."""
        if self.last_request is not None:
            wait = self.last_request + self.request_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        self.last_request = time.monotonic()

    def token(self, fixed, url):
        if fixed or not self.oauth or urlsplit(url).scheme != "https":
            return fixed
        if not self.access_token or time.monotonic() + 30 >= self.expires_at:
            token_url, client_id, secret = self.oauth
            started = time.monotonic()
            data = urlencode({"grant_type": "client_credentials", "client_id": client_id, "client_secret": secret})
            try:
                response = json.loads(self.request("POST", token_url, None, "application/json", 200, data.encode()))
                self.access_token = response["access_token"]
                self.expires_at = started + int(response["expires_in"])
            except (TypeError, KeyError, ValueError):
                raise CatalogError("Token endpoint returned an invalid OAuth response") from None
            if not isinstance(self.access_token, str) or not self.access_token:
                raise CatalogError("Token endpoint returned an invalid access token")
        return self.access_token

    def request(self, method, url, token, accept, expected_status, data=None,
                content_type="application/x-www-form-urlencoded"):
        headers = {"Accept": accept}
        if token:
            if not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
                raise CatalogError("Invalid bearer token format")
            headers["Authorization"] = f"Bearer {token}"
        if data is not None:
            headers["Content-Type"] = content_type
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=45) as response:
                if response.status != expected_status:
                    raise CatalogError(f"{method} expected HTTP {expected_status}, got {response.status}",
                                       response.status)
                return response.read()
        except HTTPError as error:
            raise CatalogError(f"{method} returned HTTP {error.code}", error.code) from None
        except (URLError, TimeoutError, OSError) as error:
            raise CatalogError(f"{method} failed during communication ({type(error).__name__})") from None

    def lookup(self, expression):
        """Returns the described records and the response body exactly as the catalog sent it."""
        query = urlencode({"database": "catalogue", "output": "xml", "limit": 100, "search": expression})
        token = self.token(self.lookup_token, self.catalog_url)
        self.throttle()
        body = self.request("GET", f"{self.catalog_url}?{query}", token, "application/xml", 200)
        try:
            root = ET.fromstring(body)
        except ET.ParseError as error:
            raise CatalogError("Lookup returned invalid XML") from error
        if root.tag != "adlibXML" or root.find("diagnostic/error") is not None:
            raise CatalogError("Lookup returned a catalog error or an unexpected response")
        hits = root.findtext("diagnostic/hits", "").strip()
        records = [describe_record(record) for record in root.findall("recordList/record")]
        if not SIZE.fullmatch(hits) or int(hits) != len(records):
            raise CatalogError("Lookup response is incomplete")
        if len({record["priref"] for record in records}) != len(records):
            raise CatalogError("Lookup returned duplicate prirefs")
        return sorted(records, key=lambda record: int(record["priref"])), body

    def by_urn(self, urn, with_body=False):
        records, body = self.lookup(f"PID_data_URN = '{urn}'")
        if any(urn not in record["urns"] or record["deleted"] for record in records):
            raise CatalogError("URN lookup returned a deleted record or a record for a different URN")
        return (records, body) if with_body else records

    def by_priref(self, priref, with_body=False):
        if not PRIREF.fullmatch(priref):
            raise CatalogError("Invalid parent priref")
        records, body = self.lookup(f"priref = {priref}")
        if len(records) != 1 or records[0]["priref"] != priref:
            raise CatalogError(f"Parent lookup for {priref} did not return exactly that record")
        return (records[0], body) if with_body else records[0]

    def exists(self, priref):
        if not PRIREF.fullmatch(priref):
            raise CatalogError("Invalid priref")
        return bool(self.lookup(f"priref = {priref}")[0])

    def clear_item_parts(self, priref):
        """Empties the parts field of one item with updaterecord. Tested in stage: other fields are kept."""
        if not PRIREF.fullmatch(priref):
            raise CatalogError("Invalid priref")
        body = json.dumps({"adlibJSON": {"recordList": [
            {"priref": int(priref), "record_type": "ITEM", "parts.reference.lref": ""}]}}).encode()
        query = urlencode({"database": "catalogue", "command": "updaterecord", "output": "json"})
        token = self.token(self.lookup_token, self.catalog_url)
        self.throttle()
        response = self.request("POST", f"{self.catalog_url}?{query}", token, "application/json", 200, body,
                                content_type="application/json")
        try:
            adlib = json.loads(response)["adlibJSON"]
        except (ValueError, KeyError, TypeError):
            raise CatalogError("updaterecord returned an unexpected response") from None
        if not isinstance(adlib, dict) or (adlib.get("diagnostic") or {}).get("error"):
            raise CatalogError("The catalog refused the updaterecord")

    def delete(self, priref):
        if not PRIREF.fullmatch(priref) or not self.delete_base_url or not (self.delete_token or self.oauth):
            raise CatalogError("Deletion requires a numeric priref, HTTPS endpoint and authentication")
        url = f"{self.delete_base_url.rstrip('/')}/api/v1/records/{priref}"
        token = self.token(self.delete_token, url)
        self.throttle()
        self.request("DELETE", url, token, "application/json", 204)


def read_urns(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["urn"]:
            raise ValueError("Input CSV must have one column with the header urn")
        seen = set()
        urns = []
        for row in reader:
            urn = (row.get("urn") or "").strip()
            if None in row or not URN.fullmatch(urn):
                raise ValueError(f"Invalid production URN at CSV line {reader.line_num}")
            if urn in seen:
                raise ValueError(f"Repeated URN at CSV line {reader.line_num}: {urn}")
            seen.add(urn)
            urns.append(urn)
        if not urns:
            raise ValueError("Input CSV contains no URNs")
        return urns


def has_blocking_links(record):
    # A relation that could not be read (None) counts as a link.
    return any(record.get(key, []) != [] for key in BLOCKING_LINKS)


def assess(urn, records):
    result = {"urn": urn, "records": records, "primary": None, "delete": []}
    if not records:
        return dict(result, status="MISSING")
    if any(record["type"] != "ITEM_PART" for record in records):
        return dict(result, status="REVIEW_TYPE")
    positive = [record for record in records if record["size"] is not None and record["size"] > 0]
    if len(positive) != 1:
        return dict(result, status="REVIEW_PRIMARY")
    primary = positive[0]["priref"]
    others = [record for record in records if record["priref"] != primary]
    if any(record["size"] != 0 for record in others):
        return dict(result, status="REVIEW_SIZE", primary=primary)
    if any(record["urns"] != [urn] for record in others):
        return dict(result, status="REVIEW_SHARED_URN", primary=primary)
    if any(has_blocking_links(record) for record in others):
        return dict(result, status="REVIEW_LINKED", primary=primary)
    deletes = [record["priref"] for record in others]
    return dict(result, status="CLEANUP" if deletes else "READY", primary=primary, delete=deletes)


def parent_problem(parent, identity, manifest):
    """Why an item may not be removed, or None. Child links are checked by the caller."""
    if parent["type"] != "ITEM" or parent["deleted"] or not parent["identity"]:
        return "Not an active ITEM with a known identity"
    if identity and parent["identity"] != identity:
        return "Item changed since it was identified"
    if parent["parents"] != manifest:
        return "Item is not under the same manifestation as the primary's item"
    if parent.get("copiedTo", []) != [] or parent.get("relatedObjects", []) != []:
        return "Item has copies or related objects"
    return None


def find_items(catalog, entry):
    """Adds the items to remove after the duplicates to the entry: items whose only item parts are the duplicates,
    under the same manifestation as the primary's item. Returns their lookup responses for the dump."""
    entry.update(items={}, itemsKept=[], primaryItem=None)
    primary = next(record for record in entry["records"] if record["priref"] == entry["primary"])
    if not primary["parents"] or len(primary["parents"]) != 1:
        entry["itemsNote"] = "Primary has no single confirmed direct parent"
        return {}
    manifest = catalog.by_priref(primary["parents"][0])["parents"]
    entry["primaryItem"] = {"priref": primary["parents"][0], "partOf": manifest}
    if not manifest:
        entry["itemsNote"] = "Primary's item has no confirmed manifestation"
        return {}
    candidates = defaultdict(set)
    for record in entry["records"]:
        parents = record["parents"]
        if record["priref"] in entry["delete"] and parents and len(parents) == 1:
            if parents[0] not in primary["parents"]:
                candidates[parents[0]].add(record["priref"])
    bodies = {}
    for priref, parts in sorted(candidates.items(), key=lambda candidate: int(candidate[0])):
        parent, body = catalog.by_priref(priref, with_body=True)
        reason = parent_problem(parent, None, manifest)
        if not reason and (parent["children"] is None or set(parent["children"]) != parts):
            reason = "Item has other or unreadable item parts"
        if reason:
            entry["itemsKept"].append({"priref": priref, "reason": reason})
            continue
        entry["items"][priref] = {"identity": parent["identity"], "objectNumber": parent["objectNumber"],
                                  "manifest": manifest, "itemParts": sorted(parts)}
        bodies[priref] = body
    return bodies


def sha256(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def dump_name(urn):
    # The URN pattern allows only letters, digits, '_' and '-' after the prefix.
    return urn.removeprefix("URN:NBN:no-nb_")


def dump_files(entry):
    """The files of an entry's dump and the prirefs each must hold."""
    return {"urn.xml": {record["priref"] for record in entry["records"]},
            **{f"item_{priref}.xml": {priref} for priref in entry["items"]}}


def write_synced(path, data):
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def write_dump(root, entry, environment, urn_body, item_bodies):
    """Saves the lookup responses for the records the cleanup changes, then reads them back."""
    folder = root / dump_name(entry["urn"])
    folder.mkdir()
    files = {"urn.xml": urn_body, **{f"item_{priref}.xml": body for priref, body in item_bodies.items()}}
    for name, data in files.items():
        write_synced(folder / name, data)
    links = {record["priref"]: {name: record[key] for key, name in LINK_NAMES.items() if record.get(key, []) != []}
             for record in entry["records"]}
    for priref, item in entry["items"].items():
        links[priref] = {"partOf": item["manifest"], "parts": item["itemParts"]}
    index = {
        "urn": entry["urn"],
        "environment": environment,
        "scannedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "primary": entry["primary"],
        "primaryItem": entry["primaryItem"],
        "duplicates": entry["delete"],
        "items": sorted(entry["items"], key=int),
        "links": links,
        "files": {name: sha256(data) for name, data in files.items()},
    }
    data = json.dumps(index, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"
    write_synced(folder / "index.json", data)
    dump = {"path": folder.name, "index": sha256(data)}
    check_dump(root, entry, dump)
    return dump


def check_dump(root, entry, dump):
    """Reads a dump back: every checksum must match and every file must hold exactly the expected records."""
    if not isinstance(dump, dict) or dump.get("path") != dump_name(entry["urn"]):
        raise DumpError("The plan has no dump for this URN")
    folder = root / dump["path"]
    try:
        data = (folder / "index.json").read_bytes()
        if sha256(data) != dump.get("index"):
            raise DumpError("index.json does not match its checksum in the plan")
        index = json.loads(data)
        expected = dump_files(entry)
        if index.get("urn") != entry["urn"] or set(index.get("files", {})) != set(expected):
            raise DumpError("index.json does not describe this URN's records")
        for name, prirefs in expected.items():
            body = (folder / name).read_bytes()
            if sha256(body) != index["files"][name]:
                raise DumpError(f"{name} does not match its checksum")
            found = {record_priref(record) for record in ET.fromstring(body).findall("recordList/record")}
            if found != prirefs:
                raise DumpError(f"{name} does not hold the expected records")
    except (OSError, ValueError, ET.ParseError) as error:
        raise DumpError(f"The dump could not be read ({type(error).__name__})") from None


def scan(catalog, urns, dump_root):
    """Reads every URN. For each URN to clean, finds the items to remove and dumps the records before any change."""
    dump_root.mkdir(parents=True)
    entries = []
    for urn in urns:
        try:
            records, body = catalog.by_urn(urn, with_body=True)
            entry = assess(urn, records)
            if entry["status"] == "CLEANUP":
                item_bodies = find_items(catalog, entry)
                entry["dump"] = write_dump(dump_root, entry, catalog.environment, body, item_bodies)
        except CatalogError as error:
            entry = {"urn": urn, "status": "LOOKUP_FAILED", "records": [], "error": str(error)}
        entries.append(entry)
    return {
        "version": SCHEMA_VERSION,
        "environment": catalog.environment,
        "catalogUrl": catalog.catalog_url,
        "deleteBaseUrl": catalog.delete_base_url,
        "dumpDir": dump_root.name,
        "counts": dict(Counter(entry["status"] for entry in entries)),
        "entries": entries,
    }


def delete_record(catalog, priref, record_type, record, emit, pause, dump=None):
    # The description goes to the audit log, and the dump holds the full record, so a restored one can be re-linked.
    emit("DELETE_REQUESTED", priref=priref, recordType=record_type, record=record, **({"dump": dump} if dump else {}))
    catalog.delete(priref)
    emit("DELETED", priref=priref, recordType=record_type)
    if pause:
        time.sleep(pause)


def delete_empty_parent(catalog, priref, expected, emit, pause):
    """Reads an item kept by an earlier run and deletes it once it lists none of its former item parts."""
    parent = catalog.by_priref(priref)

    def keep(reason, retry):
        emit("PARENT_KEPT", priref=priref, reason=reason, retry=retry, **expected)
        return False

    reason = parent_problem(parent, expected["identity"], expected["manifest"])
    if reason:
        return keep(reason, False)
    if parent["children"] is None or not set(parent["children"]) <= set(expected["itemParts"]):
        return keep("Item has other or unreadable item parts", False)
    if parent["children"]:
        return keep(DEAD_LINKS, True)
    try:
        delete_record(catalog, priref, "ITEM", parent, emit, pause)
    except CatalogError as error:
        if error.status != 409:
            raise
        return keep(REFUSED_ITEM, True)
    return True


def clean_entry(catalog, entry, dump_root, remove_empty_parents, emit, pause):
    """Checks the dump, deletes the duplicates and their items, and confirms the primary with one lookup."""
    urn = entry["urn"]
    try:
        check_dump(dump_root, entry, entry.get("dump"))
    except DumpError as error:
        emit("DUMP_INVALID", error=str(error))
        return "DUMP_INVALID"
    folder = entry["dump"]["path"]
    records = {record["priref"]: record for record in entry["records"]}
    for priref in entry["delete"]:
        delete_record(catalog, priref, "ITEM_PART", records[priref], emit, pause, f"{folder}/urn.xml")
    if remove_empty_parents:
        if entry.get("itemsNote"):
            emit("PARENT_CLEANUP_SKIPPED", reason=entry["itemsNote"])
        for kept in entry["itemsKept"]:
            emit("PARENT_KEPT", priref=kept["priref"], reason=kept["reason"], retry=False)
        for priref, item in sorted(entry["items"].items(), key=lambda pair: int(pair[0])):
            # Collections REST reads the item and refuses it (409) while it lists item parts.
            try:
                delete_record(catalog, priref, "ITEM", item, emit, pause, f"{folder}/item_{priref}.xml")
            except CatalogError as error:
                if error.status != 409:
                    raise
                emit("PARENT_KEPT", priref=priref, reason=REFUSED_ITEM, retry=True, **item)
    remaining = [record for record in entry["records"] if record["priref"] not in entry["delete"]]
    found = catalog.by_urn(urn)
    if assess(urn, found)["status"] == "READY" and found == remaining:
        emit("PRIMARY_VERIFIED", priref=entry["primary"])
        return "VERIFIED"
    return "REVIEW_AFTER_CLEANUP"


class Audit:
    """Writes NDJSON audit rows and remembers the items that were kept."""

    def __init__(self, log):
        self.log = log
        self.kept = {}

    def emit(self, action, **fields):
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.log.write(json.dumps({"timestamp": timestamp, "action": action, **fields}, ensure_ascii=False) + "\n")
        self.log.flush()
        if action == "PARENT_KEPT":
            self.kept[fields["priref"]] = {"priref": fields["priref"], "reason": fields["reason"],
                                           "retry": fields["retry"]}
        elif action == "DELETED":
            self.kept.pop(fields["priref"], None)

    def finish(self, counts):
        kept = sorted(self.kept.values(), key=lambda parent: int(parent["priref"]))
        self.emit("RUN_FINISHED", counts=dict(counts), parentsKept=kept)
        return kept


def check_endpoints(source, catalog, what):
    if (
        source.get("version") != SCHEMA_VERSION
        or source.get("environment") != catalog.environment
        or source.get("catalogUrl") != catalog.catalog_url
        or source.get("deleteBaseUrl") != catalog.delete_base_url
    ):
        raise ValueError(f"{what} and configured environment and endpoints must match")


def apply(catalog, plan, log, dump_root, remove_empty_parents=False, pause=0, only=None, limit=None):
    """Applies the CLEANUP entries of a scan plan. Other entries need no change or a manual review."""
    check_endpoints(plan, catalog, "Scan plan")
    entries = plan.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Scan plan has no URNs")
    statuses = {}
    for entry in entries:
        urn = entry["urn"]
        if not URN.fullmatch(urn) or urn in statuses:
            raise ValueError("Scan plan contains an invalid or repeated URN")
        statuses[urn] = entry["status"]
        if entry["status"] in ("CLEANUP", "READY"):
            expected = assess(urn, entry["records"])
            if any(expected[key] != entry.get(key) for key in ("status", "primary", "delete")):
                raise ValueError(f"Inconsistent cleanup selection for {urn}")
    for urn in only or []:
        if statuses.get(urn) != "CLEANUP":
            raise ValueError(f"{urn} is not a CLEANUP entry in the scan plan ({statuses.get(urn, 'missing')})")
    selected = [entry for entry in entries
                if entry["status"] == "CLEANUP" and (not only or entry["urn"] in only)][:limit]

    audit = Audit(log)
    audit.emit("RUN_STARTED", command="apply", version=SCHEMA_VERSION, environment=catalog.environment,
               catalogUrl=catalog.catalog_url, deleteBaseUrl=catalog.delete_base_url,
               removeEmptyParents=remove_empty_parents, urns=len(selected))
    counts = Counter()
    for entry in selected:
        urn = entry["urn"]
        try:
            status = clean_entry(catalog, entry, dump_root, remove_empty_parents,
                                 lambda action, **fields: audit.emit(action, urn=urn, **fields), pause)
        except CatalogError as error:
            audit.emit("URN_RESULT", urn=urn, status="FAILED", error=str(error))
            counts["FAILED"] += 1
            # Halt on service errors rather than applying the plan to more URNs.
            counts["NOT_PROCESSED"] = len(selected) - counts.total()
            break
        audit.emit("URN_RESULT", urn=urn, status=status)
        counts[status] += 1
    return counts, audit.finish(counts)


def read_audit(paths):
    rows = []
    for path in paths:
        with path.open(encoding="utf-8") as source:
            rows.extend(json.loads(line) for line in source if line.strip())
    return rows


def retry_parents(catalog, rows, log, pause=0):
    """Deletes the items an earlier run kept only because of dead links or a 409, once they are empty."""
    pending = kept_items(rows, catalog)

    audit = Audit(log)
    audit.emit("RUN_STARTED", command="parents", version=SCHEMA_VERSION, environment=catalog.environment,
               catalogUrl=catalog.catalog_url, deleteBaseUrl=catalog.delete_base_url, parents=len(pending))
    counts = Counter()
    for priref in sorted(pending, key=int):
        try:
            deleted = delete_empty_parent(catalog, priref, pending[priref], audit.emit, pause)
        except CatalogError as error:
            audit.emit("PARENT_RESULT", priref=priref, status="FAILED", error=str(error))
            counts["FAILED"] += 1
            counts["NOT_PROCESSED"] = len(pending) - counts.total()
            break
        counts["DELETED" if deleted else "KEPT"] += 1
    return counts, audit.finish(counts)


UNCHANGED_FIELDS = ("type", "objectNumber", "deleted", "urns", "size", "identity", "parents", "copiedFrom", "copiedTo",
                    "relatedObjects")


def kept_items(rows, catalog):
    """Items that earlier runs kept because of dead links or a 409, with what was expected of them."""
    for row in rows:
        if row.get("action") == "RUN_STARTED":
            check_endpoints(row, catalog, "Audit log")
    pending = {}
    for row in rows:
        if row.get("action") == "PARENT_KEPT" and row.get("retry"):
            pending[row["priref"]] = {key: row[key] for key in ("identity", "manifest", "itemParts")}
        elif row.get("action") in ("PARENT_KEPT", "DELETED"):
            pending.pop(row["priref"], None)
    if not pending:
        raise ValueError("The audit logs have no items left to retry")
    return pending


def unlink_item(catalog, priref, expected, dump_root, emit, pause):
    """Removes the dead item part links of one kept item. Returns True when the links were removed."""
    item, body = catalog.by_priref(priref, with_body=True)

    def keep(reason):
        emit("PARENT_KEPT", priref=priref, reason=reason, retry=False, **expected)
        return False

    reason = parent_problem(item, expected["identity"], expected["manifest"])
    if reason:
        return keep(reason)
    if item["children"] is None or not set(item["children"]) <= set(expected["itemParts"]):
        return keep("Item has other or unreadable item parts")
    if not item["children"]:
        emit("UNLINK_NOT_NEEDED", priref=priref)
        return False
    existing = [child for child in item["children"] if catalog.exists(child)]
    if existing:
        return keep(f"Listed item parts still exist: {existing}")

    dump = dump_root / f"item_{priref}.xml"
    write_synced(dump, body)
    if dump.read_bytes() != body:
        raise DumpError(f"{dump.name} could not be read back")
    emit("UNLINK_REQUESTED", priref=priref, deadLinks=item["children"], record=item,
         dump=f"{dump_root.name}/{dump.name}", sha256=sha256(body))
    catalog.clear_item_parts(priref)
    after = catalog.by_priref(priref)
    changed = [key for key in UNCHANGED_FIELDS if after.get(key) != item.get(key)]
    if after["children"] != [] or changed:
        # Stop the run: the update did something else than tested in stage
        emit("UNLINK_UNEXPECTED", priref=priref, children=after["children"], changedFields=changed)
        raise CatalogError(f"Item {priref} does not look as expected after the update")
    emit("UNLINKED", priref=priref, deadLinks=item["children"])
    if pause:
        time.sleep(pause)
    return True


def unlink_parents(catalog, rows, log, dump_root, pause=0):
    """Removes the dead item part links of the items earlier runs kept, so the parents command can delete them."""
    pending = kept_items(rows, catalog)
    dump_root.mkdir(parents=True)
    audit = Audit(log)
    audit.emit("RUN_STARTED", command="unlink", version=SCHEMA_VERSION, environment=catalog.environment,
               catalogUrl=catalog.catalog_url, deleteBaseUrl=catalog.delete_base_url, parents=len(pending))
    counts = Counter()
    for priref in sorted(pending, key=int):
        try:
            unlinked = unlink_item(catalog, priref, pending[priref], dump_root, audit.emit, pause)
        except CatalogError as error:
            audit.emit("PARENT_RESULT", priref=priref, status="FAILED", error=str(error))
            counts["FAILED"] += 1
            counts["NOT_PROCESSED"] = len(pending) - counts.total()
            break
        counts["UNLINKED" if unlinked else "NOT_UNLINKED"] += 1
    return counts, audit.finish(counts)


def report(counts, kept):
    print(json.dumps(dict(counts), sort_keys=True))
    if kept:
        print(f"Items kept: {len(kept)}")
        for parent in kept:
            print(f"  {parent['priref']}: {parent['reason']}")
        dead = sorted(int(parent["priref"]) for parent in kept if parent["retry"])
        if dead:
            print(f"Dead links for the catalogue team: prirefs {dead[0]}-{dead[-1]}. "
                  "Run the parents command with the audit logs once they are removed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True, choices=sorted(set(KNOWN_ENVIRONMENTS.values())))
    parser.add_argument("--catalog-url", required=True, help="Full wwwopac.ashx URL without a query string")
    parser.add_argument("--delete-base-url", help="Collections REST base URL, including /collections-rest")
    parser.add_argument("--request-interval", type=float, default=1.0,
                        help="Minimum seconds between catalog requests (default 1)")
    commands = parser.add_subparsers(dest="command", required=True)
    scan_command = commands.add_parser("scan", help="Read URNs, dump the records to change and save a JSON plan")
    scan_command.add_argument("urns", type=Path)
    scan_command.add_argument("plan", type=Path, help="New plan file; the dump goes to <plan name>.dump next to it")
    apply_command = commands.add_parser("apply", help="Apply a saved plan and write an NDJSON audit log")
    apply_command.add_argument("plan", type=Path)
    apply_command.add_argument("audit", type=Path)
    apply_command.add_argument("--remove-empty-parents", action="store_true")
    apply_command.add_argument("--only", action="append", metavar="URN", help="Apply only this URN (repeatable)")
    apply_command.add_argument("--limit", type=int, help="Apply at most this many URNs")
    parents_command = commands.add_parser("parents", help="Retry items kept because of dead links")
    parents_command.add_argument("audit", type=Path, nargs="+", help="Earlier audit logs, then the new audit log")
    unlink_command = commands.add_parser("unlink", help="Remove dead item part links from the kept items")
    unlink_command.add_argument("audit", type=Path, nargs="+", help="Earlier audit logs, then the new audit log")
    for command in (apply_command, parents_command, unlink_command):
        command.add_argument("--pause-seconds", type=float, default=2.0, help="Pause after each delete")
    args = parser.parse_args()
    oauth = tuple(os.environ.get(f"COLLECTIONS_{name}") for name in ("TOKEN_URL", "CLIENT_ID", "CLIENT_SECRET"))
    if any(oauth) and not all(oauth):
        raise ValueError("OAuth requires COLLECTIONS_TOKEN_URL, COLLECTIONS_CLIENT_ID and COLLECTIONS_CLIENT_SECRET")
    if args.request_interval < 0:
        raise ValueError("--request-interval must be zero or more")
    catalog = Catalog(args.catalog_url, args.delete_base_url, args.environment,
                      os.environ.get("COLLECTIONS_LOOKUP_TOKEN"), os.environ.get("COLLECTIONS_DELETE_TOKEN"),
                      oauth if all(oauth) else None, args.request_interval)
    if args.command == "scan":
        urns = read_urns(args.urns)
        dump_root = args.plan.with_name(f"{args.plan.stem}.dump")
        if args.plan.exists() or dump_root.exists():
            raise ValueError(f"{args.plan} or {dump_root} already exists; use a new plan filename")
        plan = scan(catalog, urns, dump_root)
        with args.plan.open("x", encoding="utf-8") as output:
            json.dump(plan, output, indent=2, ensure_ascii=False)
            output.write("\n")
        print(json.dumps(plan["counts"], sort_keys=True))
        print(f"Dump: {dump_root}")
        return int(plan["counts"].get("READY", 0) != len(urns))

    if args.command == "unlink":
        *sources, target = args.audit
        if not sources:
            raise ValueError("Give the earlier audit logs and then a new audit log to write")
        if args.pause_seconds < 0:
            raise ValueError("--pause-seconds must be zero or more")
        rows = read_audit(sources)
        dump_root = target.with_name(f"{target.stem}.dump")
        if target.exists() or dump_root.exists():
            raise ValueError(f"{target} or {dump_root} already exists; use a new audit filename")
        with target.open("x", encoding="utf-8") as audit:
            counts, kept = unlink_parents(catalog, rows, audit, dump_root, args.pause_seconds)
        report(counts, kept)
        return int(counts.get("UNLINKED", 0) != counts.total())

    if not catalog.delete_base_url or not (catalog.delete_token or catalog.oauth):
        raise ValueError(f"{args.command} requires --delete-base-url and Collections REST authentication")
    if args.pause_seconds < 0 or (args.command == "apply" and args.limit is not None and args.limit < 1):
        raise ValueError("--pause-seconds must be zero or more and --limit at least 1")
    if args.command == "parents":
        *sources, target = args.audit
        if not sources:
            raise ValueError("Give the earlier audit logs and then a new audit log to write")
        rows = read_audit(sources)
        with target.open("x", encoding="utf-8") as audit:
            counts, kept = retry_parents(catalog, rows, audit, args.pause_seconds)
        report(counts, kept)
        return int(counts.get("DELETED", 0) != counts.total())

    with args.plan.open(encoding="utf-8") as source:
        plan = json.load(source)
    dump_dir = plan.get("dumpDir")
    if not isinstance(dump_dir, str) or not dump_dir or Path(dump_dir).name != dump_dir:
        raise ValueError("Scan plan has no valid dump directory")
    with args.audit.open("x", encoding="utf-8") as audit:
        counts, kept = apply(catalog, plan, audit, args.plan.parent / dump_dir, args.remove_empty_parents,
                             args.pause_seconds, args.only, args.limit)
    report(counts, kept)
    return int(counts.get("VERIFIED", 0) != counts.total())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, CatalogError, DumpError) as error:
        sys.exit(f"Catalog cleanup: {error}")

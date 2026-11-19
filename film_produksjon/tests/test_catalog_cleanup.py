import importlib.util
import io
import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("catalog_cleanup", ROOT / "catalog-cleanup/cleanup.py")
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)

URN = "URN:NBN:no-nb_digifilm_815655_20130726"
OTHER_URN = "URN:NBN:no-nb_digifilm_999_20130802"
STAGE_LOOKUP = "https://api.collections.stage.nb.no/webapi_levendebilder/wwwopac.ashx"
STAGE_REST = "https://api.dev.produksjon.nb.no/collections-rest"
FIXTURES = ROOT / "examples/catalog_dump/digifilm_815655_20130726"
MANIFEST = ["100"]


def part(priref, size, parent=None, urns=None, **links):
    return {"priref": priref, "type": "ITEM_PART", "objectNumber": f"AV-{priref}", "size": size,
            "parents": [parent] if parent else [], "children": [],
            "identity": f"guid:{priref}", "urns": urns or [URN], "deleted": False,
            "copiedFrom": [], "copiedTo": [], "relatedObjects": [], **links}


def item(priref, children, manifest=MANIFEST, **fields):
    return {"priref": priref, "type": "ITEM", "objectNumber": f"AV-{priref}", "identity": f"guid:{priref}",
            "deleted": False, "parents": list(manifest), "children": list(children), "copiedTo": [],
            "relatedObjects": [], **fields}


class FakeCatalog:
    environment = "stage"
    catalog_url = STAGE_LOOKUP
    delete_base_url = STAGE_REST

    def __init__(self, records, parents=None):
        self.records = records
        self.parents = parents or {}
        self.events = []
        self.fail_delete = None
        self.fail_status = 502
        self.dead_links = False
        self.unlink_side_effect = None

    @staticmethod
    def body(prirefs):
        records = "".join(f'<record priref="{priref}"/>' for priref in prirefs)
        return f"<adlibXML><recordList>{records}</recordList></adlibXML>".encode()

    def by_urn(self, urn, with_body=False):
        self.events.append(("URN", urn))
        records = sorted((record.copy() for record in self.records if urn in record["urns"]),
                         key=lambda record: int(record["priref"]))
        return (records, self.body(record["priref"] for record in records)) if with_body else records

    def by_priref(self, priref, with_body=False):
        self.events.append(("PARENT", priref))
        parent = dict(self.parents[priref], children=list(self.parents[priref]["children"]))
        return (parent, self.body([priref])) if with_body else parent

    def exists(self, priref):
        self.events.append(("EXISTS", priref))
        return any(record["priref"] == priref for record in self.records) or priref in self.parents

    def clear_item_parts(self, priref):
        self.events.append(("UNLINK", priref))
        self.parents[priref]["children"] = []
        if self.unlink_side_effect:
            self.parents[priref].update(self.unlink_side_effect)

    def delete(self, priref):
        self.events.append(("DELETE", priref))
        if priref == self.fail_delete:
            raise cleanup.CatalogError(f"DELETE returned HTTP {self.fail_status}", self.fail_status)
        if self.parents.get(priref, {}).get("children"):
            # Collections REST refuses an item that lists item parts
            raise cleanup.CatalogError("DELETE returned HTTP 409", 409)
        self.records = [record for record in self.records if record["priref"] != priref]
        self.parents.pop(priref, None)
        if self.dead_links:
            return
        for parent in self.parents.values():
            if priref in parent["children"]:
                parent["children"] = [child for child in parent["children"] if child != priref]


def deletes(catalog):
    return [priref for action, priref in catalog.events if action == "DELETE"]


class CatalogCleanupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.scans = 0

    def scan(self, catalog, urns):
        self.scans += 1
        return cleanup.scan(catalog, urns, Path(self.tmp.name) / f"plan{self.scans}.dump")

    def dump_root(self, plan):
        return Path(self.tmp.name) / plan["dumpDir"]

    def apply(self, catalog, plan, remove_empty_parents=False, **options):
        log = io.StringIO()
        counts, _ = cleanup.apply(catalog, plan, log, self.dump_root(plan), remove_empty_parents, **options)
        return counts, [json.loads(line) for line in log.getvalue().splitlines()]

    def retry(self, catalog, rows):
        log = io.StringIO()
        counts, kept = cleanup.retry_parents(catalog, rows, log)
        return counts, kept, [json.loads(line) for line in log.getvalue().splitlines()]

    def test_scan_reads_the_captured_catalog_responses_and_dumps_them(self):
        responses = {
            "PID_data_URN": FIXTURES / f"digifilm_815655_20130726_DIGITAL_ITEM_PART_70398020_162311522.xml",
            "162311521": FIXTURES / "digifilm_815655_20130726_DIGITAL_ITEM_162311521.xml",
            "70398019": FIXTURES / "digifilm_815655_20130726_DIGITAL_ITEM_70398019.xml",
        }

        def respond(method, url, *args):
            self.assertEqual(method, "GET")
            return next(path.read_bytes() for key, path in responses.items() if key in url)

        catalog = cleanup.Catalog(STAGE_LOOKUP, STAGE_REST, "stage")
        catalog.request = Mock(side_effect=respond)
        plan = self.scan(catalog, [URN])
        self.assertEqual(plan["counts"], {"CLEANUP": 1})
        entry = plan["entries"][0]
        self.assertEqual((entry["primary"], entry["delete"]), ("162311522", ["70398020"]))
        duplicate = entry["records"][0]
        self.assertEqual(duplicate["parents"], ["70398019"])
        self.assertEqual(duplicate["copiedFrom"], ["69119828", "69119830"])
        self.assertEqual((duplicate["objectNumber"], duplicate["copiedTo"]), ("AV-70398020", []))
        self.assertEqual(entry["primaryItem"], {"priref": "162311521", "partOf": ["25815655"]})
        self.assertEqual(list(entry["items"]), ["70398019"])

        folder = self.dump_root(plan) / "digifilm_815655_20130726"
        self.assertEqual(sorted(path.name for path in folder.iterdir()),
                         ["index.json", "item_70398019.xml", "urn.xml"])
        self.assertEqual((folder / "urn.xml").read_bytes(), responses["PID_data_URN"].read_bytes())
        self.assertEqual((folder / "item_70398019.xml").read_bytes(), responses["70398019"].read_bytes())
        index = json.loads((folder / "index.json").read_text())
        self.assertEqual((index["primary"], index["duplicates"], index["items"]),
                         ("162311522", ["70398020"], ["70398019"]))
        self.assertEqual(index["links"]["70398020"], {"partOf": ["70398019"], "copiedFrom": ["69119828", "69119830"]})
        self.assertEqual(index["links"]["70398019"], {"partOf": ["25815655"], "parts": ["70398020"]})
        self.assertEqual(entry["dump"]["path"], folder.name)

    def test_only_exact_zero_with_one_positive_primary_is_eligible(self):
        for records, status in [
            ([], "MISSING"),
            ([part("1", 0)], "REVIEW_PRIMARY"),
            ([part("1", None)], "REVIEW_PRIMARY"),
            ([part("1", 0), part("2", 0)], "REVIEW_PRIMARY"),
            ([part("1", 1), part("2", 2), part("3", 0)], "REVIEW_PRIMARY"),
            ([part("1", 1), part("2", None), part("3", 0)], "REVIEW_SIZE"),
            ([part("1", 1), part("2", 0, urns=[URN, OTHER_URN])], "REVIEW_SHARED_URN"),
            ([part("1", 1), dict(part("2", 0), type="ITEM")], "REVIEW_TYPE"),
            ([part("1", 1), part("2", 0, copiedTo=["9"])], "REVIEW_LINKED"),
            ([part("1", 1), part("2", 0, relatedObjects=["9"])], "REVIEW_LINKED"),
            ([part("1", 1), part("2", 0, relatedObjects=None)], "REVIEW_LINKED"),
            ([part("1", 1, copiedFrom=["2"]), part("2", 0, copiedTo=["1"])], "REVIEW_LINKED"),
            ([part("1", 1)], "READY"),
        ]:
            with self.subTest(status=status, records=records):
                catalog = FakeCatalog(records)
                plan = self.scan(catalog, [URN])
                self.assertEqual(plan["entries"][0]["status"], status)
                self.assertEqual(plan["entries"][0]["delete"], [])
                self.assertEqual(list(self.dump_root(plan).iterdir()), [])
                catalog.events.clear()
                counts, _ = self.apply(catalog, plan)
                self.assertEqual(counts, {})
                self.assertEqual(catalog.events, [], "apply leaves entries without duplicates alone")

    def test_delete_parts_then_the_empty_items_then_verify_the_urn(self):
        catalog = FakeCatalog([part("1", 1, "10", copiedFrom=["9"]), part("2", 0, "11", copiedFrom=["9"]),
                               part("3", 0, "11"), part("4", 0, "10"), part("5", 0, "12")], {
            "10": item("10", ["1", "4"]), "11": item("11", ["2", "3"]), "12": item("12", ["5", "6"]),
        })
        plan = self.scan(catalog, [URN])
        entry = plan["entries"][0]
        self.assertEqual(list(entry["items"]), ["11"])
        self.assertEqual(entry["itemsKept"], [{"priref": "12", "reason": "Item has other or unreadable item parts"}])
        catalog.events.clear()

        counts, entries = self.apply(catalog, plan, remove_empty_parents=True)
        self.assertEqual(counts, {"VERIFIED": 1})
        self.assertEqual(catalog.events, [("DELETE", "2"), ("DELETE", "3"), ("DELETE", "4"), ("DELETE", "5"),
                                          ("DELETE", "11"), ("URN", URN)], "no lookups before the deletes")
        self.assertEqual(set(catalog.parents), {"10", "12"})
        self.assertEqual(entries[-1]["parentsKept"],
                         [{"priref": "12", "reason": "Item has other or unreadable item parts", "retry": False}])
        requested = [row for row in entries if row["action"] == "DELETE_REQUESTED"]
        self.assertEqual(requested[0]["record"]["copiedFrom"], ["9"])
        self.assertEqual(requested[0]["dump"], "digifilm_815655_20130726/urn.xml")
        self.assertEqual(requested[-1]["dump"], "digifilm_815655_20130726/item_11.xml")
        self.assertEqual(requested[-1]["record"]["manifest"], MANIFEST)

    def test_items_are_only_deleted_when_asked(self):
        catalog = FakeCatalog([part("1", 1, "10"), part("2", 0, "11")], {"10": item("10", ["1"]), "11": item("11", ["2"])})
        counts, _ = self.apply(catalog, self.scan(catalog, [URN]))
        self.assertEqual(counts, {"VERIFIED": 1})
        self.assertEqual(deletes(catalog), ["2"])

    def test_items_under_another_manifestation_or_with_links_are_kept(self):
        for parent in (item("11", ["2"], manifest=["200"]), item("11", ["2"], copiedTo=["9"]),
                       item("11", ["2"], relatedObjects=["9"])):
            with self.subTest(parent=parent):
                catalog = FakeCatalog([part("1", 1, "10"), part("2", 0, "11")], {"10": item("10", ["1"]), "11": parent})
                plan = self.scan(catalog, [URN])
                self.assertEqual(plan["entries"][0]["items"], {})
                counts, entries = self.apply(catalog, plan, remove_empty_parents=True)
                self.assertEqual((counts, deletes(catalog)), ({"VERIFIED": 1}, ["2"]))
                self.assertFalse(entries[-1]["parentsKept"][0]["retry"])

    def test_dead_links_keep_the_item_until_the_retry(self):
        catalog = FakeCatalog([part("1", 1, "10"), part("2", 0, "11")], {"10": item("10", ["1"]), "11": item("11", ["2"])})
        catalog.dead_links = True
        counts, entries = self.apply(catalog, self.scan(catalog, [URN]), remove_empty_parents=True)
        self.assertEqual(counts, {"VERIFIED": 1})
        self.assertEqual(entries[-1]["parentsKept"], [{"priref": "11", "reason": cleanup.REFUSED_ITEM, "retry": True}])
        self.assertIn("11", catalog.parents)

        counts, kept, _ = self.retry(catalog, entries)
        self.assertEqual((counts, kept[0]["reason"]), ({"KEPT": 1}, cleanup.DEAD_LINKS))
        catalog.parents["11"]["children"] = []  # the catalogue team removed the dead link
        counts, kept, rows = self.retry(catalog, entries)
        self.assertEqual((counts, kept), ({"DELETED": 1}, []))
        self.assertNotIn("11", catalog.parents)
        with self.assertRaisesRegex(ValueError, "no items left"):
            self.retry(catalog, entries + rows)

    def unlink(self, catalog, rows):
        log = io.StringIO()
        self.scans += 1
        counts, kept = cleanup.unlink_parents(catalog, rows, log, Path(self.tmp.name) / f"unlink{self.scans}.dump")
        return counts, kept, [json.loads(line) for line in log.getvalue().splitlines()]

    def dead_link_run(self):
        catalog = FakeCatalog([part("1", 1, "10"), part("2", 0, "11"), part("3", 0, "12", urns=[OTHER_URN]),
                               part("4", 1, "13", urns=[OTHER_URN])],
                              {"10": item("10", ["1"]), "11": item("11", ["2"]), "12": item("12", ["3"]),
                               "13": item("13", ["4"])})
        catalog.dead_links = True
        _, entries = self.apply(catalog, self.scan(catalog, [URN, OTHER_URN]), remove_empty_parents=True)
        catalog.events.clear()
        return catalog, entries

    def test_unlink_removes_dead_links_so_parents_can_delete_the_items(self):
        catalog, entries = self.dead_link_run()
        counts, kept, rows = self.unlink(catalog, entries)
        self.assertEqual((counts, kept), ({"UNLINKED": 2}, []))
        self.assertEqual([event for event in catalog.events if event[0] in ("UNLINK", "DELETE")],
                         [("UNLINK", "11"), ("UNLINK", "12")])
        requested = [row for row in rows if row["action"] == "UNLINK_REQUESTED"]
        self.assertEqual([row["deadLinks"] for row in requested], [["2"], ["3"]])
        dump = Path(self.tmp.name) / requested[0]["dump"]
        self.assertEqual(dump.read_bytes(), FakeCatalog.body(["11"]))

        counts, kept, _ = self.retry(catalog, entries + rows)
        self.assertEqual((counts, kept), ({"DELETED": 2}, []))
        self.assertEqual(set(catalog.parents), {"10", "13"})

    def test_unlink_keeps_an_item_whose_listed_part_exists_or_that_got_another_part(self):
        catalog, entries = self.dead_link_run()
        catalog.records.append(part("2", 0, "11"))        # the deleted part is back (or its priref was reused)
        catalog.parents["12"]["children"] = ["3", "7"]   # a new item part was added
        counts, kept, rows = self.unlink(catalog, entries)
        self.assertEqual(counts, {"NOT_UNLINKED": 2})
        self.assertEqual([row["retry"] for row in kept], [False, False])
        self.assertFalse(any(event[0] == "UNLINK" for event in catalog.events))

    def test_unlink_stops_when_the_update_changes_anything_else(self):
        catalog, entries = self.dead_link_run()
        catalog.unlink_side_effect = {"parents": ["999"]}
        counts, _, rows = self.unlink(catalog, entries)
        self.assertEqual(counts, {"FAILED": 1, "NOT_PROCESSED": 1})
        self.assertEqual([row["changedFields"] for row in rows if row["action"] == "UNLINK_UNEXPECTED"], [["parents"]])
        self.assertEqual([event for event in catalog.events if event[0] == "UNLINK"], [("UNLINK", "11")])

    def test_clear_item_parts_sends_one_updaterecord_with_an_empty_parts_field(self):
        catalog = cleanup.Catalog(STAGE_LOOKUP, STAGE_REST, "stage", lookup_token="demo")
        catalog.request = Mock(return_value=b'{"adlibJSON": {"diagnostic": {"hits": 1}}}')
        catalog.clear_item_parts("70941499")
        method, url, token, accept, status, body = catalog.request.call_args.args
        self.assertEqual((method, token, status), ("POST", "demo", 200))
        self.assertTrue(url.startswith(STAGE_LOOKUP + "?database=catalogue&command=updaterecord&output=json"))
        self.assertEqual(json.loads(body), {"adlibJSON": {"recordList": [
            {"priref": 70941499, "record_type": "ITEM", "parts.reference.lref": ""}]}})
        self.assertEqual(catalog.request.call_args.kwargs, {"content_type": "application/json"})
        catalog.request = Mock(return_value=b'{"adlibJSON": {"diagnostic": {"error": {"message": "locked"}}}}')
        with self.assertRaisesRegex(cleanup.CatalogError, "refused"):
            catalog.clear_item_parts("70941499")
        with self.assertRaisesRegex(cleanup.CatalogError, "Invalid"):
            catalog.clear_item_parts("1 or 2")

    def test_retry_keeps_an_item_that_got_a_new_item_part(self):
        catalog = FakeCatalog([part("1", 1, "10"), part("2", 0, "11")], {"10": item("10", ["1"]), "11": item("11", ["2"])})
        catalog.dead_links = True
        _, entries = self.apply(catalog, self.scan(catalog, [URN]), remove_empty_parents=True)
        catalog.parents["11"]["children"] = ["2", "7"]
        catalog.events.clear()
        counts, kept, _ = self.retry(catalog, entries)
        self.assertEqual((counts, kept[0]["retry"]), ({"KEPT": 1}, False))
        self.assertEqual(deletes(catalog), [])

    def test_a_missing_or_altered_dump_stops_the_deletion(self):
        for damage in ("urn.xml", "index.json", "remove"):
            with self.subTest(damage=damage):
                catalog = FakeCatalog([part("1", 1, "10"), part("2", 0, "11")],
                                      {"10": item("10", ["1"]), "11": item("11", ["2"])})
                plan = self.scan(catalog, [URN])
                folder = self.dump_root(plan) / "digifilm_815655_20130726"
                if damage == "remove":
                    (folder / "item_11.xml").unlink()
                else:
                    (folder / damage).write_bytes((folder / damage).read_bytes() + b" ")
                counts, entries = self.apply(catalog, plan, remove_empty_parents=True)
                self.assertEqual(counts, {"DUMP_INVALID": 1})
                self.assertEqual(deletes(catalog), [])
                self.assertTrue(any(row["action"] == "DUMP_INVALID" for row in entries))

    def test_dump_must_hold_the_records_it_names(self):
        catalog = FakeCatalog([part("1", 1), part("2", 0)])
        catalog.body = staticmethod(lambda prirefs: FakeCatalog.body(["1"]))  # a response without the duplicate
        with self.assertRaisesRegex(cleanup.DumpError, "expected records"):
            self.scan(catalog, [URN])

    def test_changed_primary_is_reported_after_cleanup(self):
        catalog = FakeCatalog([part("1", 1), part("2", 0)])
        plan = self.scan(catalog, [URN])
        catalog.records[0]["size"] = 10
        counts, _ = self.apply(catalog, plan)
        self.assertEqual(counts, {"REVIEW_AFTER_CLEANUP": 1})

    def test_interrupted_api_records_the_attempt_and_prior_confirmed_deletes(self):
        catalog = FakeCatalog([part("1", 1), part("2", 0), part("3", 0),
                               part("4", 1, urns=[OTHER_URN]), part("5", 0, urns=[OTHER_URN])])
        catalog.fail_delete = "3"
        counts, entries = self.apply(catalog, self.scan(catalog, [URN, OTHER_URN]))
        self.assertEqual(counts, {"FAILED": 1, "NOT_PROCESSED": 1})
        self.assertEqual([row["priref"] for row in entries if row["action"] == "DELETED"], ["2"])
        self.assertEqual([row["priref"] for row in entries if row["action"] == "DELETE_REQUESTED"], ["2", "3"])
        self.assertNotIn("5", deletes(catalog))

    def test_only_and_limit_select_urns(self):
        records = [part("1", 1), part("2", 0), part("3", 1, urns=[OTHER_URN]), part("4", 0, urns=[OTHER_URN])]
        plan = self.scan(FakeCatalog(records), [URN, OTHER_URN])
        catalog = FakeCatalog(records)
        self.assertEqual(self.apply(catalog, plan, only=[OTHER_URN])[0], {"VERIFIED": 1})
        self.assertEqual(deletes(catalog), ["4"])
        catalog = FakeCatalog(records)
        self.assertEqual(self.apply(catalog, plan, limit=1)[0], {"VERIFIED": 1})
        self.assertEqual(deletes(catalog), ["2"])
        with self.assertRaisesRegex(ValueError, "not a CLEANUP entry"):
            self.apply(FakeCatalog(records), plan, only=["URN:NBN:no-nb_digifilm_1_2"])

    def test_incomplete_response_and_mixed_environments_are_rejected(self):
        catalog = cleanup.Catalog(STAGE_LOOKUP, STAGE_REST, "stage")
        catalog.request = Mock(return_value=b"<adlibXML><recordList/><diagnostic><hits>3</hits></diagnostic></adlibXML>")
        self.assertEqual(self.scan(catalog, [URN])["counts"], {"LOOKUP_FAILED": 1})
        with self.assertRaisesRegex(ValueError, "not a known stage"):
            cleanup.Catalog(STAGE_LOOKUP, "https://api.produksjon.nb.no/collections-rest", "stage")
        with self.assertRaisesRegex(ValueError, "not a known prod"):
            cleanup.Catalog(STAGE_LOOKUP, None, "prod")
        with self.assertRaisesRegex(ValueError, "not a known stage"):
            cleanup.Catalog("https://catalog.example/wwwopac.ashx", STAGE_REST, "stage")
        plan = self.scan(FakeCatalog([part("1", 1)]), [URN])
        prod = cleanup.Catalog("https://api.produksjon.nb.no/wwwopac.ashx", None, "prod")
        with self.assertRaisesRegex(ValueError, "must match"):
            cleanup.apply(prod, plan, io.StringIO(), self.dump_root(plan))

    def test_oauth_reuses_token_until_near_expiry(self):
        catalog = cleanup.Catalog(STAGE_LOOKUP, STAGE_REST, "stage",
                                  oauth=("https://auth.example/token", "client", "secret"))
        catalog.request = Mock(return_value=b'{"access_token":"demo","expires_in":300}')
        self.assertEqual(catalog.token(None, catalog.catalog_url), "demo")
        self.assertEqual(catalog.token(None, catalog.catalog_url), "demo")
        self.assertEqual(catalog.request.call_count, 1)
        catalog.expires_at = 0
        self.assertEqual(catalog.token(None, catalog.catalog_url), "demo")
        self.assertEqual(catalog.request.call_count, 2)

    def test_catalog_requests_are_spaced_by_the_interval(self):
        catalog = cleanup.Catalog(STAGE_LOOKUP, STAGE_REST, "stage", delete_token="demo", request_interval=1.5)
        catalog.request = Mock(return_value=b"<adlibXML><recordList/><diagnostic><hits>0</hits></diagnostic></adlibXML>")
        clock = iter([10.0, 10.5, 11.5, 20.0, 20.0])
        with unittest.mock.patch.object(cleanup.time, "monotonic", lambda: next(clock)), \
                unittest.mock.patch.object(cleanup.time, "sleep") as sleep:
            catalog.by_urn(URN)      # first request: no wait
            catalog.by_urn(URN)      # 0.5 s later: waits the remaining 1.0 s
            catalog.delete("2")      # long after: no wait
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1.0])
        self.assertEqual(catalog.request.call_count, 3)

    def test_input_rejects_duplicate_or_malformed_urns(self):
        path = Path(self.tmp.name) / "urns.csv"
        path.write_text(f"urn\n{URN}\n{OTHER_URN}\n", encoding="utf-8")
        self.assertEqual(cleanup.read_urns(path), [URN, OTHER_URN])
        path.write_text(f"urn\n{URN}\n{URN}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Repeated URN"):
            cleanup.read_urns(path)
        path.write_text("urn\ninvalid\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Invalid production URN"):
            cleanup.read_urns(path)


if __name__ == "__main__":
    unittest.main()

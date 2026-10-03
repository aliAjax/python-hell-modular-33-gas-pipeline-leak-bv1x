import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import DomainError


class OfflineMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.repo.upsert_region({"code": "EAST", "name": "东片区", "capacity": 2})
        self.repo.upsert_segment({"segment_id": "S-E1", "region_code": "EAST"})
        self.repo.upsert_valve({"valve_id": "V-1", "region_code": "EAST"})
        self.repo.upsert_valve({"valve_id": "V-2", "region_code": "EAST"})
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _verified_item(self):
        item = self.service.create_item({
            "pipeline_id": "P-1", "segment_id": "S-E1",
            "reported_at": "2026-10-03T08:00:00+00:00",
            "pressure_drop_kpa": 20, "sensor_value_ppm": 50, "odor_reports": 1,
            "reporter": "d",
        }, "d", "dispatcher", region="EAST")
        return self.service.act(item["id"], "verify", {"field_confirmed": True}, "r", "responder", item["version"])

    def test_same_position_merges_without_conflict(self):
        outcome = self.service.merge_field_readings({"records": [
            {"client_id": "a", "valve_id": "V-1", "position": "closed",
             "observed_at": "2026-10-03T09:00:00+00:00", "device_id": "d1"},
            {"client_id": "b", "valve_id": "V-1", "position": "closed",
             "observed_at": "2026-10-03T09:01:00+00:00", "device_id": "d2"},
        ]}, "patrol-1", "patrol")
        self.assertEqual({r["status"] for r in outcome["results"]}, {"merged"})
        self.assertEqual(outcome["conflicts_opened"], [])
        self.assertEqual(self.service.conflicts("open"), [])

    def test_conflicting_readings_listed_side_by_side_and_block(self):
        outcome = self.service.merge_field_readings({"records": [
            {"client_id": "a", "valve_id": "V-1", "position": "closed",
             "observed_at": "2026-10-03T09:00:00+00:00", "device_id": "d1"},
            {"client_id": "b", "valve_id": "V-1", "position": "open",
             "observed_at": "2026-10-03T09:05:00+00:00", "device_id": "d2"},
        ]}, "patrol-1", "patrol")
        statuses = {r["client_id"]: r["status"] for r in outcome["results"]}
        self.assertEqual(statuses["a"], "merged")
        self.assertEqual(statuses["b"], "conflict")
        conflict = self.service.conflicts("open")[0]
        # 两份记录并排列出
        self.assertEqual([r["client_id"] for r in conflict["records"]], ["a", "b"])
        self.assertEqual({r["position"] for r in conflict["records"]}, {"closed", "open"})

        # 不能直接推进隔离
        item = self._verified_item()
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "isolate", {"valve_sequence": ["V-1", "V-2"]},
                             "d", "dispatcher", item["version"], region="EAST")
        self.assertEqual(context.exception.code, "valve_status_conflict")

        # 重放/重复回传按 client_id 去重，不会新增记录或冲突
        replay = self.service.merge_field_readings({"records": [
            {"client_id": "a", "valve_id": "V-1", "position": "closed",
             "observed_at": "2026-10-03T09:00:00+00:00", "device_id": "d1"},
        ]}, "patrol-1", "patrol")
        self.assertEqual(replay["results"][0]["status"], "duplicate")
        self.assertEqual(len(self.service.conflicts("open")), 1)

        # 复核裁决后可推进
        self.service.resolve_conflict_direct(conflict["id"], {"resolution": "closed"}, "sup", "supervisor")
        item = self.service.get_item(item["id"])
        outcome = self.service.act(item["id"], "isolate", {"valve_sequence": ["V-1", "V-2"]},
                                   "d", "dispatcher", item["version"], region="EAST")
        self.assertEqual(outcome["decision"], "started")


if __name__ == "__main__":
    unittest.main()

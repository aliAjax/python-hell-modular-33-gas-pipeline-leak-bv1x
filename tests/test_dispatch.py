import itertools
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service, CommandGateway
from src.domain import DomainError


class ScriptGateway(CommandGateway):
    """按调用次序返回 acked/failed 的脚本网关。"""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        self._counter = itertools.count(1)

    def send(self, command):
        index = len(self.calls)
        self.calls.append((command["step"], command["valve_id"], command["attempts"]))
        result = self.outcomes[min(index, len(self.outcomes) - 1)]
        return "SCRIPT-%d" % next(self._counter), result, "boom" if result == "failed" else ""


class DispatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.repo.upsert_region({"code": "EAST", "name": "东片区", "capacity": 1})
        self.repo.upsert_region({"code": "WEST", "name": "西片区", "capacity": 1})
        self.repo.upsert_segment({"segment_id": "S-E1", "region_code": "EAST"})
        self.repo.upsert_segment({"segment_id": "S-E2", "region_code": "EAST"})
        self.repo.upsert_segment({"segment_id": "S-W1", "region_code": "WEST"})
        for valve, region_code, shared in (
            ("V-E1", "EAST", None), ("V-E2", "EAST", None),
            ("V-W1", "WEST", None), ("V-W2", "WEST", None),
            ("V-B1", "EAST", "WEST"),
        ):
            self.repo.upsert_valve({"valve_id": valve, "region_code": region_code, "shared_with": shared})
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _create_verified(self, segment, at, region=None, role="dispatcher", actor="d1"):
        payload = {
            "pipeline_id": "P-1", "segment_id": segment, "reported_at": at,
            "pressure_drop_kpa": 20, "sensor_value_ppm": 50, "odor_reports": 1,
            "reporter": actor,
        }
        item = self.service.create_item(payload, actor, role, region=region)
        return self.service.act(item["id"], "verify", {"field_confirmed": True}, "r1", "responder", item["version"])

    def test_dispatcher_cannot_open_other_region_order(self):
        with self.assertRaises(DomainError) as context:
            self.service.create_item({
                "pipeline_id": "P-1", "segment_id": "S-E1",
                "reported_at": "2026-10-01T08:00:00+00:00",
                "pressure_drop_kpa": 20, "sensor_value_ppm": 50, "odor_reports": 1,
                "reporter": "west-dispatch",
            }, "west-dispatch", "dispatcher", region="WEST")
        self.assertEqual(context.exception.code, "out_of_region")
        self.assertEqual(context.exception.status, 403)

    def test_dispatcher_cannot_run_other_region_action(self):
        item = self._create_verified("S-E1", "2026-10-01T08:00:00+00:00", region="EAST")
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "isolate", {"valve_sequence": ["V-E1", "V-E2"]},
                             "west-d", "dispatcher", item["version"], region="WEST")
        self.assertEqual(context.exception.code, "out_of_region")

    def test_cross_region_needs_center_confirmation(self):
        item = self._create_verified("S-W1", "2026-10-01T08:00:00+00:00", region="WEST")
        # 片区调度员开跨片区联合作业：越权
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "isolate", {"valve_sequence": ["V-W1", "V-B1"]},
                             "w-d", "dispatcher", item["version"], region="WEST")
        self.assertEqual(context.exception.code, "joint_confirmation_required")
        # 中心调度发起：先挂确认，不占容量
        outcome = self.service.act(item["id"], "isolate", {"valve_sequence": ["V-W1", "V-B1"]},
                                   "center", "regulator", item["version"])
        self.assertEqual(outcome["decision"], "joint_confirmation_required")
        self.assertEqual(self.service.get_item(item["id"])["status"], "verified")
        self.assertEqual([c["status"] for c in self.service.get_item(item["id"])["commands"]], [])
        state = self.service.state()
        west = next(r for r in state["regions"] if r["code"] == "WEST")
        self.assertEqual(west["active"], 0)
        # 中心确认后开始执行
        item = self.service.get_item(item["id"])
        outcome = self.service.act(item["id"], "joint_confirm", {}, "center", "regulator", item["version"])
        self.assertEqual(outcome["decision"], "started")
        self.assertEqual(self.service.get_item(item["id"])["status"], "isolated")

    def test_boundary_valve_busy_queues_both_sides(self):
        first = self._create_verified("S-E1", "2026-10-01T08:00:00+00:00", region="EAST")
        outcome = self.service.act(first["id"], "isolate", {"valve_sequence": ["V-E1", "V-B1"]},
                                   "e-d", "dispatcher", first["version"], region="EAST")
        self.assertEqual(outcome["decision"], "started")
        second = self._create_verified("S-W1", "2026-10-01T09:00:00+00:00", region="WEST")
        outcome = self.service.act(second["id"], "isolate", {"valve_sequence": ["V-W1", "V-B1"]},
                                   "center", "regulator", second["version"])
        self.assertEqual(outcome["decision"], "queued")
        self.assertEqual(outcome["reason"], "boundary_valve_busy")
        self.assertEqual(self.service.get_item(second["id"])["status"], "queued")

    def test_capacity_full_queues_and_auto_starts_after_release(self):
        first = self._create_verified("S-E1", "2026-10-01T08:00:00+00:00", region="EAST")
        self.service.act(first["id"], "isolate", {"valve_sequence": ["V-E1", "V-E2"]},
                         "e-d", "dispatcher", first["version"], region="EAST")
        second = self._create_verified("S-E2", "2026-10-01T09:00:00+00:00", region="EAST")
        outcome = self.service.act(second["id"], "isolate", {"valve_sequence": ["V-E1", "V-E2"]},
                                   "e-d", "dispatcher", second["version"], region="EAST")
        self.assertEqual(outcome["decision"], "queued")
        self.assertEqual(outcome["reason"], "region_capacity_full")
        queue = self.service.queue()
        self.assertEqual([q["id"] for q in queue], [second["id"]])
        # 第一单走修复-试压-恢复，容量释放瞬间排队单 FIFO 自动接上并执行完毕
        first = self.service.get_item(first["id"])
        first = self.service.act(first["id"], "repair", {"work_order": "WO-1"}, "t", "technician", first["version"])
        first = self.service.act(first["id"], "pressure_test",
                                 {"test_passed": True, "pressure_kpa": 150, "minimum_pressure_kpa": 100},
                                 "t", "technician", first["version"])
        self.service.act(first["id"], "restore", {"hazards_clear": True},
                         "s", "supervisor", first["version"], region="EAST")
        self.assertEqual(self.service.get_item(second["id"])["status"], "isolated")
        self.assertEqual(self.service.queue(), [])

    def test_open_conflict_blocks_admission_and_resolution_promotes(self):
        first = self._create_verified("S-E1", "2026-10-01T08:00:00+00:00", region="EAST")
        self.service.act(first["id"], "isolate", {"valve_sequence": ["V-E1", "V-E2"]},
                         "e-d", "dispatcher", first["version"], region="EAST")
        # 第二单先因容量排队
        second = self._create_verified("S-E2", "2026-10-01T09:00:00+00:00", region="EAST")
        self.service.act(second["id"], "isolate", {"valve_sequence": ["V-E1", "V-E2"]},
                         "e-d", "dispatcher", second["version"], region="EAST")
        # 现场回传冲突阀位
        self.service.merge_field_readings({"records": [
            {"client_id": "c-1", "valve_id": "V-E1", "position": "closed",
             "observed_at": "2026-10-01T10:00:00+00:00", "device_id": "dev-1"},
            {"client_id": "c-2", "valve_id": "V-E1", "position": "open",
             "observed_at": "2026-10-01T10:05:00+00:00", "device_id": "dev-2"},
        ]}, "patrol-1", "patrol")
        first = self.service.get_item(first["id"])
        first = self.service.act(first["id"], "repair", {"work_order": "WO-1"}, "t", "technician", first["version"])
        first = self.service.act(first["id"], "pressure_test",
                                 {"test_passed": True, "pressure_kpa": 150, "minimum_pressure_kpa": 100},
                                 "t", "technician", first["version"])
        self.service.act(first["id"], "restore", {"hazards_clear": True},
                         "s", "supervisor", first["version"], region="EAST")
        # 有未复核冲突，排队单仍不能准入
        self.assertEqual(self.service.get_item(second["id"])["status"], "queued")
        conflict = self.service.conflicts("open")[0]
        # 直接申请也被挡
        fresh = self.service.get_item(second["id"])
        with self.assertRaises(DomainError) as context:
            self.service.act(second["id"], "isolate", {"valve_sequence": ["V-E1", "V-E2"]},
                             "e-d", "dispatcher", fresh["version"], region="EAST")
        self.assertEqual(context.exception.code, "valve_status_conflict")
        # 复核裁决后自动放行
        self.service.resolve_conflict_direct(conflict["id"], {"resolution": "closed"}, "sup", "supervisor")
        self.assertEqual(self.service.get_item(second["id"])["status"], "isolated")


if __name__ == "__main__":
    unittest.main()

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
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        self._counter = itertools.count(1)

    def send(self, command):
        index = len(self.calls)
        self.calls.append((command["step"], command["valve_id"], command["attempts"]))
        result = self.outcomes[min(index, len(self.outcomes) - 1)]
        return "SCRIPT-%d" % next(self._counter), result, "boom" if result == "failed" else ""


class CommandPersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.repo.upsert_region({"code": "EAST", "name": "东片区", "capacity": 2})
        self.repo.upsert_segment({"segment_id": "S-E1", "region_code": "EAST"})
        for valve in ("V-1", "V-2", "V-3"):
            self.repo.upsert_valve({"valve_id": valve, "region_code": "EAST"})
        self.gateway = ScriptGateway(["failed"])
        self.service = Service(self.repo, self.gateway)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _verified_item(self, at="2026-10-02T08:00:00+00:00"):
        item = self.service.create_item({
            "pipeline_id": "P-9", "segment_id": "S-E1", "reported_at": at,
            "pressure_drop_kpa": 20, "sensor_value_ppm": 50, "odor_reports": 1,
            "reporter": "d",
        }, "d", "dispatcher", region="EAST")
        return self.service.act(item["id"], "verify", {"field_confirmed": True}, "r", "responder", item["version"])

    def test_failed_command_retries_from_breakpoint(self):
        item = self._verified_item()
        outcome = self.service.act(item["id"], "isolate", {"valve_sequence": ["V-1", "V-2", "V-3"]},
                                   "d", "dispatcher", item["version"], region="EAST")
        self.assertEqual(outcome["decision"], "started")
        self.assertEqual(outcome["dispatched"]["step"], 0)
        self.assertEqual(outcome["dispatched"]["result"], "failed")
        item = self.service.get_item(item["id"])
        self.assertEqual(item["status"], "isolating")
        commands = item["commands"]
        self.assertEqual([c["status"] for c in commands], ["failed", "pending", "pending"])
        self.assertEqual(commands[0]["attempts"], 1)
        # 后两道指令未下发
        self.gateway.outcomes = ["acked", "acked", "acked"]
        outcome = self.service.retry_command(item["id"], "d", "dispatcher", region="EAST")
        # 从断点（第一道）重试，成功后顺序推进后续指令
        self.assertEqual(outcome["dispatched"]["step"], 0)
        self.assertEqual(outcome["dispatched"]["attempt"], 2)
        self.assertEqual([call[0] for call in self.gateway.calls], [0, 0, 1, 2])
        self.assertTrue(outcome["next"]["next"]["sequence_completed"])
        item = self.service.get_item(item["id"])
        self.assertEqual(item["status"], "isolated")
        self.assertEqual([c["status"] for c in item["commands"]], ["acked", "acked", "acked"])

    def test_duplicate_receipt_does_not_reexecute(self):
        item = self._verified_item()
        outcome = self.service.act(item["id"], "isolate", {"valve_sequence": ["V-1", "V-2"]},
                                   "d", "dispatcher", item["version"], region="EAST")
        # 首道失败
        self.assertEqual(outcome["dispatched"]["result"], "failed")
        command = self.service.get_item(item["id"])["commands"][0]
        calls_before = len(self.gateway.calls)
        # 同编号回执重复上报：幂等，不再执行
        first = self.service.command_receipt(item["id"], command["id"],
                                             {"receipt_id": command["receipt_id"], "result": "acked"})
        self.assertTrue(first["duplicate"])
        second = self.service.command_receipt(item["id"], command["id"],
                                              {"receipt_id": command["receipt_id"], "result": "acked"})
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(self.gateway.calls), calls_before)
        self.assertEqual(self.service.get_command(command["id"])["status"], "failed")

    def test_restart_resumes_capacity_conflicts_and_unfinished_commands(self):
        item = self._verified_item()
        self.service.act(item["id"], "isolate", {"valve_sequence": ["V-1", "V-2"]},
                         "d", "dispatcher", item["version"], region="EAST")
        self.assertEqual(self.service.get_item(item["id"])["status"], "isolating")
        # 待复核记录
        self.service.merge_field_readings({"records": [
            {"client_id": "r-1", "valve_id": "V-1", "position": "closed",
             "observed_at": "2026-10-02T09:00:00+00:00", "device_id": "dev-1"},
            {"client_id": "r-2", "valve_id": "V-1", "position": "50%",
             "observed_at": "2026-10-02T09:01:00+00:00", "device_id": "dev-2"},
        ]}, "patrol-1", "patrol")

        # 关掉服务再启动：用同一数据库文件重建仓储与服务
        del self.service
        del self.repo
        new_repo = Repository(self.tmp.name)
        new_gateway = ScriptGateway(["acked", "acked"])
        new_service = Service(new_repo, new_gateway)
        resume = new_service.resume_on_startup()
        # 容量占用照旧：失败的指令仍是断点，状态里能指回来
        unfinished = [r for r in resume["resumed"] if r["item_id"] == item["id"]]
        self.assertEqual(unfinished[0]["action"], "await_retry")
        state = new_service.state()
        east = next(r for r in state["regions"] if r["code"] == "EAST")
        self.assertEqual(east["active"], 1)
        self.assertEqual(len(state["open_conflicts"]), 1)
        # 未完成指令照旧接得上：断点重试后跑完
        outcome = new_service.retry_command(item["id"], "d", "dispatcher", region="EAST")
        self.assertEqual(outcome["dispatched"]["result"], "acked")
        self.assertEqual(new_service.get_item(item["id"])["status"], "isolated")
        self.assertEqual(len(new_service.conflicts("open")), 1)


if __name__ == "__main__":
    unittest.main()

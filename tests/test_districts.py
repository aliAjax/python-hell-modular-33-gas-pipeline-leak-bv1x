import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


class DistrictIsolationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        self._setup_ledger()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _setup_ledger(self):
        # D-1 capacity 2, D-2 capacity 1
        self.service.register_district({"code": "D-1", "name": "城东", "capacity": 2}, "c", "center")
        self.service.register_district({"code": "D-2", "name": "城西", "capacity": 1}, "c", "center")
        self.service.register_segment({"code": "S-1", "district_code": "D-1", "pipeline_id": "P-1"}, "c", "center")
        self.service.register_segment({"code": "S-2", "district_code": "D-2", "pipeline_id": "P-2"}, "c", "center")
        self.service.register_valve({"code": "V-1", "name": "阀1", "district_code": "D-1"}, "c", "center")
        self.service.register_valve(
            {"code": "V-2", "name": "边界阀", "district_code": "D-1", "is_boundary": True, "shared_with": ["D-2"]},
            "c", "center",
        )
        self.service.register_valve({"code": "V-3", "name": "阀3", "district_code": "D-2"}, "c", "center")
        self.service.register_valve({"code": "V-4", "name": "阀4", "district_code": "D-1"}, "c", "center")

    def _position(self, valve, position, source, at):
        return {"valve_code": valve, "position": position, "recorded_at": at, "source": source}

    def test_dispatcher_only_own_district(self):
        # D-1 调度员开本片区 S-1 的单 -> 成功
        task = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1"]}, "d1", "dispatcher", region="D-1"
        )
        self.assertEqual(task["district_code"], "D-1")
        self.assertEqual(task["status"], "active")
        # D-1 调度员替 D-2 的 S-2 开单 -> 越权
        with self.assertRaises(DomainError) as context:
            self.service.create_isolation_task(
                {"segment_code": "S-2", "valve_codes": ["V-3"]}, "d1", "dispatcher", region="D-1"
            )
        self.assertEqual(context.exception.code, "region_overstep")
        self.assertEqual(context.exception.status, 403)

    def test_capacity_queue_and_promote(self):
        # D-1 容量 2：前两个 active，第三个 queued
        t1 = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1"]}, "a", "dispatcher", region="D-1"
        )
        t2 = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1"]}, "b", "dispatcher", region="D-1"
        )
        t3 = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1"]}, "c", "dispatcher", region="D-1"
        )
        self.assertEqual(t1["status"], "active")
        self.assertEqual(t2["status"], "active")
        self.assertEqual(t3["status"], "queued")
        # 完工一个 -> 排队的 t3 自动补上
        self.service.complete_task(t1["id"], "a", "dispatcher")
        t3 = self.service.get_isolation_task(t3["id"])
        self.assertEqual(t3["status"], "active")
        # 台账占用数正确
        districts = {d["code"]: d for d in self.service.list_districts()}
        self.assertEqual(districts["D-1"]["active_count"], 2)

    def test_cross_district_needs_center(self):
        # 跨片区（动边界阀 V-2）-> 待中心确认
        task = self.service.create_isolation_task(
            {"segment_code": "S-2", "valve_codes": ["V-2", "V-3"]}, "d2", "dispatcher", region="D-2"
        )
        self.assertTrue(task["cross_district"])
        self.assertEqual(task["status"], "pending_center")
        # 非中心角色不能确认
        with self.assertRaises(DomainError) as context:
            self.service.confirm_cross_district(task["id"], "d2", "dispatcher")
        self.assertEqual(context.exception.code, "forbidden")
        # 中心确认后 -> active
        task = self.service.confirm_cross_district(task["id"], "center", "center")
        self.assertEqual(task["status"], "active")
        self.assertTrue(task["center_confirmed"])

    def test_valve_command_ack_and_retry(self):
        task = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1", "V-4"]}, "a", "dispatcher", region="D-1"
        )
        task = self.service.issue_commands(task["id"], "a", "dispatcher")
        c1, c2 = task["commands"][0], task["commands"][1]
        # 不能跳过前序阀门直接回执
        with self.assertRaises(DomainError) as context:
            self.service.ack_command(c2["id"], {"result": "succeeded"}, "tech", "technician")
        self.assertEqual(context.exception.code, "command_out_of_order")
        # c1 成功，c2 失败
        self.service.ack_command(c1["id"], {"result": "succeeded"}, "tech", "technician")
        self.service.ack_command(c2["id"], {"result": "failed", "error": "timeout"}, "tech", "technician")
        c2 = self.service.get_command(c2["id"])
        self.assertEqual(c2["status"], "failed")
        # 从断点重试 c2
        self.service.retry_command(c2["id"], "tech", "technician")
        c2 = self.service.get_command(c2["id"])
        self.assertEqual(c2["status"], "pending")
        self.service.ack_command(c2["id"], {"result": "succeeded"}, "tech", "technician")
        c2 = self.service.get_command(c2["id"])
        self.assertEqual(c2["status"], "succeeded")
        # 重复回执不重复执行（attempts 不增加）
        again = self.service.ack_command(c1["id"], {"result": "succeeded"}, "tech", "technician")
        self.assertEqual(again["attempts"], 1)
        # 全部成功后可完工
        self.service.complete_task(task["id"], "a", "dispatcher")
        task = self.service.get_isolation_task(task["id"])
        self.assertEqual(task["status"], "completed")

    def test_offline_merge_conflict_review_blocks(self):
        task = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1"]}, "a", "dispatcher", region="D-1"
        )
        # 先有一条在线阀位（open）
        self.service.merge_positions(
            task["id"], {"records": [self._position("V-1", "open", "online", "2026-10-03T07:00:00+00:00")]},
            "tech", "technician",
        )
        # 回网后合并断网记录（close）-> 同一阀门两份记录并排列出等复核
        result = self.service.merge_positions(
            task["id"], {"records": [self._position("V-1", "close", "offline", "2026-10-03T08:00:00+00:00")]},
            "tech", "technician",
        )
        self.assertEqual(len(result["reviews_created"]), 1)
        review = result["reviews"][0]
        self.assertEqual(review["record_a"]["position"], "open")
        self.assertEqual(review["record_b"]["position"], "close")
        # 待复核期间不能推进（下发指令 / 完工）
        with self.assertRaises(DomainError) as context:
            self.service.issue_commands(task["id"], "a", "dispatcher")
        self.assertEqual(context.exception.code, "review_pending")
        with self.assertRaises(DomainError) as context:
            self.service.complete_task(task["id"], "a", "dispatcher")
        self.assertEqual(context.exception.code, "review_pending")
        # 复核确认 b（断网记录）
        self.service.resolve_review(task["id"], review["id"], {"choice": "b"}, "a", "dispatcher")
        task = self.service.get_isolation_task(task["id"])
        self.assertEqual(task["reviews"][0]["status"], "resolved")
        # 复核后可正常推进
        self.service.issue_commands(task["id"], "a", "dispatcher")
        task = self.service.get_isolation_task(task["id"])
        for cmd in task["commands"]:
            self.service.ack_command(cmd["id"], {"result": "succeeded"}, "tech", "technician")
        self.service.complete_task(task["id"], "a", "dispatcher")
        task = self.service.get_isolation_task(task["id"])
        self.assertEqual(task["status"], "completed")

    def test_same_position_merge_no_review(self):
        task = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1"]}, "a", "dispatcher", region="D-1"
        )
        self.service.merge_positions(
            task["id"], {"records": [self._position("V-1", "close", "online", "2026-10-03T07:00:00+00:00")]},
            "tech", "technician",
        )
        # 回网记录与在线一致 -> 只合并，不产生复核
        result = self.service.merge_positions(
            task["id"], {"records": [self._position("V-1", "close", "offline", "2026-10-03T08:00:00+00:00")]},
            "tech", "technician",
        )
        self.assertEqual(len(result["reviews_created"]), 0)
        self.assertEqual(len(result["reviews"]), 0)

    def test_restart_resumes_state(self):
        # 建任务、下发指令、部分回执、产生待复核记录
        task = self.service.create_isolation_task(
            {"segment_code": "S-1", "valve_codes": ["V-1", "V-4"]}, "a", "dispatcher", region="D-1"
        )
        task = self.service.issue_commands(task["id"], "a", "dispatcher")
        c1 = task["commands"][0]
        self.service.ack_command(c1["id"], {"result": "succeeded"}, "tech", "technician")
        self.service.merge_positions(
            task["id"], {"records": [self._position("V-1", "open", "online", "2026-10-03T07:00:00+00:00")]},
            "tech", "technician",
        )
        self.service.merge_positions(
            task["id"], {"records": [self._position("V-1", "close", "offline", "2026-10-03T08:00:00+00:00")]},
            "tech", "technician",
        )

        # 模拟关掉服务再启动：新实例指向同一个数据库文件
        repo2 = Repository(self.tmp.name)
        repo2.initialize()
        service2 = Service(repo2)

        resumed = service2.get_isolation_task(task["id"])
        # 容量占用照旧
        self.assertTrue(resumed["occupies_capacity"])
        self.assertEqual(resumed["status"], "active")
        # 未完成指令照旧（c1 成功，c2 仍 pending）
        statuses = {c["sequence"]: c["status"] for c in resumed["commands"]}
        self.assertEqual(statuses[1], "succeeded")
        self.assertEqual(statuses[2], "pending")
        # 待复核记录照旧
        self.assertEqual(len(resumed["reviews"]), 1)
        self.assertEqual(resumed["reviews"][0]["status"], "pending")
        # 台账占用数照旧
        districts = {d["code"]: d for d in service2.list_districts()}
        self.assertEqual(districts["D-1"]["active_count"], 1)

    def test_duplicate_district_and_valve(self):
        with self.assertRaises(ConflictError):
            self.service.register_district({"code": "D-1", "name": "城东", "capacity": 2}, "c", "center")
        with self.assertRaises(ConflictError):
            self.service.register_valve({"code": "V-1", "district_code": "D-1"}, "c", "center")


if __name__ == "__main__":
    unittest.main()

import itertools

from . import domain, rules
from .domain import DomainError


class CommandGateway:
    """阀门命令网关接口。生产实现可对接 SCADA/RTU；测试可用脚本化实现。"""

    def send(self, command):
        """返回 (receipt_id, result, detail)。result 为 acked 或 failed；只代表本次发送结果。"""
        raise NotImplementedError


class StaticGateway(CommandGateway):
    """内存脚本网关：按阀门配置成功/失败，便于演示与测试。进程重启后状态以数据库为准。"""

    def __init__(self, failing_valves=None):
        self.failing_valves = set(failing_valves or [])
        self._counter = itertools.count(1)

    def send(self, command):
        receipt_id = "RCP-%d" % next(self._counter)
        if command["valve_id"] in self.failing_valves:
            return receipt_id, "failed", "device unreachable"
        return receipt_id, "acked", ""


class Service:
    def __init__(self, repository, gateway=None):
        self.repository = repository
        self.gateway = gateway or StaticGateway()

    # ------------------------------------------------------------------ items

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        segment = self.repository.get_segment_region(normalized["segment_id"])
        if segment is None:
            raise DomainError("segment_not_in_ledger", "管段未登记在片区台账中", 404)
        owner_region = segment["region_code"]
        # 调度员只开本片区管段的单；中心调度（regulator）不受限
        if role != "regulator":
            if not region:
                raise DomainError("region_required", "需要声明所属片区", 403)
            if region != owner_region:
                raise DomainError("out_of_region", "替别的片区开单算越权", 403)
        normalized["region"] = owner_region
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and role != "regulator":
            if not region:
                raise DomainError("region_required", "需要声明所属片区", 403)
            if item["payload"].get("region") != region:
                raise DomainError("out_of_region", "不能处理其他片区的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)

        if action == "isolate":
            return self._request_isolation(item_id, payload, actor, role, region, expected_version)
        if action == "joint_confirm":
            return self._joint_confirm(item_id, actor, role, expected_version)
        if action == "resolve_valve_conflict":
            return self._resolve_conflict(payload, actor, role)

        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        result = self.get_item(item_id)
        # 恢复供气或取消后容量释放，排队任务按 FIFO 自动准入并从断点下发指令
        if action in ("restore", "cancel"):
            self._promote_and_dispatch()
        return result

    def _promote_and_dispatch(self, region_codes=None):
        promoted = self.repository.promote_queues(region_codes)["promoted"]
        dispatched = {}
        for promoted_id in promoted:
            dispatched[promoted_id] = self._advance_commands(promoted_id)
        return {"promoted": promoted, "dispatched": dispatched}

    # ------------------------------------------------------------- isolation

    def _check_valve_conflicts(self, valve_ids):
        open_valves = set(self.repository.open_conflict_valves())
        blocked = sorted(open_valves.intersection(valve_ids))
        if blocked:
            raise DomainError(
                "valve_status_conflict",
                "阀门 %s 有并列待复核记录，复核完成前不能推进" % ",".join(blocked),
                409,
            )

    def _request_isolation(self, item_id, payload, actor, role, region, expected_version):
        sequence = domain.normalize_valve_sequence(payload)
        valves = self.repository.get_valves(sequence)
        unknown = [valve_id for valve_id in sequence if valve_id not in valves]
        if unknown:
            raise DomainError("valve_not_in_ledger", "阀门 %s 未登记在片区台账中" % ",".join(unknown), 404)
        item = self.repository.get_item(item_id)
        owner_region = item["payload"].get("region")
        # 跨片区：指令序列触及属片区以外的任何阀门
        cross_region = any(valves[v]["region_code"] != owner_region for v in sequence)
        if cross_region and role != "regulator":
            # 片区调度员只能开本片区的隔离单；跨片区联合作业报越权，需中心调度走确认
            raise DomainError("joint_confirmation_required",
                              "跨片区联合作业需要中心调度确认", 403)
        self._check_valve_conflicts(sequence)
        plan = {valve_id: valves[valve_id] for valve_id in sequence}
        outcome = self.repository.request_isolation(
            item_id, sequence, plan, cross_region, actor, role, expected_version
        )
        outcome["item"] = self.get_item(item_id)
        if outcome["decision"] == "started":
            outcome.update(self._advance_commands(item_id))
        return outcome

    def _joint_confirm(self, item_id, actor, role, expected_version):
        outcome = self.repository.joint_confirm(item_id, actor, role, expected_version)
        outcome["item"] = self.get_item(item_id)
        if outcome["decision"] == "started":
            outcome.update(self._advance_commands(item_id))
        return outcome

    # ---------------------------------------------------------------- commands

    def _advance_commands(self, item_id):
        """从断点续传：只下发序列中第一条尚未完成的指令；无断点则全部已完成。"""
        command = self.repository.next_command_to_dispatch(item_id)
        if command is None:
            return {"dispatched": None, "pending": self.repository.list_commands(item_id)}
        attempts = int(command["attempts"]) + 1
        self.repository.mark_dispatched(command["id"], attempts)
        receipt_id, result, detail = self.gateway.send(command)
        applied = self.repository.apply_receipt(command["id"], receipt_id, result, detail)
        response = {
            "dispatched": {
                "command_id": command["id"],
                "step": command["step"],
                "valve_id": command["valve_id"],
                "attempt": attempts,
                "receipt_id": receipt_id,
                "result": result,
            },
            "pending": self.repository.list_commands(item_id),
        }
        if result == "failed":
            response["resume"] = "retry_failed_command"
            return response
        if applied.get("sequence_completed"):
            response["sequence_completed"] = True
            # 容量未释放，无需 promote；仅在极端情况下对同片区排队者无影响
            return response
        # 本步成功：继续推进下一步（同一次调用内顺序下发）；失败则停在断点等 retry
        response["next"] = self._advance_commands(item_id)
        return response

    def retry_command(self, item_id, actor, role, region=None):
        """阀门命令失败后从断点重试。"""
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in ("dispatcher", "supervisor", "regulator"):
            raise DomainError("forbidden", "当前角色不能重试阀门指令", 403)
        item = self.repository.get_item(item_id)
        if role != "regulator":
            if not region:
                raise DomainError("region_required", "需要声明所属片区", 403)
            if item["payload"].get("region") != region:
                raise DomainError("out_of_region", "不能处理其他片区的指令", 403)
        if item["status"] != rules.ISOLATING_STATUS:
            raise DomainError("invalid_state", "任务不处于指令执行中，无需重试", 409)
        failed = self.repository.next_command_to_dispatch(item_id)
        if failed is None:
            return {"dispatched": None, "pending": self.repository.list_commands(item_id)}
        if failed["status"] != "failed":
            raise DomainError("command_not_failed", "断点指令不在失败状态，请等待回执", 409)
        return self._advance_commands(item_id)

    def command_receipt(self, item_id, command_id, payload):
        """外部回执上报：重复回执不重复执行，只回显既有状态。"""
        normalized = domain.normalize_receipt(payload)
        command = self.repository.get_command(command_id)
        if command["item_id"] != item_id:
            raise DomainError("command_mismatch", "指令不属于该隔离任务", 409)
        existing = self.repository.find_command_by_receipt(normalized["receipt_id"])
        if existing is not None and existing["id"] != command_id:
            raise DomainError("receipt_id_conflict", "回执编号已用于其它指令", 409)
        applied = self.repository.apply_receipt(
            command_id, normalized["receipt_id"], normalized["result"], normalized["detail"]
        )
        response = {"duplicate": applied["duplicate"], "command": applied["command"]}
        if applied["duplicate"]:
            response["message"] = "重复回执，未重复执行"
            return response
        response["sequence_completed"] = applied.get("sequence_completed", False)
        if normalized["result"] == "acked" and not response["sequence_completed"]:
            # 网关异步回执到达后，继续从下一断点下发
            response["next"] = self._advance_commands(item_id)
        return response

    def resume_on_startup(self):
        """服务重启后续跑：未完成指令、容量占用、排队、待复核记录均由数据库恢复。"""
        promoted = self.repository.promote_queues()
        resumed = []
        for item in self.repository.list_items(status=rules.ISOLATING_STATUS):
            command = self.repository.next_command_to_dispatch(item["id"])
            if command is not None and command["status"] == "failed":
                resumed.append({"item_id": item["id"], "action": "await_retry",
                                "command_id": command["id"], "valve_id": command["valve_id"]})
            elif command is not None and command["status"] == "sent":
                resumed.append({"item_id": item["id"], "action": "await_receipt",
                                "command_id": command["id"], "valve_id": command["valve_id"]})
        return {"promoted": promoted["promoted"], "resumed": resumed,
                "open_conflicts": self.repository.list_conflicts("open")}

    # ------------------------------------------------------------- readings

    def merge_field_readings(self, payload, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in ("patrol", "responder", "dispatcher", "regulator"):
            raise DomainError("forbidden", "当前角色不能回传现场阀位", 403)
        records = domain.normalize_reading_records(payload)
        for record in records:
            valve = self.repository.get_valve(record["valve_id"])
            if valve is None:
                raise DomainError("valve_not_in_ledger", "阀门 %s 未登记在片区台账中" % record["valve_id"], 404)
        return self.repository.merge_readings(records, actor, role)

    def resolve_conflict_direct(self, conflict_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.ACTION_ROLES["resolve_valve_conflict"]:
            raise DomainError("forbidden", "当前角色不能复核阀位冲突", 403)
        if not isinstance(payload, dict):
            payload = {}
        payload = dict(payload)
        payload["conflict_id"] = conflict_id
        return self._resolve_conflict(payload, actor, role)

    def _resolve_conflict(self, payload, actor, role):
        if not isinstance(payload.get("conflict_id"), int):
            raise DomainError("conflict_id_required", "需要 conflict_id")
        resolution = domain.require_text(payload, "resolution")
        outcome = self.repository.resolve_conflict(payload["conflict_id"], resolution, actor, role)
        if outcome["decision"] == "resolved":
            # 复核完成后尝试放行因冲突挂起的排队任务，并下发指令
            outcome["promotion"] = self._promote_and_dispatch()
        return outcome

    # ------------------------------------------------------------------ ledger

    def upsert_region(self, payload, actor, role):
        self._require_admin(actor, role)
        region = domain.normalize_region(payload)
        return self.repository.upsert_region(region)

    def upsert_segment(self, payload, actor, role):
        self._require_admin(actor, role)
        segment = domain.normalize_segment(payload)
        return self.repository.upsert_segment(segment)

    def upsert_valve(self, payload, actor, role):
        self._require_admin(actor, role)
        valve = domain.normalize_valve(payload)
        return self.repository.upsert_valve(valve)

    def ledger(self):
        return self.repository.ledger()

    def _require_admin(self, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role != "regulator":
            raise DomainError("forbidden", "只有中心调度能维护片区台账", 403)

    # ----------------------------------------------------------------- queries

    def get_command(self, command_id):
        return self.repository.get_command(command_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["commands"] = self.repository.list_commands(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def queue(self):
        return self.repository.list_queue()

    def conflicts(self, status=None):
        return self.repository.list_conflicts(status)

    def state(self):
        return self.repository.state_summary()

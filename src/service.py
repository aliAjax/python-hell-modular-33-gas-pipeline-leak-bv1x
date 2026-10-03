from . import domain, rules
from .domain import DomainError


class Service:
    def __init__(self, repository):
        self.repository = repository

    def _identity(self, actor, role):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        return actor, role

    def _require_role(self, role, allowed):
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
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
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
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
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        return self.repository.state_summary()

    # ---------------- 片区台账 ----------------

    def register_district(self, payload, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"center"})
        normalized = domain.normalize_district(payload)
        return self.repository.register_district(
            normalized["code"], normalized["name"], normalized["capacity"], actor, role
        )

    def list_districts(self):
        return self.repository.list_districts()

    def register_segment(self, payload, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"center", "dispatcher", "supervisor"})
        normalized = domain.normalize_segment(payload)
        return self.repository.register_segment(
            normalized["code"], normalized["district_code"], normalized.get("pipeline_id", ""), actor, role
        )

    def list_segments(self):
        return self.repository.list_segments()

    def register_valve(self, payload, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"center", "supervisor"})
        normalized = domain.normalize_valve(payload)
        return self.repository.register_valve(
            normalized["code"], normalized["name"], normalized["district_code"],
            normalized["is_boundary"], normalized["shared_with"], actor, role
        )

    def list_valves(self):
        return self.repository.list_valves()

    # ---------------- 隔离任务 ----------------

    def create_isolation_task(self, payload, actor, role, region=None):
        self._identity(actor, role)
        self._require_role(role, {"dispatcher", "supervisor", "center"})
        normalized = domain.normalize_task_create(payload)
        return self.repository.create_isolation_task(
            normalized["segment_code"], normalized["valve_codes"],
            normalized.get("leak_item_id"), actor, role, region
        )

    def list_isolation_tasks(self, district_code=None, status=None):
        return self.repository.list_isolation_tasks(district_code, status)

    def get_isolation_task(self, task_id):
        return self.repository.get_isolation_task(task_id)

    def confirm_cross_district(self, task_id, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"center"})
        return self.repository.confirm_cross_district(task_id, actor, role)

    def complete_task(self, task_id, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"dispatcher", "supervisor", "center"})
        return self.repository.complete_task(task_id, actor, role)

    def cancel_task(self, task_id, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"dispatcher", "supervisor", "center"})
        return self.repository.cancel_task(task_id, actor, role)

    # ---------------- 阀门指令 ----------------

    def issue_commands(self, task_id, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"dispatcher", "supervisor", "center"})
        return self.repository.issue_commands(task_id, actor, role)

    def list_commands(self, task_id):
        return self.repository.list_commands(task_id)

    def get_command(self, command_id):
        return self.repository.get_command(command_id)

    def ack_command(self, command_id, payload, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"technician", "dispatcher", "supervisor", "center"})
        normalized = domain.normalize_ack(payload)
        return self.repository.ack_command(
            command_id, normalized["result"], normalized.get("error", ""), actor, role
        )

    def retry_command(self, command_id, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"technician", "dispatcher", "supervisor", "center"})
        return self.repository.retry_command(command_id, actor, role)

    # ---------------- 断网阀位合并与复核 ----------------

    def merge_positions(self, task_id, payload, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"technician", "dispatcher", "supervisor", "center"})
        records = domain.normalize_positions(payload)
        return self.repository.merge_positions(task_id, records, actor, role)

    def list_reviews(self, task_id, status=None):
        return self.repository.list_reviews(task_id, status)

    def resolve_review(self, task_id, review_id, payload, actor, role):
        self._identity(actor, role)
        self._require_role(role, {"dispatcher", "supervisor", "center"})
        normalized = domain.normalize_review_resolve(payload)
        return self.repository.resolve_review(review_id, normalized["choice"], actor, role)

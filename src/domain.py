from datetime import datetime


class DomainError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.status = status


class ConflictError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 409)


class NotFoundError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 404)


def require_text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def number(payload, name, minimum=None):
    value = payload.get(name)
    if isinstance(value, bool):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    if minimum is not None and value < minimum:
        raise DomainError("invalid_number", "%s 不能小于 %s" % (name, minimum))
    return value


def parse_timestamp(payload, name):
    value = require_text(payload, name)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)
    return value


def normalize_create(payload):
    pipeline_id = require_text(payload, "pipeline_id")
    segment_id = require_text(payload, "segment_id")
    reported_at = parse_timestamp(payload, "reported_at")
    pressure_drop = number(payload, "pressure_drop_kpa", 0)
    sensor_ppm = number(payload, "sensor_value_ppm", 0)
    odor_reports = int(payload.get("odor_reports", 0) or 0)
    if odor_reports < 0:
        raise DomainError("invalid_odor_reports", "异味报告数不能为负数")
    reporter = require_text(payload, "reporter")
    stable_key = "%s|%s|%s" % (pipeline_id, segment_id, reported_at)
    return {
        "pipeline_id": pipeline_id,
        "segment_id": segment_id,
        "reported_at": reported_at,
        "pressure_drop_kpa": pressure_drop,
        "sensor_value_ppm": sensor_ppm,
        "odor_reports": odor_reports,
        "reporter": reporter,
        "source_comparison": [],
        "valve_sequence": [],
        "hazards_clear": False,
        "_stable_key": stable_key,
    }


def normalize_source(payload):
    source_type = require_text(payload, "source_type")
    external_id = require_text(payload, "external_id")
    observed_at = parse_timestamp(payload, "observed_at")
    result = {
        "source_type": source_type,
        "external_id": external_id,
        "observed_at": observed_at,
        "sensor_value_ppm": number(payload, "sensor_value_ppm", 0) if "sensor_value_ppm" in payload else None,
        "pressure_drop_kpa": number(payload, "pressure_drop_kpa", 0) if "pressure_drop_kpa" in payload else None,
        "odor_reports": int(payload.get("odor_reports", 0) or 0),
        "note": payload.get("note", ""),
    }
    return result


def normalize_district(payload):
    code = require_text(payload, "code")
    name = require_text(payload, "name")
    capacity = int(payload.get("capacity", 1) or 1)
    if capacity < 1:
        raise DomainError("invalid_capacity", "容量必须大于 0")
    return {"code": code, "name": name, "capacity": capacity}


def normalize_segment(payload):
    code = require_text(payload, "code")
    district_code = require_text(payload, "district_code")
    pipeline_id = payload.get("pipeline_id", "")
    return {"code": code, "district_code": district_code, "pipeline_id": pipeline_id}


def normalize_valve(payload):
    code = require_text(payload, "code")
    district_code = require_text(payload, "district_code")
    name = payload.get("name", "")
    is_boundary = bool(payload.get("is_boundary", False))
    shared_with = payload.get("shared_with", [])
    if not isinstance(shared_with, list):
        raise DomainError("invalid_shared_with", "共用片区必须是列表")
    shared_with = [str(value).strip() for value in shared_with if str(value).strip()]
    return {"code": code, "name": name, "district_code": district_code,
            "is_boundary": is_boundary, "shared_with": shared_with}


def normalize_task_create(payload):
    segment_code = require_text(payload, "segment_code")
    valve_codes = payload.get("valve_codes", [])
    if not isinstance(valve_codes, list) or not valve_codes:
        raise DomainError("valve_codes_required", "至少需要一个阀门")
    valve_codes = [str(value).strip() for value in valve_codes if str(value).strip()]
    if not valve_codes:
        raise DomainError("valve_codes_required", "至少需要一个阀门")
    leak_item_id = payload.get("leak_item_id")
    if leak_item_id is not None:
        leak_item_id = int(leak_item_id)
    return {"segment_code": segment_code, "valve_codes": valve_codes, "leak_item_id": leak_item_id}


def normalize_positions(payload):
    records = payload.get("records", [])
    if not isinstance(records, list) or not records:
        raise DomainError("records_required", "至少需要一条阀位记录")
    result = []
    for rec in records:
        if not isinstance(rec, dict):
            raise DomainError("invalid_record", "阀位记录格式无效")
        valve_code = require_text(rec, "valve_code")
        position = require_text(rec, "position")
        if position not in ("open", "close", "unknown"):
            raise DomainError("invalid_position", "阀位必须是 open/close/unknown")
        recorded_at = parse_timestamp(rec, "recorded_at")
        source = rec.get("source", "offline")
        if source not in ("online", "offline"):
            raise DomainError("invalid_source", "来源必须是 online/offline")
        result.append({"valve_code": valve_code, "position": position,
                       "recorded_at": recorded_at, "source": source, "note": rec.get("note", "")})
    return result


def normalize_review_resolve(payload):
    choice = require_text(payload, "choice")
    if choice not in ("a", "b"):
        raise DomainError("invalid_choice", "选择必须是 a 或 b")
    return {"choice": choice}


def normalize_ack(payload):
    result = require_text(payload, "result")
    if result not in ("succeeded", "failed"):
        raise DomainError("invalid_result", "回执结果必须是 succeeded/failed")
    return {"result": result, "error": payload.get("error", "")}

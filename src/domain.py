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


def normalize_region(payload):
    code = require_text(payload, "code")
    name = require_text(payload, "name")
    capacity = payload.get("capacity", 0)
    if isinstance(capacity, bool):
        raise DomainError("invalid_capacity", "容量必须是非负整数")
    try:
        capacity = int(capacity)
    except (TypeError, ValueError):
        raise DomainError("invalid_capacity", "容量必须是非负整数")
    if capacity < 0:
        raise DomainError("invalid_capacity", "容量不能为负数")
    return {"code": code, "name": name, "capacity": capacity}


def normalize_segment(payload):
    return {"segment_id": require_text(payload, "segment_id"), "region_code": require_text(payload, "region_code")}


def normalize_valve(payload):
    valve_id = require_text(payload, "valve_id")
    region_code = require_text(payload, "region_code")
    shared_with = payload.get("shared_with")
    if shared_with is not None:
        if not isinstance(shared_with, str) or not shared_with.strip():
            raise DomainError("invalid_shared_with", "shared_with 必须是片区编号或留空")
        shared_with = shared_with.strip()
        if shared_with == region_code:
            raise DomainError("invalid_shared_with", "界阀不能与所属片区相同")
    return {"valve_id": valve_id, "region_code": region_code, "shared_with": shared_with}


def normalize_valve_sequence(payload):
    sequence = payload.get("valve_sequence")
    if not isinstance(sequence, list) or len(sequence) < 2:
        raise DomainError("valve_sequence_required", "至少需要提交两个阀门及顺序")
    result = []
    for value in sequence:
        if not isinstance(value, str) or not value.strip():
            raise DomainError("invalid_valve_sequence", "阀门顺序格式无效")
        valve_id = value.strip()
        if valve_id in result:
            raise DomainError("duplicate_valve_in_sequence", "同一阀门在指令序列中出现了两次")
        result.append(valve_id)
    return result


def normalize_reading_records(payload):
    records = payload.get("records")
    if not isinstance(records, list) or not records:
        raise DomainError("records_required", "至少需要一条阀位记录")
    normalized = []
    for record in records:
        if not isinstance(record, dict):
            raise DomainError("invalid_reading", "阀位记录格式无效")
        normalized.append(
            {
                "client_id": require_text(record, "client_id"),
                "valve_id": require_text(record, "valve_id"),
                "position": require_text(record, "position"),
                "observed_at": parse_timestamp(record, "observed_at"),
                "device_id": str(record.get("device_id", "") or "").strip(),
            }
        )
    return normalized


def normalize_receipt(payload):
    receipt_id = require_text(payload, "receipt_id")
    result = require_text(payload, "result")
    if result not in ("acked", "failed"):
        raise DomainError("invalid_receipt_result", "回执结果只能是 acked 或 failed")
    return {"receipt_id": receipt_id, "result": result, "detail": str(payload.get("detail", "") or "")}

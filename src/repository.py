import json
import sqlite3
from datetime import datetime, timezone

from . import rules
from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS regions (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    capacity INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS segments (
                    segment_id TEXT PRIMARY KEY,
                    region_code TEXT NOT NULL REFERENCES regions(code)
                );
                CREATE TABLE IF NOT EXISTS valves (
                    valve_id TEXT PRIMARY KEY,
                    region_code TEXT NOT NULL REFERENCES regions(code),
                    shared_with TEXT REFERENCES regions(code)
                );
                CREATE TABLE IF NOT EXISTS valve_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id),
                    step INTEGER NOT NULL,
                    valve_id TEXT NOT NULL,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    receipt_id TEXT,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    UNIQUE(item_id, step),
                    UNIQUE(receipt_id)
                );
                CREATE TABLE IF NOT EXISTS valve_readings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL UNIQUE,
                    valve_id TEXT NOT NULL,
                    position TEXT NOT NULL,
                    device_id TEXT NOT NULL DEFAULT '',
                    observed_at TEXT NOT NULL,
                    merged_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS valve_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    valve_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by TEXT,
                    resolution TEXT,
                    records TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_items_status ON items(status);
                CREATE INDEX IF NOT EXISTS idx_commands_item ON valve_commands(item_id, step);
                CREATE INDEX IF NOT EXISTS idx_readings_valve ON valve_readings(valve_id);
                CREATE INDEX IF NOT EXISTS idx_conflicts_status ON valve_conflicts(status);
                """
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ utils

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def _record_action(self, conn, item_id, action, actor, role, payload):
        conn.execute(
            "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
            (item_id, action, actor, role, canonical_json(payload), now_iso()),
        )

    # ------------------------------------------------------------------ items

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            self._record_action(conn, item_id, action, actor, role, event_payload)
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    # ------------------------------------------------------------------ ledger

    def has_ledger(self):
        conn = self.connect()
        try:
            return conn.execute("SELECT COUNT(*) AS total FROM regions").fetchone()["total"] > 0
        finally:
            conn.close()

    def upsert_region(self, region):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO regions(code,name,capacity) VALUES(?,?,?) "
                "ON CONFLICT(code) DO UPDATE SET name=excluded.name, capacity=excluded.capacity",
                (region["code"], region["name"], region["capacity"]),
            )
            conn.execute("COMMIT")
            return self.get_region(region["code"])
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_region(self, code):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM regions WHERE code=?", (code,)).fetchone()
            if row is None:
                raise NotFoundError("region_not_found", "片区台账中没有该片区")
            return dict(row)
        finally:
            conn.close()

    def upsert_segment(self, segment):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO segments(segment_id,region_code) VALUES(?,?) "
                    "ON CONFLICT(segment_id) DO UPDATE SET region_code=excluded.region_code",
                    (segment["segment_id"], segment["region_code"]),
                )
            except sqlite3.IntegrityError:
                raise NotFoundError("region_not_found", "片区台账中没有该片区")
            conn.execute("COMMIT")
            return dict(conn.execute("SELECT * FROM segments WHERE segment_id=?", (segment["segment_id"],)).fetchone())
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_segment_region(self, segment_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM segments WHERE segment_id=?", (segment_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_valve(self, valve):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO valves(valve_id,region_code,shared_with) VALUES(?,?,?) "
                    "ON CONFLICT(valve_id) DO UPDATE SET region_code=excluded.region_code, shared_with=excluded.shared_with",
                    (valve["valve_id"], valve["region_code"], valve.get("shared_with")),
                )
            except sqlite3.IntegrityError:
                raise NotFoundError("region_not_found", "片区台账中没有该片区")
            conn.execute("COMMIT")
            return self.get_valve(valve["valve_id"])
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_valve(self, valve_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM valves WHERE valve_id=?", (valve_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_valves(self, valve_ids):
        if not valve_ids:
            return {}
        conn = self.connect()
        try:
            placeholders = ",".join("?" for _ in valve_ids)
            rows = conn.execute(
                "SELECT * FROM valves WHERE valve_id IN (%s)" % placeholders, tuple(valve_ids)
            ).fetchall()
            return {row["valve_id"]: dict(row) for row in rows}
        finally:
            conn.close()

    def ledger(self):
        conn = self.connect()
        try:
            return {
                "regions": [dict(row) for row in conn.execute("SELECT * FROM regions ORDER BY code").fetchall()],
                "segments": [dict(row) for row in conn.execute("SELECT * FROM segments ORDER BY segment_id").fetchall()],
                "valves": [dict(row) for row in conn.execute("SELECT * FROM valves ORDER BY valve_id").fetchall()],
            }
        finally:
            conn.close()

    def seed_demo_ledger(self):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO regions(code,name,capacity) VALUES(?,?,?) ON CONFLICT(code) DO NOTHING",
                [("EAST", "东片区", 1), ("WEST", "西片区", 1)],
            )
            conn.executemany(
                "INSERT INTO segments(segment_id,region_code) VALUES(?,?) ON CONFLICT(segment_id) DO NOTHING",
                [("S-E1", "EAST"), ("S-E2", "EAST"), ("S-W1", "WEST")],
            )
            conn.executemany(
                "INSERT INTO valves(valve_id,region_code,shared_with) VALUES(?,?,?) ON CONFLICT(valve_id) DO NOTHING",
                [
                    ("V-E1", "EAST", None),
                    ("V-E2", "EAST", None),
                    ("V-W1", "WEST", None),
                    ("V-W2", "WEST", None),
                    ("V-B1", "EAST", "WEST"),
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    # ------------------------------------------------------- isolation & queue

    def _active_item_ids_for_valves(self, conn, valve_ids):
        if not valve_ids:
            return {}
        valve_marks = ",".join("?" for _ in valve_ids)
        status_marks = ",".join("?" for _ in rules.ACTIVE_ISOLATION_STATUSES)
        query = (
            "SELECT vc.valve_id AS valve_id, vc.item_id AS item_id FROM valve_commands vc "
            "JOIN items i ON i.id = vc.item_id "
            "WHERE vc.valve_id IN (" + valve_marks + ") AND i.status IN (" + status_marks + ")"
        )
        rows = conn.execute(query, tuple(valve_ids) + rules.ACTIVE_ISOLATION_STATUSES).fetchall()
        return {row["valve_id"]: row["item_id"] for row in rows}

    def _item_locked(self, conn, item_id):
        row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("item_not_found", "业务实体不存在")
        return self._row_to_item(row)

    def request_isolation(self, item_id, sequence, plan, cross_region, actor, role, expected_version=None):
        """容量满排队、界阀占用排队；已排队任务重放只返回现状。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            item = self._row_to_item(row)
            if item["status"] in ("isolating", "isolated"):
                conn.execute("COMMIT")
                return {"decision": "active", "item": self.get_item(item_id)}
            if item["status"] == "queued":
                conn.execute("COMMIT")
                return {"decision": "queued", "item": self.get_item(item_id)}
            if item["status"] not in ("verified",):
                raise DomainError("invalid_state", "当前状态 %s 不允许申请隔离" % item["status"])
            if expected_version is not None and int(expected_version) != item["version"]:
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")

            payload = dict(item["payload"])
            payload["valve_sequence"] = sequence
            payload["isolation_plan"] = plan
            payload["cross_region"] = cross_region
            payload["joint_confirmed"] = bool(cross_region is False)

            # 先看未复核冲突，再看本片区容量，最后看界阀是否被相邻片区占用
            if self._open_conflict_for_valves_locked(conn, sequence):
                self._persist_requested(conn, item_id, payload, rules.QUEUE_WAIT_STATUS, actor, role,
                                        {"reason": "valve_conflict_open"})
                conn.execute("COMMIT")
                return {"decision": "queued", "reason": "valve_conflict_open", "item": self.get_item(item_id)}

            region = conn.execute(
                "SELECT * FROM regions WHERE code=?", (payload["region"],)
            ).fetchone()
            if region is None:
                raise NotFoundError("region_not_found", "片区台账中没有该片区")
            if self._region_usage_locked(conn, payload["region"]) >= int(region["capacity"]):
                self._persist_requested(conn, item_id, payload, rules.QUEUE_WAIT_STATUS, actor, role,
                                        {"reason": "region_capacity_full", "capacity": int(region["capacity"])})
                conn.execute("COMMIT")
                return {"decision": "queued", "reason": "region_capacity_full",
                        "capacity": int(region["capacity"]), "item": self.get_item(item_id)}

            blocked_valves = self._active_item_ids_for_valves(conn, sequence)
            if blocked_valves:
                self._persist_requested(conn, item_id, payload, rules.QUEUE_WAIT_STATUS, actor, role,
                                        {"reason": "boundary_valve_busy", "blocked_by": blocked_valves})
                conn.execute("COMMIT")
                return {"decision": "queued", "reason": "boundary_valve_busy", "blocked_by": blocked_valves,
                        "item": self.get_item(item_id)}

            if cross_region:
                # 跨片区联合作业必须先经中心调度确认，确认前不占用容量
                self._persist_requested(conn, item_id, payload, "verified", actor, role,
                                        {"reason": "joint_confirmation_required", "cross_region": True})
                conn.execute("COMMIT")
                return {"decision": "joint_confirmation_required", "item": self.get_item(item_id)}

            decision = self._try_admit_locked(conn, item_id, payload, sequence, actor, role)
            conn.execute("COMMIT")
            decision["item"] = self.get_item(item_id)
            return decision
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _persist_requested(self, conn, item_id, payload, new_status, actor, role, event_payload):
        row = conn.execute("SELECT version FROM items WHERE id=?", (item_id,)).fetchone()
        version = int(row["version"]) + 1
        conn.execute(
            "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
            (new_status, version, canonical_json(payload), now_iso(), item_id),
        )
        self._record_action(conn, item_id, "isolate_requested", actor, role, event_payload)
        self.append_audit(conn, item_id, "isolate_requested", actor, role, event_payload)

    def _region_usage_locked(self, conn, region_code):
        placeholders = ",".join("?" for _ in rules.ACTIVE_ISOLATION_STATUSES)
        row = conn.execute(
            "SELECT COUNT(*) AS total FROM items WHERE json_extract(payload,'$.region')=? AND status IN (%s)"
            % placeholders,
            (region_code,) + rules.ACTIVE_ISOLATION_STATUSES,
        ).fetchone()
        return int(row["total"])

    def _open_conflict_for_valves_locked(self, conn, valve_ids):
        if not valve_ids:
            return set()
        placeholders = ",".join("?" for _ in valve_ids)
        rows = conn.execute(
            "SELECT valve_id FROM valve_conflicts WHERE status=? AND valve_id IN (%s)" % placeholders,
            (rules.OPEN_CONFLICT,) + tuple(valve_ids),
        ).fetchall()
        return {row["valve_id"] for row in rows}

    def _try_admit_locked(self, conn, item_id, payload, sequence, actor, role, event_extra=None):
        region_code = payload["region"]
        region = conn.execute("SELECT * FROM regions WHERE code=?", (region_code,)).fetchone()
        if region is None:
            raise NotFoundError("region_not_found", "片区台账中没有该片区")
        if self._open_conflict_for_valves_locked(conn, sequence):
            self._persist_requested(conn, item_id, payload, rules.QUEUE_WAIT_STATUS, actor, role,
                                    {"reason": "valve_conflict_open"})
            return {"decision": "queued", "reason": "valve_conflict_open"}
        capacity = int(region["capacity"])
        if self._region_usage_locked(conn, region_code) >= capacity:
            self._persist_requested(conn, item_id, payload, rules.QUEUE_WAIT_STATUS, actor, role,
                                    {"reason": "region_capacity_full", "capacity": capacity})
            return {"decision": "queued", "reason": "region_capacity_full", "capacity": capacity}
        # 再确认界阀没有被相邻片区占用
        blocked_valves = self._active_item_ids_for_valves(conn, sequence)
        if blocked_valves:
            self._persist_requested(conn, item_id, payload, rules.QUEUE_WAIT_STATUS, actor, role,
                                    {"reason": "boundary_valve_busy", "blocked_by": blocked_valves})
            return {"decision": "queued", "reason": "boundary_valve_busy", "blocked_by": blocked_valves}
        # 准入：建指令序列，进入 isolating 并占用本片区容量
        event_payload = dict(event_extra or {})
        event_payload["capacity"] = capacity
        self._admit_locked(conn, item_id, payload, sequence, actor, role, event_payload)
        return {"decision": "started"}

    def _admit_locked(self, conn, item_id, payload, sequence, actor, role, event_payload):
        timestamp = now_iso()
        for step, valve_id in enumerate(sequence):
            conn.execute(
                "INSERT INTO valve_commands(item_id,step,valve_id,command,status,attempts,updated_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (item_id, step, valve_id, "close", "pending", 0, timestamp),
            )
        version = self._bump_version(conn, item_id, payload, rules.ISOLATING_STATUS)
        event_payload = dict(event_payload)
        event_payload["sequence"] = sequence
        self._record_action(conn, item_id, "isolate", actor, role, event_payload)
        self.append_audit(conn, item_id, "isolate", actor, role, event_payload)
        return version

    def _bump_version(self, conn, item_id, payload, new_status):
        row = conn.execute("SELECT version FROM items WHERE id=?", (item_id,)).fetchone()
        version = int(row["version"]) + 1
        conn.execute(
            "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
            (new_status, version, canonical_json(payload), now_iso(), item_id),
        )
        return version

    def joint_confirm(self, item_id, actor, role, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            item = self._row_to_item(row)
            if not item["payload"].get("cross_region"):
                raise DomainError("not_cross_region", "该任务不是跨片区联合作业", 409)
            if item["payload"].get("joint_confirmed"):
                conn.execute("COMMIT")
                return {"decision": "already_confirmed", "item": self.get_item(item_id)}
            if expected_version is not None and int(expected_version) != item["version"]:
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            payload = dict(item["payload"])
            payload["joint_confirmed"] = True
            decision = {"decision": "joint_confirmed"}
            if item["status"] == "verified":
                # 确认时再做一次容量/占用/复核判断
                admitted = self._try_admit_locked(
                    conn, item_id, payload, payload["valve_sequence"], actor, role,
                    {"joint_confirm": True},
                )
                if admitted["decision"] == "started":
                    decision["decision"] = "started"
                else:
                    decision["decision"] = "queued_" + admitted["reason"]
                    decision.update(admitted)
            else:
                self._bump_version(conn, item_id, payload, item["status"])
                self._record_action(conn, item_id, "joint_confirm", actor, role, {"joint_confirm": True})
                self.append_audit(conn, item_id, "joint_confirm", actor, role, {"joint_confirm": True})
            conn.execute("COMMIT")
            decision["item"] = self.get_item(item_id)
            return decision
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def promote_queues(self, region_codes=None):
        """容量释放或界阀解锁后，按 FIFO 尝试准入排队任务。"""
        conn = self.connect()
        promoted = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT * FROM items WHERE status=? ORDER BY id ASC", (rules.QUEUE_WAIT_STATUS,)
            ).fetchall()
            for row in rows:
                item = self._row_to_item(row)
                region_code = item["payload"].get("region")
                if region_codes and region_code not in region_codes:
                    continue
                payload = dict(item["payload"])
                sequence = payload.get("valve_sequence") or []
                if payload.get("cross_region") and not payload.get("joint_confirmed"):
                    continue
                if self._active_item_ids_for_valves(conn, sequence):
                    continue
                if self._open_conflict_for_valves_locked(conn, sequence):
                    continue
                region = conn.execute("SELECT * FROM regions WHERE code=?", (region_code,)).fetchone()
                if region is None:
                    continue
                if self._region_usage_locked(conn, region_code) >= int(region["capacity"]):
                    continue
                self._admit_locked(conn, item["id"], payload, sequence, "system", "scheduler",
                                   {"promoted_from_queue": True})
                promoted.append(item["id"])
            conn.execute("COMMIT")
            return {"promoted": promoted}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_queue(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM items WHERE status=? ORDER BY id ASC", (rules.QUEUE_WAIT_STATUS,)
            ).fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    # ---------------------------------------------------------------- commands

    def list_commands(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM valve_commands WHERE item_id=? ORDER BY step ASC", (item_id,)
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_command(self, command_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM valve_commands WHERE id=?", (command_id,)).fetchone()
            if row is None:
                raise NotFoundError("command_not_found", "阀门指令不存在")
            return dict(row)
        finally:
            conn.close()

    def next_command_to_dispatch(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if self._row_to_item(row)["status"] != rules.ISOLATING_STATUS:
                return None
            row = conn.execute(
                "SELECT * FROM valve_commands WHERE item_id=? AND status!=? ORDER BY step ASC LIMIT 1",
                (item_id, "acked"),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def mark_dispatched(self, command_id, attempts):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE valve_commands SET attempts=?, status=?, updated_at=? WHERE id=?",
                (attempts, "sent", now_iso(), command_id),
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def find_command_by_receipt(self, receipt_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM valve_commands WHERE receipt_id=?", (receipt_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def apply_receipt(self, command_id, receipt_id, result, detail):
        """回执幂等：同一回执编号重放不重复执行；已 acked 的指令不再执行；
        失败指令用新回执编号上报，视为断点重试的新结果。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            command = conn.execute("SELECT * FROM valve_commands WHERE id=?", (command_id,)).fetchone()
            if command is None:
                raise NotFoundError("command_not_found", "阀门指令不存在")
            existing = conn.execute(
                "SELECT * FROM valve_commands WHERE receipt_id=? AND id!=?", (receipt_id, command_id)
            ).fetchone()
            if existing is not None:
                raise ConflictError("receipt_id_conflict", "回执编号已用于其它指令")
            # 同一物理回执重复到达，或指令已完成：幂等返回，不改状态、不再执行
            if command["receipt_id"] == receipt_id or command["status"] == "acked":
                conn.execute("COMMIT")
                return {"duplicate": True, "command": self.get_command(command_id)}
            new_status = "acked" if result == "acked" else "failed"
            conn.execute(
                "UPDATE valve_commands SET status=?, receipt_id=?, last_error=?, updated_at=? WHERE id=?",
                (new_status, receipt_id, detail if result == "failed" else "", now_iso(), command_id),
            )
            completed = False
            if result == "acked":
                pending = conn.execute(
                    "SELECT COUNT(*) AS total FROM valve_commands WHERE item_id=? AND status!=?",
                    (command["item_id"], "acked"),
                ).fetchone()["total"]
                if pending == 0:
                    completed = True
                    item_row = conn.execute(
                        "SELECT * FROM items WHERE id=?", (command["item_id"],)
                    ).fetchone()
                    payload = self._row_to_item(item_row)["payload"]
                    payload["isolated_at"] = now_iso()
                    self._bump_version(conn, command["item_id"], payload, rules.ISOLATED_STATUS)
            self.append_audit(
                conn, command["item_id"], "valve_receipt", "gateway", "system",
                {"command_id": command_id, "step": command["step"], "valve_id": command["valve_id"],
                 "receipt_id": receipt_id, "result": result, "sequence_completed": completed},
            )
            conn.execute("COMMIT")
            return {"duplicate": False, "command": self.get_command(command_id), "sequence_completed": completed}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------- readings

    def merge_readings(self, records, actor, role):
        """断网现场阀位批量回网合并；同阀不同阀位并排列出挂起复核，不能直接推进。"""
        conn = self.connect()
        result = []
        conflicts_opened = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                dup = conn.execute(
                    "SELECT id FROM valve_readings WHERE client_id=?", (record["client_id"],)
                ).fetchone()
                if dup is not None:
                    result.append({"client_id": record["client_id"], "status": "duplicate"})
                    continue
                timestamp = now_iso()
                conn.execute(
                    "INSERT INTO valve_readings(client_id,valve_id,position,device_id,observed_at,merged_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (record["client_id"], record["valve_id"], record["position"], record["device_id"],
                     record["observed_at"], timestamp),
                )
                reading_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                other = conn.execute(
                    "SELECT * FROM valve_readings WHERE valve_id=? AND id!=? AND position!=? LIMIT 1",
                    (record["valve_id"], reading_id, record["position"]),
                ).fetchone()
                if other is None:
                    result.append({"client_id": record["client_id"], "status": "merged",
                                   "reading_id": reading_id})
                    continue
                open_conflict = conn.execute(
                    "SELECT * FROM valve_conflicts WHERE valve_id=? AND status=?",
                    (record["valve_id"], rules.OPEN_CONFLICT),
                ).fetchone()
                if open_conflict is None:
                    rows = conn.execute(
                        "SELECT id,client_id,position,device_id,observed_at,merged_at FROM valve_readings "
                        "WHERE valve_id=? ORDER BY observed_at ASC, id ASC", (record["valve_id"],)
                    ).fetchall()
                    conflict_records = [dict(r) for r in rows]
                    conn.execute(
                        "INSERT INTO valve_conflicts(valve_id,status,opened_at,records) VALUES(?,?,?,?)",
                        (record["valve_id"], rules.OPEN_CONFLICT, timestamp, canonical_json(conflict_records)),
                    )
                    conflict_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    conflicts_opened.append(conflict_id)
                    self.append_audit(conn, None, "valve_conflict_opened", actor, role,
                                      {"conflict_id": conflict_id, "valve_id": record["valve_id"],
                                       "records": conflict_records})
                    result.append({"client_id": record["client_id"], "status": "conflict",
                                   "reading_id": reading_id, "conflict_id": conflict_id})
                else:
                    conflict_id = open_conflict["id"]
                    rows = conn.execute(
                        "SELECT id,client_id,position,device_id,observed_at,merged_at FROM valve_readings "
                        "WHERE valve_id=? ORDER BY observed_at ASC, id ASC", (record["valve_id"],)
                    ).fetchall()
                    conn.execute("UPDATE valve_conflicts SET records=? WHERE id=?",
                                 (canonical_json([dict(r) for r in rows]), conflict_id))
                    result.append({"client_id": record["client_id"], "status": "conflict",
                                   "reading_id": reading_id, "conflict_id": conflict_id})
            conn.execute("COMMIT")
            return {"results": result, "conflicts_opened": conflicts_opened}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_conflicts(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute(
                    "SELECT * FROM valve_conflicts WHERE status=? ORDER BY id DESC", (status,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM valve_conflicts ORDER BY id DESC").fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["records"] = json.loads(value["records"])
                result.append(value)
            return result
        finally:
            conn.close()

    def get_conflict(self, conflict_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM valve_conflicts WHERE id=?", (conflict_id,)).fetchone()
            if row is None:
                raise NotFoundError("conflict_not_found", "待复核记录不存在")
            value = dict(row)
            value["records"] = json.loads(value["records"])
            return value
        finally:
            conn.close()

    def open_conflict_valves(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT valve_id FROM valve_conflicts WHERE status=?", (rules.OPEN_CONFLICT,)
            ).fetchall()
            return [row["valve_id"] for row in rows]
        finally:
            conn.close()

    def resolve_conflict(self, conflict_id, resolution, actor, role):
        """复核裁决后关闭冲突；权威阀位作为一条已合并记录落库，随后尝试放行排队任务。"""
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM valve_conflicts WHERE id=?", (conflict_id,)).fetchone()
            if row is None:
                raise NotFoundError("conflict_not_found", "待复核记录不存在")
            if row["status"] != rules.OPEN_CONFLICT:
                conn.execute("COMMIT")
                return {"decision": "already_resolved", "conflict": self.get_conflict(conflict_id)}
            timestamp = now_iso()
            conn.execute(
                "INSERT INTO valve_readings(client_id,valve_id,position,device_id,observed_at,merged_at) "
                "VALUES(?,?,?,?,?,?)",
                ("resolved-%s-%d" % (row["valve_id"], conflict_id), row["valve_id"], resolution, actor,
                 timestamp, timestamp),
            )
            conn.execute(
                "UPDATE valve_conflicts SET status=?,resolved_at=?,resolved_by=?,resolution=? WHERE id=?",
                (rules.RESOLVED_CONFLICT, timestamp, actor, resolution, conflict_id),
            )
            self.append_audit(conn, None, "valve_conflict_resolved", actor, role,
                              {"conflict_id": conflict_id, "valve_id": row["valve_id"], "resolution": resolution})
            conn.execute("COMMIT")
            return {"decision": "resolved", "conflict": self.get_conflict(conflict_id)}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ state

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            regions = []
            for row in conn.execute("SELECT code,name,capacity FROM regions ORDER BY code").fetchall():
                code = row["code"]
                usage = self._region_usage_locked(conn, code)
                value = dict(row)
                value["active"] = usage
                value["available"] = max(0, int(row["capacity"]) - usage)
                regions.append(value)
            queued = [self._row_to_item(r) for r in conn.execute(
                "SELECT * FROM items WHERE status=? ORDER BY id ASC", (rules.QUEUE_WAIT_STATUS,)).fetchall()]
            pending_commands = [dict(r) for r in conn.execute(
                "SELECT * FROM valve_commands WHERE status!=? ORDER BY item_id,step ASC", ("acked",)).fetchall()]
            open_conflicts = [dict(r) for r in conn.execute(
                "SELECT id,valve_id,opened_at FROM valve_conflicts WHERE status=? ORDER BY id",
                (rules.OPEN_CONFLICT,)).fetchall()]
            return {
                "counts": counts,
                "regions": regions,
                "queue": queued,
                "pending_commands": pending_commands,
                "open_conflicts": open_conflicts,
                "items": [self._row_to_item(r) for r in conn.execute(
                    "SELECT * FROM items ORDER BY id DESC").fetchall()],
            }
        finally:
            conn.close()

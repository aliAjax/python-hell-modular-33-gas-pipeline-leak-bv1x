import json
import sqlite3
from datetime import datetime, timezone

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
                CREATE TABLE IF NOT EXISTS districts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    capacity INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS segments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    district_id INTEGER NOT NULL,
                    pipeline_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(district_id) REFERENCES districts(id)
                );
                CREATE TABLE IF NOT EXISTS valves (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT,
                    district_id INTEGER NOT NULL,
                    is_boundary INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(district_id) REFERENCES districts(id)
                );
                CREATE TABLE IF NOT EXISTS valve_shares (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    valve_id INTEGER NOT NULL,
                    district_id INTEGER NOT NULL,
                    UNIQUE(valve_id, district_id),
                    FOREIGN KEY(valve_id) REFERENCES valves(id),
                    FOREIGN KEY(district_id) REFERENCES districts(id)
                );
                CREATE TABLE IF NOT EXISTS isolation_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    district_id INTEGER NOT NULL,
                    segment_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    occupies_capacity INTEGER NOT NULL DEFAULT 0,
                    cross_district INTEGER NOT NULL DEFAULT 0,
                    center_confirmed INTEGER NOT NULL DEFAULT 0,
                    leak_item_id INTEGER,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(district_id) REFERENCES districts(id),
                    FOREIGN KEY(segment_id) REFERENCES segments(id)
                );
                CREATE TABLE IF NOT EXISTS task_valves (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    valve_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    UNIQUE(task_id, sequence),
                    FOREIGN KEY(task_id) REFERENCES isolation_tasks(id),
                    FOREIGN KEY(valve_id) REFERENCES valves(id)
                );
                CREATE TABLE IF NOT EXISTS valve_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    valve_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_result TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES isolation_tasks(id),
                    FOREIGN KEY(valve_id) REFERENCES valves(id)
                );
                CREATE TABLE IF NOT EXISTS valve_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    valve_id INTEGER NOT NULL,
                    task_id INTEGER,
                    position TEXT NOT NULL,
                    source TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    merged INTEGER NOT NULL DEFAULT 0,
                    authoritative INTEGER NOT NULL DEFAULT 0,
                    review_group TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(valve_id) REFERENCES valves(id)
                );
                CREATE TABLE IF NOT EXISTS review_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    valve_id INTEGER NOT NULL,
                    review_group TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    record_a TEXT NOT NULL,
                    record_b TEXT NOT NULL,
                    resolution TEXT,
                    resolved_by TEXT,
                    resolved_at TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES isolation_tasks(id),
                    FOREIGN KEY(valve_id) REFERENCES valves(id)
                );
                """
            )
        finally:
            conn.close()

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
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
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

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()

    # ---------------- 片区台账 ----------------

    def register_district(self, code, name, capacity, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO districts(code,name,capacity,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (code, name, capacity, now_iso(), now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_district", "片区已经存在")
            district_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, None, "district_registered", actor, role, {"code": code, "capacity": capacity})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return {"id": district_id, "code": code, "name": name, "capacity": capacity, "active_count": 0}

    def get_district(self, district_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM districts WHERE id=?", (district_id,)).fetchone()
            if row is None:
                raise NotFoundError("district_not_found", "片区不存在")
            result = dict(row)
            result["active_count"] = self._active_count(conn, district_id)
            return result
        finally:
            conn.close()

    def get_district_by_code(self, code):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM districts WHERE code=?", (code,)).fetchone()
            if row is None:
                raise NotFoundError("district_not_found", "片区不存在: %s" % code)
            result = dict(row)
            result["active_count"] = self._active_count(conn, row["id"])
            return result
        finally:
            conn.close()

    def _active_count(self, conn, district_id):
        return conn.execute(
            "SELECT COUNT(*) AS c FROM isolation_tasks WHERE district_id=? AND occupies_capacity=1",
            (district_id,),
        ).fetchone()["c"]

    def list_districts(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                """
                SELECT d.*,
                       (SELECT COUNT(*) FROM isolation_tasks t
                         WHERE t.district_id=d.id AND t.occupies_capacity=1) AS active_count
                FROM districts d ORDER BY d.id
                """
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def register_segment(self, code, district_code, pipeline_id, actor, role):
        district = self.get_district_by_code(district_code)
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO segments(code,district_id,pipeline_id,created_at) VALUES(?,?,?,?)",
                    (code, district["id"], pipeline_id, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_segment", "管段已经存在")
            segment_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, None, "segment_registered", actor, role, {"code": code, "district": district_code})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return {"id": segment_id, "code": code, "district_id": district["id"],
                "district_code": district_code, "pipeline_id": pipeline_id}

    def get_segment_by_code(self, code):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT s.*, d.code AS district_code FROM segments s JOIN districts d ON d.id=s.district_id WHERE s.code=?",
                (code,),
            ).fetchone()
            if row is None:
                raise NotFoundError("segment_not_found", "管段不存在: %s" % code)
            return dict(row)
        finally:
            conn.close()

    def list_segments(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT s.*, d.code AS district_code FROM segments s JOIN districts d ON d.id=s.district_id ORDER BY s.id"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def register_valve(self, code, name, district_code, is_boundary, shared_with_codes, actor, role):
        district = self.get_district_by_code(district_code)
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO valves(code,name,district_id,is_boundary,created_at) VALUES(?,?,?,?,?)",
                    (code, name, district["id"], 1 if is_boundary else 0, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_valve", "阀门已经存在")
            valve_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            for shared_code in shared_with_codes:
                shared = conn.execute("SELECT id FROM districts WHERE code=?", (shared_code,)).fetchone()
                if shared is None:
                    raise NotFoundError("district_not_found", "共用片区不存在: %s" % shared_code)
                conn.execute(
                    "INSERT OR IGNORE INTO valve_shares(valve_id,district_id) VALUES(?,?)",
                    (valve_id, shared["id"]),
                )
            self.append_audit(conn, None, "valve_registered", actor, role,
                              {"code": code, "district": district_code, "is_boundary": bool(is_boundary)})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return {"id": valve_id, "code": code, "name": name, "district_id": district["id"],
                "district_code": district_code, "is_boundary": bool(is_boundary), "shared_with": list(shared_with_codes)}

    def get_valve_by_code(self, code):
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT v.*, d.code AS district_code FROM valves v JOIN districts d ON d.id=v.district_id WHERE v.code=?",
                (code,),
            ).fetchone()
            if row is None:
                raise NotFoundError("valve_not_found", "阀门不存在: %s" % code)
            result = dict(row)
            result["is_boundary"] = bool(result["is_boundary"])
            shares = conn.execute(
                "SELECT d.code AS c FROM valve_shares vs JOIN districts d ON d.id=vs.district_id WHERE vs.valve_id=?",
                (result["id"],),
            ).fetchall()
            result["shared_with"] = [r["c"] for r in shares]
            return result
        finally:
            conn.close()

    def list_valves(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT v.*, d.code AS district_code FROM valves v JOIN districts d ON d.id=v.district_id ORDER BY v.id"
            ).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["is_boundary"] = bool(value["is_boundary"])
                shares = conn.execute(
                    "SELECT d.code AS c FROM valve_shares vs JOIN districts d ON d.id=vs.district_id WHERE vs.valve_id=?",
                    (value["id"],),
                ).fetchall()
                value["shared_with"] = [r["c"] for r in shares]
                result.append(value)
            return result
        finally:
            conn.close()

    def _valve_district_ids(self, conn, valve_id):
        rows = conn.execute(
            """
            SELECT district_id FROM (
                SELECT district_id FROM valves WHERE id=?
                UNION
                SELECT district_id FROM valve_shares WHERE valve_id=?
            )
            """,
            (valve_id, valve_id),
        ).fetchall()
        return [r["district_id"] for r in rows]

    # ---------------- 隔离任务 ----------------

    def create_isolation_task(self, segment_code, valve_codes, leak_item_id, actor, role, region):
        segment = self.get_segment_by_code(segment_code)
        district_id = segment["district_id"]
        if role != "center":
            if not region:
                raise DomainError("region_required", "需要本片区身份才能开单", 401)
            if region != segment["district_code"]:
                raise DomainError("region_overstep", "不能越权处理其他片区的管段", 403)
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            valve_ids = []
            involved = set()
            for code in valve_codes:
                valve = conn.execute("SELECT id FROM valves WHERE code=?", (code,)).fetchone()
                if valve is None:
                    raise NotFoundError("valve_not_found", "阀门不存在: %s" % code)
                valve_ids.append(valve["id"])
                involved.update(self._valve_district_ids(conn, valve["id"]))
            cross_district = len(involved) > 1
            capacity = conn.execute("SELECT capacity FROM districts WHERE id=?", (district_id,)).fetchone()["capacity"]
            active_count = conn.execute(
                "SELECT COUNT(*) AS c FROM isolation_tasks WHERE district_id=? AND occupies_capacity=1",
                (district_id,),
            ).fetchone()["c"]
            seq = conn.execute("SELECT COUNT(*) AS c FROM isolation_tasks").fetchone()["c"] + 1
            code = "IT-%d" % seq
            if cross_district:
                status, occupies = "pending_center", 0
            elif active_count >= capacity:
                status, occupies = "queued", 0
            else:
                status, occupies = "active", 1
            payload = {"segment_code": segment_code, "valve_codes": list(valve_codes), "cross_district": cross_district}
            conn.execute(
                """INSERT INTO isolation_tasks(code,district_id,segment_id,status,occupies_capacity,cross_district,
                     center_confirmed,leak_item_id,version,payload,created_by,created_role,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,1,?,?,?,?,?)""",
                (code, district_id, segment["id"], status, occupies, 1 if cross_district else 0, 0,
                 leak_item_id, canonical_json(payload), actor, role, now_iso(), now_iso()),
            )
            task_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            for idx, vid in enumerate(valve_ids, start=1):
                conn.execute(
                    "INSERT INTO task_valves(task_id,valve_id,sequence) VALUES(?,?,?)",
                    (task_id, vid, idx),
                )
            self.append_audit(conn, task_id, "task_created", actor, role,
                              {"code": code, "status": status, "cross_district": cross_district})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_isolation_task(task_id)

    def _task_row_to_dict(self, row):
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        result["occupies_capacity"] = bool(result["occupies_capacity"])
        result["cross_district"] = bool(result["cross_district"])
        result["center_confirmed"] = bool(result["center_confirmed"])
        return result

    def _commands_for_task(self, conn, task_id):
        rows = conn.execute(
            """SELECT c.*, v.code AS valve_code FROM valve_commands c
               JOIN valves v ON v.id=c.valve_id
               WHERE c.task_id=? ORDER BY c.sequence""",
            (task_id,),
        ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            if value.get("last_result"):
                try:
                    value["last_result"] = json.loads(value["last_result"])
                except (ValueError, TypeError):
                    pass
            result.append(value)
        return result

    def _reviews_for_task(self, conn, task_id, status=None):
        if status:
            rows = conn.execute(
                "SELECT * FROM review_records WHERE task_id=? AND status=? ORDER BY id",
                (task_id, status),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM review_records WHERE task_id=? ORDER BY id",
                (task_id,),
            ).fetchall()
        result = []
        for row in rows:
            value = dict(row)
            value["record_a"] = json.loads(value["record_a"])
            value["record_b"] = json.loads(value["record_b"])
            result.append(value)
        return result

    def get_isolation_task(self, task_id):
        conn = self.connect()
        try:
            row = conn.execute(
                """SELECT t.*, d.code AS district_code, s.code AS segment_code
                   FROM isolation_tasks t
                   JOIN districts d ON d.id=t.district_id
                   JOIN segments s ON s.id=t.segment_id
                   WHERE t.id=?""",
                (task_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("task_not_found", "隔离任务不存在")
            result = self._task_row_to_dict(row)
            valves = conn.execute(
                """SELECT tv.sequence, v.code, v.id AS valve_id
                   FROM task_valves tv JOIN valves v ON v.id=tv.valve_id
                   WHERE tv.task_id=? ORDER BY tv.sequence""",
                (task_id,),
            ).fetchall()
            result["valves"] = [dict(v) for v in valves]
            result["commands"] = self._commands_for_task(conn, task_id)
            result["reviews"] = self._reviews_for_task(conn, task_id)
            return result
        finally:
            conn.close()

    def list_isolation_tasks(self, district_code=None, status=None):
        conn = self.connect()
        try:
            sql = """SELECT t.*, d.code AS district_code, s.code AS segment_code
                     FROM isolation_tasks t
                     JOIN districts d ON d.id=t.district_id
                     JOIN segments s ON s.id=t.segment_id WHERE 1=1"""
            params = []
            if district_code:
                sql += " AND d.code=?"
                params.append(district_code)
            if status:
                sql += " AND t.status=?"
                params.append(status)
            sql += " ORDER BY t.id DESC"
            rows = conn.execute(sql, params).fetchall()
            return [self._task_row_to_dict(row) for row in rows]
        finally:
            conn.close()

    def confirm_cross_district(self, task_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM isolation_tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("task_not_found", "隔离任务不存在")
            if row["status"] != "pending_center":
                raise DomainError("invalid_task_state", "当前状态不需要中心确认")
            district_id = row["district_id"]
            capacity = conn.execute("SELECT capacity FROM districts WHERE id=?", (district_id,)).fetchone()["capacity"]
            active_count = conn.execute(
                "SELECT COUNT(*) AS c FROM isolation_tasks WHERE district_id=? AND occupies_capacity=1",
                (district_id,),
            ).fetchone()["c"]
            if active_count >= capacity:
                new_status, occupies = "queued", 0
            else:
                new_status, occupies = "active", 1
            conn.execute(
                "UPDATE isolation_tasks SET status=?, occupies_capacity=?, center_confirmed=1, version=version+1, updated_at=? WHERE id=?",
                (new_status, occupies, now_iso(), task_id),
            )
            self.append_audit(conn, task_id, "center_confirmed", actor, role, {"status": new_status})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_isolation_task(task_id)

    def _promote_queued(self, conn, district_id):
        row = conn.execute(
            "SELECT id FROM isolation_tasks WHERE district_id=? AND status='queued' ORDER BY id LIMIT 1",
            (district_id,),
        ).fetchone()
        if row is not None:
            conn.execute(
                "UPDATE isolation_tasks SET status='active', occupies_capacity=1, version=version+1, updated_at=? WHERE id=?",
                (now_iso(), row["id"]),
            )
            self.append_audit(conn, row["id"], "task_activated", "system", "system", {"reason": "capacity_freed"})

    def complete_task(self, task_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM isolation_tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("task_not_found", "隔离任务不存在")
            if row["status"] != "active":
                raise DomainError("invalid_task_state", "只有进行中的任务可以完工")
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM valve_commands WHERE task_id=? AND status!='succeeded'",
                (task_id,),
            ).fetchone()["c"]
            if pending > 0:
                raise DomainError("commands_incomplete", "还有阀门指令未完成，不能完工", 409)
            if self._has_pending_review(conn, task_id):
                raise DomainError("review_pending", "阀门位存在待复核记录，不能完工", 409)
            conn.execute(
                "UPDATE isolation_tasks SET status='completed', occupies_capacity=0, version=version+1, updated_at=? WHERE id=?",
                (now_iso(), task_id),
            )
            self.append_audit(conn, task_id, "task_completed", actor, role, {})
            self._promote_queued(conn, row["district_id"])
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_isolation_task(task_id)

    def cancel_task(self, task_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM isolation_tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("task_not_found", "隔离任务不存在")
            if row["status"] not in ("active", "queued", "pending_center"):
                raise DomainError("invalid_task_state", "当前状态不能取消")
            freed = row["occupies_capacity"]
            conn.execute(
                "UPDATE isolation_tasks SET status='cancelled', occupies_capacity=0, version=version+1, updated_at=? WHERE id=?",
                (now_iso(), task_id),
            )
            self.append_audit(conn, task_id, "task_cancelled", actor, role, {})
            if freed:
                self._promote_queued(conn, row["district_id"])
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_isolation_task(task_id)

    # ---------------- 阀门指令 ----------------

    def issue_commands(self, task_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM isolation_tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError("task_not_found", "隔离任务不存在")
            if row["status"] != "active":
                raise DomainError("invalid_task_state", "只有进行中的任务可以下发阀门指令")
            if self._has_pending_review(conn, task_id):
                raise DomainError("review_pending", "阀门位存在待复核记录，不能下发指令", 409)
            valves = conn.execute(
                "SELECT valve_id, sequence FROM task_valves WHERE task_id=? ORDER BY sequence",
                (task_id,),
            ).fetchall()
            for valve in valves:
                key = "cmd:%d:%d" % (task_id, valve["sequence"])
                conn.execute(
                    """INSERT OR IGNORE INTO valve_commands(task_id,valve_id,sequence,command,status,
                         idempotency_key,attempts,created_at,updated_at)
                       VALUES(?,?,?, 'close','pending',?,0,?,?)""",
                    (task_id, valve["valve_id"], valve["sequence"], key, now_iso(), now_iso()),
                )
            self.append_audit(conn, task_id, "commands_issued", actor, role, {"count": len(valves)})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_isolation_task(task_id)

    def list_commands(self, task_id):
        conn = self.connect()
        try:
            return self._commands_for_task(conn, task_id)
        finally:
            conn.close()

    def get_command(self, command_id):
        conn = self.connect()
        try:
            row = conn.execute(
                """SELECT c.*, v.code AS valve_code, t.code AS task_code
                   FROM valve_commands c
                   JOIN valves v ON v.id=c.valve_id
                   JOIN isolation_tasks t ON t.id=c.task_id
                   WHERE c.id=?""",
                (command_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError("command_not_found", "阀门指令不存在")
            result = dict(row)
            if result.get("last_result"):
                try:
                    result["last_result"] = json.loads(result["last_result"])
                except (ValueError, TypeError):
                    pass
            return result
        finally:
            conn.close()

    def ack_command(self, command_id, result, error, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM valve_commands WHERE id=?", (command_id,)).fetchone()
            if row is None:
                raise NotFoundError("command_not_found", "阀门指令不存在")
            task_id = row["task_id"]
            if row["status"] == "succeeded":
                conn.execute("COMMIT")
                return self.get_command(command_id)
            breakpoint = conn.execute(
                "SELECT MIN(sequence) AS s FROM valve_commands WHERE task_id=? AND status!='succeeded'",
                (task_id,),
            ).fetchone()["s"]
            if breakpoint is not None and row["sequence"] > breakpoint:
                raise DomainError("command_out_of_order", "前序阀门指令尚未完成，不能回执", 409)
            if result == "succeeded":
                new_status = "succeeded"
                last_result = json.dumps({"result": "succeeded"}, ensure_ascii=False)
            else:
                new_status = "failed"
                last_result = json.dumps({"result": "failed", "error": error or "valve command failed"}, ensure_ascii=False)
            conn.execute(
                "UPDATE valve_commands SET status=?, attempts=attempts+1, last_result=?, updated_at=? WHERE id=?",
                (new_status, last_result, now_iso(), command_id),
            )
            self.append_audit(conn, task_id, "command_acked", actor, role,
                              {"command_id": command_id, "result": new_status})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_command(command_id)

    def retry_command(self, command_id, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM valve_commands WHERE id=?", (command_id,)).fetchone()
            if row is None:
                raise NotFoundError("command_not_found", "阀门指令不存在")
            if row["status"] != "failed":
                raise DomainError("command_not_failed", "只有失败的指令需要重试", 409)
            conn.execute(
                "UPDATE valve_commands SET status='pending', updated_at=? WHERE id=?",
                (now_iso(), command_id),
            )
            self.append_audit(conn, row["task_id"], "command_retried", actor, role, {"command_id": command_id})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_command(command_id)

    # ---------------- 断网阀位合并与复核 ----------------

    def _has_pending_review(self, conn, task_id):
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM review_records WHERE task_id=? AND status='pending'",
            (task_id,),
        ).fetchone()
        return row["c"] > 0

    def list_reviews(self, task_id, status=None):
        conn = self.connect()
        try:
            return self._reviews_for_task(conn, task_id, status)
        finally:
            conn.close()

    def merge_positions(self, task_id, records, actor, role):
        conn = self.connect()
        reviews_created = []
        merged_count = 0
        try:
            conn.execute("BEGIN IMMEDIATE")
            task = conn.execute("SELECT * FROM isolation_tasks WHERE id=?", (task_id,)).fetchone()
            if task is None:
                raise NotFoundError("task_not_found", "隔离任务不存在")
            if task["status"] not in ("active", "queued"):
                raise DomainError("invalid_task_state", "当前任务状态不能合并阀位记录")
            for rec in records:
                valve = conn.execute("SELECT id FROM valves WHERE code=?", (rec["valve_code"],)).fetchone()
                if valve is None:
                    raise NotFoundError("valve_not_found", "阀门不存在: %s" % rec["valve_code"])
                valve_id = valve["id"]
                position = rec["position"]
                source = rec.get("source", "offline")
                recorded_at = rec["recorded_at"]
                payload = {"valve_code": rec["valve_code"], "position": position, "source": source,
                           "recorded_at": recorded_at, "note": rec.get("note", "")}
                current = conn.execute(
                    """SELECT * FROM valve_positions
                       WHERE valve_id=? AND authoritative=1
                       ORDER BY recorded_at DESC, id DESC LIMIT 1""",
                    (valve_id,),
                ).fetchone()
                if current is not None and current["position"] != position:
                    review_group = "RV-%d-%d" % (task_id, valve_id)
                    conn.execute(
                        """INSERT INTO valve_positions(valve_id,task_id,position,source,recorded_at,merged,
                             authoritative,review_group,payload,created_at)
                           VALUES(?,?,?,?,?,1,0,?,?,?)""",
                        (valve_id, task_id, position, source, recorded_at, review_group,
                         canonical_json(payload), now_iso()),
                    )
                    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    record_b = dict(payload)
                    record_b["source_record_id"] = new_id
                    record_a = json.loads(current["payload"])
                    record_a["source_record_id"] = current["id"]
                    conn.execute(
                        """INSERT INTO review_records(task_id,valve_id,review_group,status,record_a,record_b,created_at)
                           VALUES(?,?,?, 'pending',?,?,?)""",
                        (task_id, valve_id, review_group, canonical_json(record_a),
                         canonical_json(record_b), now_iso()),
                    )
                    conn.execute("UPDATE valve_positions SET authoritative=0 WHERE id=?", (current["id"],))
                    reviews_created.append({"review_group": review_group, "valve_code": rec["valve_code"]})
                else:
                    conn.execute(
                        """INSERT INTO valve_positions(valve_id,task_id,position,source,recorded_at,merged,
                             authoritative,review_group,payload,created_at)
                           VALUES(?,?,?,?,?,1,1,NULL,?,?)""",
                        (valve_id, task_id, position, source, recorded_at,
                         canonical_json(payload), now_iso()),
                    )
                    merged_count += 1
            self.append_audit(conn, task_id, "positions_merged", actor, role,
                              {"merged": merged_count, "reviews": reviews_created})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return {"merged": merged_count, "reviews_created": reviews_created,
                "reviews": self.list_reviews(task_id, "pending")}

    def resolve_review(self, review_id, choice, actor, role):
        conn = self.connect()
        task_id = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM review_records WHERE id=?", (review_id,)).fetchone()
            if row is None:
                raise NotFoundError("review_not_found", "复核记录不存在")
            task_id = row["task_id"]
            if row["status"] != "pending":
                raise DomainError("review_already_resolved", "该复核已处理")
            if choice not in ("a", "b"):
                raise DomainError("invalid_choice", "选择必须是 a 或 b")
            record = json.loads(row["record_a"] if choice == "a" else row["record_b"])
            source_record_id = record.get("source_record_id")
            if source_record_id:
                conn.execute(
                    "UPDATE valve_positions SET authoritative=1 WHERE id=?",
                    (source_record_id,),
                )
            other = "b" if choice == "a" else "a"
            other_record = json.loads(row["record_%s" % other])
            if other_record.get("source_record_id"):
                conn.execute(
                    "UPDATE valve_positions SET authoritative=0 WHERE id=?",
                    (other_record["source_record_id"],),
                )
            conn.execute(
                "UPDATE review_records SET status='resolved', resolution=?, resolved_by=?, resolved_at=? WHERE id=?",
                (choice, actor, now_iso(), review_id),
            )
            self.append_audit(conn, task_id, "review_resolved", actor, role,
                              {"review_id": review_id, "choice": choice})
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.get_isolation_task(task_id)

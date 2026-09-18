"""配额裁决核心逻辑：多级令牌桶、幂等存档与撤销。

所有公共写操作都必须在 database.write_tx（BEGIN IMMEDIATE）事务中调用，
保证「补充 -> 判足 -> 扣减」这一读-改-写序列的原子性与隔离性。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import HTTPException

# 各策略层的空标识统一用空串存储（SQLite STRICT 主键列不允许 NULL 参与唯一比较）
EMPTY = ""


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """统一的 ISO 8601 UTC 文本，例如 2026-09-17T12:00:00.123456Z。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def payload_hash(payload: dict[str, Any]) -> str:
    """对除 request_id 外的裁决载荷做规范序列化后取 SHA-256。"""
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- 策略管理


def _normalize_scope(
    scope: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
) -> tuple[str, str]:
    """把 (scope, tenant_id, route_id) 规范化成存储用的 (tenant, route)。

    非法组合直接抛 422；各层空标识统一用空串。
    """
    tenant = tenant_id or EMPTY
    route = route_id or EMPTY
    if scope == "global":
        return EMPTY, EMPTY
    if scope == "tenant":
        if not tenant:
            raise HTTPException(422, "tenant 作用域必须提供 tenant_id")
        return tenant, EMPTY
    if scope == "route":
        if not tenant or not route:
            raise HTTPException(422, "route 作用域必须同时提供 tenant_id 与 route_id")
        return tenant, route
    raise HTTPException(422, f"非法 scope: {scope!r}")


def _validate_policy_values(
    capacity: float,
    refill_rate: float,
    low_balance_threshold: Optional[float],
) -> None:
    if capacity < 0 or refill_rate < 0:
        raise HTTPException(422, "capacity 与 refill_rate 必须非负")
    if low_balance_threshold is not None and not (
        0.0 <= low_balance_threshold <= capacity
    ):
        raise HTTPException(
            422, "low_balance_threshold 必须为 null（关闭）或在 0 到 capacity 之间"
        )


def list_policies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT scope, tenant_id, route_id, capacity, refill_rate, "
        "low_balance_threshold, updated_at "
        "FROM policies ORDER BY scope, tenant_id, route_id"
    ).fetchall()
    return [_policy_out(r) for r in rows]


def _pending_change(
    conn: sqlite3.Connection, scope: str, tenant: str, route: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT change_id, effective_at FROM policy_changes "
        "WHERE scope=? AND tenant_id=? AND route_id=? AND status='pending'",
        (scope, tenant, route),
    ).fetchone()


def upsert_policy(
    conn: sqlite3.Connection,
    *,
    scope: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    capacity: float,
    refill_rate: float,
    low_balance_threshold: Optional[float] = None,
) -> dict[str, Any]:
    """创建或更新一层策略。

    更新容量时，已存在的桶先按旧策略补充到当前时刻，再截断到新容量，
    保证余额永远不超过当前容量。
    low_balance_threshold 为 None 时关闭该层的低余额事件；否则须在
    [0, capacity] 内。PUT 为全量替换：不传即视为关闭。

    该策略存在待生效（pending）的定时变更时拒绝直接更新，返回 409：
    两条更新路径并发交错会破坏变更应用时的「旧策略 -> 新策略」余额换算，
    请先取消变更或等其生效后再更新。
    """
    tenant, route = _normalize_scope(scope, tenant_id, route_id)
    _validate_policy_values(capacity, refill_rate, low_balance_threshold)

    pending = _pending_change(conn, scope, tenant, route)
    if pending is not None:
        raise HTTPException(
            409,
            f"该策略存在待生效的定时变更 change_id={pending['change_id']!r}"
            f"（effective_at={pending['effective_at']}），请先取消或等其生效后再更新",
        )

    ts = now_utc()
    old = conn.execute(
        "SELECT capacity, refill_rate FROM policies "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()

    bucket = conn.execute(
        "SELECT balance, updated_at FROM buckets "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if bucket is not None:
        # 用旧策略（若存在）先把应补充的令牌补上，再按新容量截断
        cap_old = old["capacity"] if old else capacity
        rate_old = old["refill_rate"] if old else refill_rate
        balance = _refill(bucket["balance"], bucket["updated_at"], cap_old, rate_old, ts)
        balance = min(balance, capacity)
        conn.execute(
            "UPDATE buckets SET balance=?, updated_at=? "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (balance, iso(ts), scope, tenant, route),
        )

    conn.execute(
        """
        INSERT INTO policies
            (scope, tenant_id, route_id, capacity, refill_rate,
             low_balance_threshold, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (scope, tenant_id, route_id) DO UPDATE SET
            capacity=excluded.capacity,
            refill_rate=excluded.refill_rate,
            low_balance_threshold=excluded.low_balance_threshold,
            updated_at=excluded.updated_at
        """,
        (
            scope, tenant, route, float(capacity), float(refill_rate),
            None if low_balance_threshold is None else float(low_balance_threshold),
            iso(ts),
        ),
    )
    # 每次 PUT 成功都在同一事务追加一条不可变版本（source=put）
    _append_policy_version(
        conn,
        scope=scope, tenant=tenant, route=route,
        capacity=capacity, refill_rate=refill_rate,
        low_balance_threshold=low_balance_threshold,
        source="put", created_at=iso(ts),
    )
    row = conn.execute(
        "SELECT scope, tenant_id, route_id, capacity, refill_rate, "
        "low_balance_threshold, updated_at "
        "FROM policies WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    return _policy_out(row)


# ------------------------------------------------------------ 策略版本审计


_VERSION_COLS = (
    "version, scope, tenant_id, route_id, capacity, refill_rate, "
    "low_balance_threshold, source, change_id, restored_from_version, created_at"
)


def _version_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "version": row["version"],
        "scope": row["scope"],
        "tenant_id": row["tenant_id"] or None,
        "route_id": row["route_id"] or None,
        "capacity": row["capacity"],
        "refill_rate": row["refill_rate"],
        "low_balance_threshold": row["low_balance_threshold"],
        "source": row["source"],
        "change_id": row["change_id"],
        "restored_from_version": row["restored_from_version"],
        "created_at": row["created_at"],
    }


def _next_version(
    conn: sqlite3.Connection, scope: str, tenant: str, route: str
) -> int:
    """在写事务内分配该策略的下一个版本号（max(version)+1，首个为 1）。

    所有调用方都在 BEGIN IMMEDIATE 事务内，写锁已排他持有，
    max 读与随后的插入之间不会有其他写者插入。
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM policy_versions "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    return int(row["v"]) + 1


def _append_policy_version(
    conn: sqlite3.Connection,
    *,
    scope: str,
    tenant: str,
    route: str,
    capacity: float,
    refill_rate: float,
    low_balance_threshold: Optional[float],
    source: str,
    created_at: str,
    change_id: Optional[str] = None,
    restored_from_version: Optional[int] = None,
) -> dict[str, Any]:
    """在当前写事务内追加一条不可变版本记录并返回它。

    与触发变更（PUT 成功 / 定时切换生效 / 回滚成功）在同一事务内提交，
    保证「策略参数已变」与「版本已落账」要么同时可见要么都不可见。
    """
    version = _next_version(conn, scope, tenant, route)
    conn.execute(
        """
        INSERT INTO policy_versions
            (scope, tenant_id, route_id, version, capacity, refill_rate,
             low_balance_threshold, source, change_id, restored_from_version,
             created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scope, tenant, route, version, float(capacity), float(refill_rate),
            None if low_balance_threshold is None else float(low_balance_threshold),
            source, change_id, restored_from_version, created_at,
        ),
    )
    row = conn.execute(
        f"SELECT {_VERSION_COLS} FROM policy_versions "
        "WHERE scope=? AND tenant_id=? AND route_id=? AND version=?",
        (scope, tenant, route, version),
    ).fetchone()
    return _version_out(row)


def list_policy_versions(
    conn: sqlite3.Connection,
    *,
    scope: str,
    tenant_id: Optional[str] = None,
    route_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """按策略键返回版本历史，新版本在前（version 降序）。只读。"""
    tenant, route = _normalize_scope(scope, tenant_id, route_id)
    rows = conn.execute(
        f"SELECT {_VERSION_COLS} FROM policy_versions "
        "WHERE scope=? AND tenant_id=? AND route_id=? "
        "ORDER BY version DESC, id DESC",
        (scope, tenant, route),
    ).fetchall()
    return [_version_out(r) for r in rows]


def rollback_policy(
    conn: sqlite3.Connection,
    *,
    scope: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    version: int,
) -> dict[str, Any]:
    """在同一写事务内把策略恢复到指定历史版本，并追加一条 source=rollback 版本。

    约束：目标策略不存在 -> 404；目标版本不属于该策略 -> 404；
    该策略存在 pending 定时变更 -> 409（避免与到期应用交错）。
    桶按 PUT 相同规则处理：先按当前策略补充到事务时刻，再按目标容量截断。
    """
    tenant, route = _normalize_scope(scope, tenant_id, route_id)

    pending = _pending_change(conn, scope, tenant, route)
    if pending is not None:
        raise HTTPException(
            409,
            f"该策略存在待生效的定时变更 change_id={pending['change_id']!r}"
            f"（effective_at={pending['effective_at']}），请先取消或等其生效后再回滚",
        )

    current = conn.execute(
        "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if current is None:
        raise HTTPException(404, "目标策略不存在，请先用 PUT /policies 创建")

    target = conn.execute(
        f"SELECT {_VERSION_COLS} FROM policy_versions "
        "WHERE scope=? AND tenant_id=? AND route_id=? AND version=?",
        (scope, tenant, route, version),
    ).fetchone()
    if target is None:
        raise HTTPException(
            404, f"版本 version={version} 不属于该策略，无法回滚"
        )

    cap = float(target["capacity"])
    rate = float(target["refill_rate"])
    threshold = target["low_balance_threshold"]

    ts = now_utc()
    # 已有桶：先按当前策略补充到事务时刻，再截断到目标版本容量
    bucket = conn.execute(
        "SELECT balance, updated_at FROM buckets "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if bucket is not None:
        balance = _refill(
            bucket["balance"], bucket["updated_at"],
            current["capacity"], current["refill_rate"], ts,
        )
        balance = min(balance, cap)
        conn.execute(
            "UPDATE buckets SET balance=?, updated_at=? "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (balance, iso(ts), scope, tenant, route),
        )

    conn.execute(
        """
        UPDATE policies
        SET capacity=?, refill_rate=?, low_balance_threshold=?, updated_at=?
        WHERE scope=? AND tenant_id=? AND route_id=?
        """,
        (cap, rate, threshold, iso(ts), scope, tenant, route),
    )
    return _append_policy_version(
        conn,
        scope=scope, tenant=tenant, route=route,
        capacity=cap, refill_rate=rate, low_balance_threshold=threshold,
        source="rollback", created_at=iso(ts),
        restored_from_version=version,
    )


def _policy_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "scope": row["scope"],
        "tenant_id": row["tenant_id"] or None,
        "route_id": row["route_id"] or None,
        "capacity": row["capacity"],
        "refill_rate": row["refill_rate"],
        "low_balance_threshold": row["low_balance_threshold"],
        "updated_at": row["updated_at"],
    }


# ------------------------------------------------------------ 策略定时切换


def _change_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "change_id": row["change_id"],
        "scope": row["scope"],
        "tenant_id": row["tenant_id"] or None,
        "route_id": row["route_id"] or None,
        "capacity": row["new_capacity"],
        "refill_rate": row["new_refill_rate"],
        "low_balance_threshold": row["new_low_balance_threshold"],
        "effective_at": row["effective_at"],
        "status": row["status"],
        "created_at": row["created_at"],
        "applied_at": row["applied_at"],
        "cancelled_at": row["cancelled_at"],
    }


_CHANGE_COLS = (
    "change_id, scope, tenant_id, route_id, new_capacity, new_refill_rate, "
    "new_low_balance_threshold, effective_at, status, created_at, "
    "applied_at, cancelled_at"
)


def create_policy_change(
    conn: sqlite3.Connection,
    *,
    change_id: str,
    scope: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    capacity: float,
    refill_rate: float,
    low_balance_threshold: Optional[float],
    effective_at: datetime,
) -> dict[str, Any]:
    """为已存在的策略登记一条未来生效的容量/速率/阈值变更。

    约束：目标策略必须已存在（404）；effective_at 必须是未来 UTC 时刻（422）；
    同一策略至多一条 pending 变更（409）；change_id 全局唯一（409）。
    """
    tenant, route = _normalize_scope(scope, tenant_id, route_id)
    _validate_policy_values(capacity, refill_rate, low_balance_threshold)
    if effective_at.tzinfo is None:
        raise HTTPException(422, "effective_at 必须显式带时区（UTC）")
    effective_at = effective_at.astimezone(timezone.utc)

    ts = now_utc()
    if not effective_at > ts:
        raise HTTPException(422, "effective_at 必须是晚于当前时间的未来 UTC 时刻")

    policy = conn.execute(
        "SELECT 1 FROM policies WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if policy is None:
        raise HTTPException(404, "目标策略不存在，请先用 PUT /policies 创建")

    dup = conn.execute(
        f"SELECT {_CHANGE_COLS} FROM policy_changes WHERE change_id=?",
        (change_id,),
    ).fetchone()
    if dup is not None:
        raise HTTPException(409, f"change_id={change_id!r} 已存在")
    if _pending_change(conn, scope, tenant, route) is not None:
        raise HTTPException(409, "同一策略已存在待生效变更，取消或等其生效后才能提交新的")

    conn.execute(
        """
        INSERT INTO policy_changes
            (change_id, scope, tenant_id, route_id, new_capacity, new_refill_rate,
             new_low_balance_threshold, effective_at, status, created_at,
             applied_at, cancelled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL)
        """,
        (
            change_id, scope, tenant, route, float(capacity), float(refill_rate),
            None if low_balance_threshold is None else float(low_balance_threshold),
            iso(effective_at), iso(ts),
        ),
    )
    row = conn.execute(
        f"SELECT {_CHANGE_COLS} FROM policy_changes WHERE change_id=?",
        (change_id,),
    ).fetchone()
    return _change_out(row)


def list_policy_changes(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    scope: Optional[str] = None,
    tenant_id: Optional[str] = None,
    route_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """列出定时变更，可按 status 与策略键过滤；按提交顺序稳定返回。"""
    sql = f"SELECT {_CHANGE_COLS} FROM policy_changes"
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append("status=?")
        params.append(status)
    if scope is not None:
        where.append("scope=?")
        params.append(scope)
    if tenant_id is not None:
        where.append("tenant_id=?")
        params.append(tenant_id or EMPTY)
    if route_id is not None:
        where.append("route_id=?")
        params.append(route_id or EMPTY)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id"
    return [_change_out(r) for r in conn.execute(sql, params).fetchall()]


def get_policy_change(conn: sqlite3.Connection, change_id: str) -> dict[str, Any]:
    row = conn.execute(
        f"SELECT {_CHANGE_COLS} FROM policy_changes WHERE change_id=?",
        (change_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, f"change_id={change_id!r} 不存在")
    return _change_out(row)


def cancel_policy_change(conn: sqlite3.Connection, change_id: str) -> dict[str, Any]:
    """取消一条尚未生效的 pending 变更（重复取消幂等）。

    不存在 -> 404；已生效或已到生效时刻 -> 409；
    已取消 -> 原样返回该 cancelled 记录。
    """
    row = conn.execute(
        f"SELECT {_CHANGE_COLS} FROM policy_changes WHERE change_id=?",
        (change_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, f"change_id={change_id!r} 不存在")
    if row["status"] == "applied":
        raise HTTPException(
            409, f"change_id={change_id!r} 已生效，无法取消"
        )
    if row["status"] == "cancelled":
        return _change_out(row)

    ts = now_utc()
    # 双保险：effective_at 已到期的变更可能正被/已被裁决事务应用，
    # 只取消仍为 pending 且生效时刻仍在未来的行
    cur = conn.execute(
        "UPDATE policy_changes SET status='cancelled', cancelled_at=? "
        "WHERE change_id=? AND status='pending' AND effective_at > ?",
        (iso(ts), change_id, iso(ts)),
    )
    if cur.rowcount == 0:
        raise HTTPException(409, f"change_id={change_id!r} 已到生效时刻，无法取消")
    row = conn.execute(
        f"SELECT {_CHANGE_COLS} FROM policy_changes WHERE change_id=?",
        (change_id,),
    ).fetchone()
    return _change_out(row)


# ---------------------------------------------------------------- 令牌桶


def _refill(
    balance: float,
    updated_at: str,
    capacity: float,
    refill_rate: float,
    now: datetime,
) -> float:
    elapsed = max(0.0, (now - parse_iso(updated_at)).total_seconds())
    return min(float(capacity), float(balance) + elapsed * float(refill_rate))


def _bucket_out(
    scope: str,
    tenant: str,
    route: str,
    capacity: float,
    refill_rate: float,
    balance: float,
) -> dict[str, Any]:
    return {
        "scope": scope,
        "tenant_id": tenant or None,
        "route_id": route or None,
        "capacity": float(capacity),
        "refill_rate": float(refill_rate),
        "balance": float(balance),
    }


def list_buckets(conn: sqlite3.Connection, *, live: bool = True) -> list[dict[str, Any]]:
    """列出所有已创建的桶；live=True 时展示补充到当前时刻的余额（不落库）。"""
    rows = conn.execute(
        """
        SELECT b.scope, b.tenant_id, b.route_id, b.balance, b.updated_at,
               p.capacity, p.refill_rate
        FROM buckets b JOIN policies p
          ON p.scope=b.scope AND p.tenant_id=b.tenant_id AND p.route_id=b.route_id
        ORDER BY b.scope, b.tenant_id, b.route_id
        """
    ).fetchall()
    now = now_utc()
    out: list[dict[str, Any]] = []
    for r in rows:
        balance = (
            _refill(r["balance"], r["updated_at"], r["capacity"], r["refill_rate"], now)
            if live
            else r["balance"]
        )
        out.append(
            _bucket_out(
                r["scope"], r["tenant_id"], r["route_id"],
                r["capacity"], r["refill_rate"], balance,
            )
        )
    return out


# ---------------------------------------------------------------- 裁决


def _hit_keys(
    tenant_id: Optional[str], route_id: Optional[str]
) -> list[tuple[str, str, str]]:
    """一次裁决按 全局 -> 租户 -> 路由 顺序命中的策略键。"""
    keys: list[tuple[str, str, str]] = [("global", EMPTY, EMPTY)]
    if tenant_id:
        keys.append(("tenant", tenant_id, EMPTY))
        if route_id:
            keys.append(("route", tenant_id, route_id))
    return keys


def _apply_due_changes(
    conn: sqlite3.Connection,
    keys: list[tuple[str, str, str]],
    now: datetime,
) -> None:
    """在裁决事务内、裁决之前，应用所有命中且已到期（effective_at <= now）的变更。

    逐条串行应用：每条都在同一事务内完成「余额换算 -> 覆盖策略 -> 标记 applied」，
    因此并发请求只能观察到完整的旧状态或完整的新状态，不会看到半成品。
    """
    now_iso = iso(now)
    for scope, tenant, route in keys:
        rows = conn.execute(
            """
            SELECT change_id, scope, tenant_id, route_id, new_capacity,
                   new_refill_rate, new_low_balance_threshold, effective_at
            FROM policy_changes
            WHERE scope=? AND tenant_id=? AND route_id=?
              AND status='pending' AND effective_at <= ?
            ORDER BY effective_at, id
            """,
            (scope, tenant, route, now_iso),
        ).fetchall()
        for ch in rows:
            _apply_policy_change(conn, ch, now)


def _apply_policy_change(
    conn: sqlite3.Connection, ch: sqlite3.Row, now: datetime
) -> None:
    """应用单条变更：旧策略补充到 effective_at -> 按比例缩放余额 ->
    覆盖为新策略 -> 按新速率从 effective_at 补充至 now（由随后的裁决补充完成）。

    余额换算（余额为「剩余比例」语义）：
      1. 先按旧容量/旧速率把余额补充到 effective_at（上限旧容量）；
      2. 旧容量为 0 时余额直接置 0，否则
         balance = 旧余额 / 旧容量 × 新容量（不超过新容量）；
      3. 桶的 updated_at 置为 effective_at，之后统一按新速率继续补充。
    """
    scope, tenant, route = ch["scope"], ch["tenant_id"], ch["route_id"]
    effective_at = parse_iso(ch["effective_at"])
    eff_iso = ch["effective_at"]
    new_cap = float(ch["new_capacity"])
    new_rate = float(ch["new_refill_rate"])
    new_threshold = ch["new_low_balance_threshold"]

    old = conn.execute(
        "SELECT capacity, refill_rate FROM policies "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if old is None:
        # 策略行已不存在（当前版本不支持删除，仅作防御）：放弃本条
        return
    old_cap, old_rate = float(old["capacity"]), float(old["refill_rate"])

    bucket = conn.execute(
        "SELECT balance, updated_at FROM buckets "
        "WHERE scope=? AND tenant_id=? AND route_id=?",
        (scope, tenant, route),
    ).fetchone()
    if bucket is None:
        # 桶尚未惰性创建：没有需要换算的余额，新策略生效后按新容量满额创建
        pass
    else:
        # 1) 旧策略补充到生效时刻
        at_eff = _refill(
            bucket["balance"], bucket["updated_at"], old_cap, old_rate, effective_at
        )
        # 2) 按剩余比例缩放到新容量（旧容量为 0 -> 余额 0）
        if old_cap <= 0.0:
            balance = 0.0
        else:
            balance = min(new_cap, at_eff / old_cap * new_cap)
        # 3) 时间锚点移到 effective_at，后续补充统一走新速率
        conn.execute(
            "UPDATE buckets SET balance=?, updated_at=? "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (balance, eff_iso, scope, tenant, route),
        )

    conn.execute(
        """
        UPDATE policies
        SET capacity=?, refill_rate=?, low_balance_threshold=?, updated_at=?
        WHERE scope=? AND tenant_id=? AND route_id=?
        """,
        (
            new_cap, new_rate, new_threshold, eff_iso,
            scope, tenant, route,
        ),
    )
    cur = conn.execute(
        "UPDATE policy_changes SET status='applied', applied_at=? "
        "WHERE change_id=? AND status='pending'",
        (iso(now), ch["change_id"]),
    )
    if cur.rowcount > 0:
        # 定时切换实际生效（由本次裁决/撤销事务抢到 pending -> applied）时，
        # 在同一事务追加不可变版本（source=scheduled）
        _append_policy_version(
            conn,
            scope=scope, tenant=tenant, route=route,
            capacity=new_cap, refill_rate=new_rate,
            low_balance_threshold=new_threshold,
            source="scheduled", created_at=iso(now),
            change_id=ch["change_id"],
        )


def _maybe_emit_low_balance(
    conn: sqlite3.Connection,
    state: dict[str, Any],
    *,
    before: float,
    after: float,
    request_id: str,
    now: datetime,
) -> None:
    """获准扣减令桶「从阈值之上降至不高于阈值」时，在扣减事务内写入唯一事件。

    阈值关闭（None）或本次未越线时不写。余额已在阈值之下的继续扣减不算
    越线；只有当补充/撤销/策略更新把余额重新抬到阈值之上后，再次越线才
    会产生新事件。
    """
    threshold = state["low_balance_threshold"]
    if threshold is None:
        return
    # after 是浮点减法结果，给「不高于阈值」一点容差，避免恰好等于阈值时漏报
    if before > threshold and after <= threshold + 1e-12:
        conn.execute(
            """
            INSERT INTO low_balance_events
                (scope, tenant_id, route_id, balance_before, balance_after,
                 threshold, request_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                state["scope"], state["tenant"], state["route"],
                float(before), float(after), float(threshold),
                request_id, iso(now),
            ),
        )


def decide(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    tenant_id: Optional[str],
    route_id: Optional[str],
    cost: float,
) -> tuple[dict[str, Any], bool]:
    """对一次请求做多级裁决，返回 (结果, 是否为重放)。

    命中的每个桶在同一事务、同一时刻 now 下：
      1. 按距上次更新经过的时间补充令牌（不超过容量）；
      2. 全部桶余额都足够才统一扣减；任一不足则一个都不扣。
    """
    if cost <= 0:
        raise HTTPException(422, "cost 必须为正数")
    if route_id and not tenant_id:
        raise HTTPException(422, "提供 route_id 时必须同时提供 tenant_id")

    now = now_utc()

    # 0) 在同一裁决事务内先应用所有命中且到期的定时策略变更（无后台任务，
    #    惰性驱动），随后的读策略/读桶看到的必是切换后的完整新状态
    _apply_due_changes(conn, _hit_keys(tenant_id, route_id), now)

    # 1) 幂等：request_id 已存在时，载荷一致则重放，冲突则 409
    existing = conn.execute(
        "SELECT payload_hash, result_json FROM decisions WHERE request_id=?",
        (request_id,),
    ).fetchone()
    payload = {"tenant_id": tenant_id or None, "route_id": route_id or None, "cost": cost}
    digest = payload_hash(payload)
    payload_json = json.dumps(payload, ensure_ascii=False)
    if existing is not None:
        if existing["payload_hash"] != digest:
            raise HTTPException(
                409,
                f"request_id={request_id!r} 已存在但裁决载荷不一致，拒绝重放",
            )
        result = json.loads(existing["result_json"])
        result["replayed"] = True
        return result, True

    # 2) 解析本次请求命中的策略层（全局 -> 租户 -> 路由），缺省层不做限制
    levels: list[tuple[str, str, str, sqlite3.Row]] = []
    g = conn.execute(
        "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
        "WHERE scope='global' AND tenant_id='' AND route_id=''"
    ).fetchone()
    if g is not None:
        levels.append(("global", EMPTY, EMPTY, g))
    if tenant_id:
        t = conn.execute(
            "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
            "WHERE scope='tenant' AND tenant_id=? AND route_id=''",
            (tenant_id,),
        ).fetchone()
        if t is not None:
            levels.append(("tenant", tenant_id, EMPTY, t))
        if route_id:
            r = conn.execute(
                "SELECT capacity, refill_rate, low_balance_threshold FROM policies "
                "WHERE scope='route' AND tenant_id=? AND route_id=?",
                (tenant_id, route_id),
            ).fetchone()
            if r is not None:
                levels.append(("route", tenant_id, route_id, r))

    if not levels:
        raise HTTPException(404, "没有任何命中的配额策略（请先配置至少一层策略）")

    # 3) 取桶（惰性创建，新桶满额），并在同一 now 下补充
    states: list[dict[str, Any]] = []
    for scope, tenant, route, pol in levels:
        row = conn.execute(
            "SELECT balance, updated_at FROM buckets "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if row is None:
            balance = float(pol["capacity"])
            conn.execute(
                "INSERT INTO buckets (scope, tenant_id, route_id, balance, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (scope, tenant, route, balance, iso(now)),
            )
        else:
            balance = _refill(
                row["balance"], row["updated_at"],
                pol["capacity"], pol["refill_rate"], now,
            )
        states.append(
            {
                "scope": scope, "tenant": tenant, "route": route,
                "capacity": float(pol["capacity"]),
                "refill_rate": float(pol["refill_rate"]),
                "low_balance_threshold": pol["low_balance_threshold"],
                "balance": balance,
            }
        )

    allowed = all(s["balance"] + 1e-12 >= cost for s in states)

    # 4) 不足：整组一个都不扣（补充本身会落库，这是时间应得的令牌）；
    #    充足：所有命中桶在同一事务中一起扣减
    if allowed:
        for s in states:
            before = s["balance"]
            after = before - float(cost)
            s["balance"] = after
            conn.execute(
                "UPDATE buckets SET balance=?, updated_at=? "
                "WHERE scope=? AND tenant_id=? AND route_id=?",
                (after, iso(now), s["scope"], s["tenant"], s["route"]),
            )
            _maybe_emit_low_balance(
                conn, s, before=before, after=after,
                request_id=request_id, now=now,
            )
        retry_at, retry_after, retry_possible = None, None, True
    else:
        # 整组最早可重试时刻 = 各不足桶就绪耗时的最大值
        waits: list[float] = []
        retry_possible = True
        for s in states:
            if s["balance"] + 1e-12 >= cost:
                continue
            deficit = float(cost) - s["balance"]
            rate = s["refill_rate"]
            # rate=0 永不补充，或 cost 超过桶容量 -> 无论等多久都不可能通过
            if rate <= 0 or float(cost) > s["capacity"] + 1e-12:
                retry_possible = False
                break
            waits.append(deficit / rate)
        if retry_possible:
            wait = max(waits)
            retry_dt = now + timedelta(seconds=wait)
            retry_at, retry_after = iso(retry_dt), max(0.0, wait)
        else:
            retry_at, retry_after = None, None
        # 拒绝不扣减，但补充后的余额（时间应得的令牌）一律落库
        for s in states:
            conn.execute(
                "UPDATE buckets SET balance=?, updated_at=? "
                "WHERE scope=? AND tenant_id=? AND route_id=?",
                (s["balance"], iso(now), s["scope"], s["tenant"], s["route"]),
            )

    result = {
        "request_id": request_id,
        "allowed": allowed,
        "cost": float(cost),
        "tenant_id": tenant_id or None,
        "route_id": route_id or None,
        "decided_at": iso(now),
        "retry_at": retry_at,
        "retry_after_seconds": retry_after,
        "retry_possible": retry_possible,
        "replayed": False,
        "revoked": False,
        "buckets": [
            _bucket_out(
                s["scope"], s["tenant"], s["route"],
                s["capacity"], s["refill_rate"], s["balance"],
            )
            for s in states
        ],
    }

    # 5) 存档（通过与拒绝都存），保证 request_id 可重放
    conn.execute(
        """
        INSERT INTO decisions
            (request_id, payload_hash, payload_json, allowed, cost, decided_at,
             retry_at, retry_after_secs, result_json, revoked, revoked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        """,
        (
            request_id, digest, payload_json, int(allowed), float(cost), iso(now),
            retry_at, retry_after, json.dumps(result, ensure_ascii=False),
        ),
    )
    return result, False


# ---------------------------------------------------------------- 撤销


def revoke(conn: sqlite3.Connection, request_id: str) -> tuple[dict[str, Any], bool]:
    """撤销一次已通过的裁决并补回令牌，返回 (结果, 是否为重复撤销)。

    重复撤销幂等；被拒绝/不存在的 request_id 分别返回 409 / 404。
    每个桶先按经过时间补充，再加回 cost，最后截断到容量。
    """
    row = conn.execute(
        "SELECT allowed, cost, payload_json, result_json, revoked FROM decisions "
        "WHERE request_id=?",
        (request_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, f"request_id={request_id!r} 不存在")
    if not row["allowed"]:
        raise HTTPException(409, f"request_id={request_id!r} 是被拒绝的裁决，无需撤销")
    if row["revoked"]:
        result = json.loads(row["result_json"])
        result["replayed"] = True
        return result, True

    payload = json.loads(row["payload_json"])
    cost = float(row["cost"])
    now = now_utc()

    # 用原裁决载荷定位当时命中的桶；桶行在裁决通过时必然已创建
    keys = _hit_keys(payload.get("tenant_id"), payload.get("route_id"))

    # 与裁决相同：先在本事务内应用命中且到期的定时变更，再补回令牌，
    # 避免撤销用旧策略把桶时间锚点推过 effective_at 而重复计息
    _apply_due_changes(conn, keys, now)

    refunded_buckets: list[dict[str, Any]] = []
    for scope, tenant, route in keys:
        pol = conn.execute(
            "SELECT capacity, refill_rate FROM policies "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if pol is None:
            continue  # 策略已被移除（当前版本不支持删除，仅作防御）
        b = conn.execute(
            "SELECT balance, updated_at FROM buckets "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (scope, tenant, route),
        ).fetchone()
        if b is None:
            continue
        # 先补充时间应得的令牌，再补回本次扣减，并以容量为上限
        balance = _refill(
            b["balance"], b["updated_at"],
            pol["capacity"], pol["refill_rate"], now,
        )
        balance = min(float(pol["capacity"]), balance + cost)
        conn.execute(
            "UPDATE buckets SET balance=?, updated_at=? "
            "WHERE scope=? AND tenant_id=? AND route_id=?",
            (balance, iso(now), scope, tenant, route),
        )
        refunded_buckets.append(
            _bucket_out(
                scope, tenant, route,
                pol["capacity"], pol["refill_rate"], balance,
            )
        )

    revoked_at = iso(now)
    result = json.loads(row["result_json"])
    result["revoked"] = True
    result["revoked_at"] = revoked_at
    result["replayed"] = False
    result["buckets"] = refunded_buckets

    conn.execute(
        "UPDATE decisions SET revoked=1, revoked_at=?, result_json=? WHERE request_id=?",
        (revoked_at, json.dumps(result, ensure_ascii=False), request_id),
    )
    return result, False


# ---------------------------------------------------------------- 事件


def list_events(
    conn: sqlite3.Connection, *, after_id: int = 0, limit: int = 100
) -> list[dict[str, Any]]:
    """按 id 递增返回低余额事件；after_id 为游标（不含），limit 为页大小。"""
    rows = conn.execute(
        "SELECT id, scope, tenant_id, route_id, balance_before, balance_after, "
        "threshold, request_id, created_at "
        "FROM low_balance_events WHERE id > ? ORDER BY id LIMIT ?",
        (after_id, limit),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "scope": r["scope"],
            "tenant_id": r["tenant_id"] or None,
            "route_id": r["route_id"] or None,
            "balance_before": r["balance_before"],
            "balance_after": r["balance_after"],
            "threshold": r["threshold"],
            "request_id": r["request_id"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]

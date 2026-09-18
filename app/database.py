"""SQLite 连接管理与建表。

写事务统一使用 BEGIN IMMEDIATE 抢占写锁；配合 busy_timeout，
无论单进程多线程还是多 worker 部署，同一时刻只有一个写者，
保证裁决/撤销的读-改-写序列不会交错超发。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

# 所有策略层（全局 / 租户 / 路由）的公共行
# scope: 'global' | 'tenant' | 'route'
# low_balance_threshold: NULL 表示关闭低余额事件；否则取值须在 [0, capacity]
SCHEMA = """
CREATE TABLE IF NOT EXISTS policies (
    scope         TEXT NOT NULL,
    tenant_id     TEXT NOT NULL DEFAULT '',
    route_id      TEXT NOT NULL DEFAULT '',
    capacity      REAL NOT NULL CHECK (capacity >= 0),
    refill_rate   REAL NOT NULL CHECK (refill_rate >= 0),
    low_balance_threshold REAL
        CHECK (low_balance_threshold IS NULL OR low_balance_threshold >= 0),
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (scope, tenant_id, route_id)
) STRICT;

-- 令牌桶惰性补充：仅在被裁决/撤销命中时才按经过时间补充并落库
CREATE TABLE IF NOT EXISTS buckets (
    scope         TEXT NOT NULL,
    tenant_id     TEXT NOT NULL DEFAULT '',
    route_id      TEXT NOT NULL DEFAULT '',
    balance       REAL NOT NULL CHECK (balance >= 0),
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (scope, tenant_id, route_id)
) STRICT;

-- 裁决结果按 request_id 幂等存档
CREATE TABLE IF NOT EXISTS decisions (
    request_id        TEXT PRIMARY KEY,
    payload_hash      TEXT NOT NULL,
    payload_json      TEXT NOT NULL,
    allowed           INTEGER NOT NULL,
    cost              REAL NOT NULL,
    decided_at        TEXT NOT NULL,
    retry_at          TEXT,
    retry_after_secs  REAL,
    result_json       TEXT NOT NULL,
    revoked           INTEGER NOT NULL DEFAULT 0,
    revoked_at        TEXT
) STRICT;

-- 低余额事件：获准裁决令某桶从阈值之上降至不高于阈值时，
-- 与扣减在同一事务内写入；id 自增，作为 /events 分页的稳定游标
CREATE TABLE IF NOT EXISTS low_balance_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scope          TEXT NOT NULL,
    tenant_id      TEXT NOT NULL DEFAULT '',
    route_id       TEXT NOT NULL DEFAULT '',
    balance_before REAL NOT NULL,
    balance_after  REAL NOT NULL,
    threshold      REAL NOT NULL,
    request_id     TEXT NOT NULL,
    created_at     TEXT NOT NULL
) STRICT;

-- 策略定时切换：一条变更在 effective_at 到期后，由下一次命中该策略的
-- 裁决在同一写事务内惰性应用（无后台任务）。status 生命周期：
-- pending -> applied（已应用）/ cancelled（生效前取消）
CREATE TABLE IF NOT EXISTS policy_changes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    change_id    TEXT NOT NULL UNIQUE,
    scope        TEXT NOT NULL,
    tenant_id    TEXT NOT NULL DEFAULT '',
    route_id     TEXT NOT NULL DEFAULT '',
    new_capacity REAL NOT NULL CHECK (new_capacity >= 0),
    new_refill_rate REAL NOT NULL CHECK (new_refill_rate >= 0),
    new_low_balance_threshold REAL
        CHECK (new_low_balance_threshold IS NULL OR new_low_balance_threshold >= 0),
    effective_at TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'applied', 'cancelled')),
    created_at   TEXT NOT NULL,
    applied_at   TEXT,
    cancelled_at TEXT
) STRICT;

-- 同一策略至多一条待生效变更（应用/取消后可再提交新的）
CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_changes_one_pending
ON policy_changes (scope, tenant_id, route_id)
WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
"""


def db_path() -> str:
    path = os.environ.get("QUOTA_DB_PATH", "/data/quota.db")
    return path


def connect(path: str | None = None) -> sqlite3.Connection:
    """打开一个连接。isolation_level=None 后由我们手动控制事务边界。"""
    target = path or db_path()
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        target,
        isolation_level=None,
        timeout=30.0,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    # WAL + busy_timeout 让并发写者自动排队等待而不是立刻报锁
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path: str | None = None) -> None:
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """老库就地升级：CREATE TABLE IF NOT EXISTS 不会给已有表补新列。"""
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(policies)")}
    if "low_balance_threshold" not in cols:
        conn.execute(
            "ALTER TABLE policies ADD COLUMN low_balance_threshold REAL "
            "CHECK (low_balance_threshold IS NULL OR low_balance_threshold >= 0)"
        )


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """BEGIN IMMEDIATE 立即获取 RESERVED 锁，把整个读-改-写串行化。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

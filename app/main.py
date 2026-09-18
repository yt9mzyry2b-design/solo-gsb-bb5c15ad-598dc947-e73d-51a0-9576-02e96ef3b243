"""FastAPI 入口：多级配额裁决服务。

启动后默认监听 8000 端口，交互式文档位于 /docs。
每个写请求使用独立连接 + BEGIN IMMEDIATE 事务，天然串行化，
并发裁决不会超发（见 app/database.py 与 app/quota.py）。
"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Iterator

from fastapi import Depends, FastAPI, Query

from . import quota
from .database import connect, init_db, write_tx
from .schemas import (
    BucketOut,
    DecideIn,
    DecisionOut,
    LowBalanceEventOut,
    PolicyChangeIn,
    PolicyChangeOut,
    PolicyIn,
    PolicyOut,
    PolicyRollbackIn,
    PolicyVersionOut,
    RevokeOut,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="多级配额裁决 API",
    description=(
        "全局 / 租户 / 路由三级令牌桶配额裁决。"
        "一次裁决在同一事务中补充并扣减所有命中桶，全部满足才扣减；"
        "request_id 幂等重放；支持撤销补回；"
        "策略可配置 low_balance_threshold，余额越线时产生 low_balance 事件；"
        "支持提交未来生效的策略定时切换，由裁决在同一事务内惰性应用；"
        "策略变更追加不可变版本记录，支持历史查询与同事务回滚。"
    ),
    version="1.3.0",
    lifespan=lifespan,
)


def get_conn() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------- 策略


@app.get("/policies", response_model=list[PolicyOut], tags=["策略"])
def get_policies(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict]:
    return quota.list_policies(conn)


@app.put("/policies", response_model=PolicyOut, tags=["策略"])
def put_policy(
    body: PolicyIn, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    with write_tx(conn):
        return quota.upsert_policy(
            conn,
            scope=body.scope,
            tenant_id=body.tenant_id,
            route_id=body.route_id,
            capacity=body.capacity,
            refill_rate=body.refill_rate,
            low_balance_threshold=body.low_balance_threshold,
        )


@app.get(
    "/policies/history",
    response_model=list[PolicyVersionOut],
    tags=["策略版本审计"],
)
def get_policy_history(
    scope: str = Query(..., pattern="^(global|tenant|route)$"),
    tenant_id: str | None = Query(default=None),
    route_id: str | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[dict]:
    """查询单个策略的不可变版本历史，按版本号新到旧返回。只读。"""
    return quota.list_policy_versions(
        conn, scope=scope, tenant_id=tenant_id, route_id=route_id
    )


@app.post(
    "/policies/rollback",
    response_model=PolicyVersionOut,
    tags=["策略版本审计"],
    responses={
        404: {"description": "目标策略或版本不存在"},
        409: {"description": "该策略存在待生效（pending）的定时变更"},
    },
)
def post_policy_rollback(
    body: PolicyRollbackIn, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    """在同一写事务内把策略恢复到指定历史版本。

    已有桶先按当前策略补充到事务时刻，再按目标版本容量截断；
    恢复成功会追加一条 source=rollback 的新版本（版本号继续递增）。
    """
    with write_tx(conn):
        return quota.rollback_policy(
            conn,
            scope=body.scope,
            tenant_id=body.tenant_id,
            route_id=body.route_id,
            version=body.version,
        )


# ------------------------------------------------------------ 策略定时切换


@app.post(
    "/policy-changes",
    response_model=PolicyChangeOut,
    tags=["策略定时切换"],
    responses={
        404: {"description": "目标策略不存在"},
        409: {"description": "change_id 已存在，或同一策略已有待生效变更"},
    },
)
def post_policy_change(
    body: PolicyChangeIn, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    """为已存在的策略登记一条未来生效的定时切换。

    同一策略至多一条 pending 变更；存在 pending 期间 PUT /policies 更新该
    策略会返回 409。到期后无需后台任务：下一次命中该策略的裁决会在同一
    BEGIN IMMEDIATE 事务内先应用变更，再执行裁决。
    """
    with write_tx(conn):
        return quota.create_policy_change(
            conn,
            change_id=body.change_id,
            scope=body.scope,
            tenant_id=body.tenant_id,
            route_id=body.route_id,
            capacity=body.capacity,
            refill_rate=body.refill_rate,
            low_balance_threshold=body.low_balance_threshold,
            effective_at=body.effective_at,
        )


@app.get(
    "/policy-changes",
    response_model=list[PolicyChangeOut],
    tags=["策略定时切换"],
)
def get_policy_changes(
    status: str | None = Query(default=None, pattern="^(pending|applied|cancelled)$"),
    scope: str | None = Query(default=None),
    tenant_id: str | None = Query(default=None),
    route_id: str | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[dict]:
    """列出策略定时变更，可按 status / 策略键过滤。只读。"""
    return quota.list_policy_changes(
        conn,
        status=status,
        scope=scope,
        tenant_id=tenant_id,
        route_id=route_id,
    )


@app.get(
    "/policy-changes/{change_id}",
    response_model=PolicyChangeOut,
    tags=["策略定时切换"],
    responses={404: {"description": "变更不存在"}},
)
def get_policy_change(
    change_id: str, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    return quota.get_policy_change(conn, change_id)


@app.delete(
    "/policy-changes/{change_id}",
    response_model=PolicyChangeOut,
    tags=["策略定时切换"],
    responses={
        404: {"description": "变更不存在"},
        409: {"description": "变更已生效或已到生效时刻，无法取消"},
    },
)
def delete_policy_change(
    change_id: str, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    """在生效前取消一条 pending 变更；重复取消幂等。"""
    with write_tx(conn):
        return quota.cancel_policy_change(conn, change_id)


# ---------------------------------------------------------------- 桶观察


@app.get("/buckets", response_model=list[BucketOut], tags=["桶状态"])
def get_buckets(conn: sqlite3.Connection = Depends(get_conn)) -> list[dict]:
    """查看所有已创建桶的当前（补充后）余额，便于排障。只读，不落库。"""
    return quota.list_buckets(conn)


# ---------------------------------------------------------------- 事件


@app.get("/events", response_model=list[LowBalanceEventOut], tags=["事件"])
def get_events(
    after_id: int = Query(default=0, ge=0, description="游标：只返回 id 大于它的事件"),
    limit: int = Query(default=100, ge=1, le=1000, description="每页最多返回条数"),
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[dict]:
    """按 id 稳定递增分页返回 low_balance 事件。

    用法：先取 `GET /events?limit=N`，然后以最后一条的 id 作为
    `after_id` 继续翻页，直到返回空列表。
    """
    return quota.list_events(conn, after_id=after_id, limit=limit)


# ---------------------------------------------------------------- 裁决 / 撤销


@app.post(
    "/decide",
    response_model=DecisionOut,
    tags=["裁决"],
    responses={409: {"description": "request_id 已存在但载荷冲突"}},
)
def decide(body: DecideIn, conn: sqlite3.Connection = Depends(get_conn)) -> dict:
    with write_tx(conn):
        result, _replayed = quota.decide(
            conn,
            request_id=body.request_id,
            tenant_id=body.tenant_id,
            route_id=body.route_id,
            cost=body.cost,
        )
        return result


@app.post(
    "/decide/{request_id}/revoke",
    response_model=RevokeOut,
    tags=["裁决"],
    responses={
        404: {"description": "裁决不存在"},
        409: {"description": "裁决本身被拒绝，无需撤销"},
    },
)
def revoke_decision(
    request_id: str, conn: sqlite3.Connection = Depends(get_conn)
) -> dict:
    with write_tx(conn):
        result, _duplicate = quota.revoke(conn, request_id)
        return result

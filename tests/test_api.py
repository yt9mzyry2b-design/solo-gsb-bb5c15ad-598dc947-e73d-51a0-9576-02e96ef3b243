"""核心行为测试。用 QUOTA_DB_PATH 指向每个测试独立的临时 SQLite 文件。"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("QUOTA_DB_PATH", str(db_file))
    # 延迟导入 app，确保它读到环境变量
    import importlib

    from app import database, main

    importlib.reload(database)
    importlib.reload(main)
    database.init_db(str(db_file))
    with TestClient(main.app) as c:
        yield c


_UNSET = object()


def put_policy(client, scope, capacity, refill_rate, tenant=None, route=None,
               threshold=_UNSET):
    body = {
        "scope": scope,
        "tenant_id": tenant,
        "route_id": route,
        "capacity": capacity,
        "refill_rate": refill_rate,
    }
    if threshold is not _UNSET:
        body["low_balance_threshold"] = threshold
    return client.put("/policies", json=body)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def post_change(client, change_id, scope, capacity, refill_rate, *,
                effective_in=1.0, tenant=None, route=None, threshold=_UNSET):
    body = {
        "change_id": change_id,
        "scope": scope,
        "tenant_id": tenant,
        "route_id": route,
        "capacity": capacity,
        "refill_rate": refill_rate,
        "effective_at": _iso(
            datetime.now(timezone.utc) + timedelta(seconds=effective_in)
        ),
    }
    if threshold is not _UNSET:
        body["low_balance_threshold"] = threshold
    return client.post("/policy-changes", json=body)


def rollback(client, scope, version, tenant=None, route=None):
    return client.post(
        "/policies/rollback",
        json={
            "scope": scope,
            "tenant_id": tenant,
            "route_id": route,
            "version": version,
        },
    )


def history(client, scope, **params):
    return client.get("/policies/history", params={"scope": scope, **params})


# ------------------------------------------------------------ 基本扣减与拒绝


def test_allow_and_atomic_deny_multilevel(client):
    assert put_policy(client, "global", 5, 0).status_code == 200
    assert put_policy(client, "tenant", 3, 0, tenant="t1").status_code == 200
    assert (
        put_policy(client, "route", 4, 0, tenant="t1", route="rA").status_code == 200
    )

    r = client.post(
        "/decide",
        json={"request_id": "d1", "tenant_id": "t1", "route_id": "rA", "cost": 3},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["allowed"] is True
    assert body["replayed"] is False
    by_scope = {b["scope"]: b["balance"] for b in body["buckets"]}
    assert by_scope == {"global": 2.0, "tenant": 0.0, "route": 1.0}

    # 租户桶已空 -> 拒绝；全局/路由桶一个都不应被扣
    r = client.post(
        "/decide",
        json={"request_id": "d2", "tenant_id": "t1", "route_id": "rA", "cost": 1},
    )
    body = r.json()
    assert body["allowed"] is False
    assert body["retry_possible"] is False  # refill_rate=0 永不可能
    assert body["retry_at"] is None
    by_scope = {b["scope"]: b["balance"] for b in body["buckets"]}
    assert by_scope == {"global": 2.0, "tenant": 0.0, "route": 1.0}


def test_deny_does_not_deduct_any_bucket(client):
    put_policy(client, "global", 2, 0)
    put_policy(client, "tenant", 10, 0, tenant="t")
    r = client.post(
        "/decide", json={"request_id": "x", "tenant_id": "t", "cost": 3}
    ).json()
    assert r["allowed"] is False
    # 全局桶容量只有 2，但绝不能被扣成负数
    global_b = [b for b in r["buckets"] if b["scope"] == "global"][0]
    tenant_b = [b for b in r["buckets"] if b["scope"] == "tenant"][0]
    assert global_b["balance"] == 2.0
    assert tenant_b["balance"] == 10.0


# ------------------------------------------------------------ 时间补充与重试时间


def test_refill_over_time(client):
    put_policy(client, "global", 10, 10)  # 10 令牌/秒
    # 连续打空
    for i in range(10):
        r = client.post("/decide", json={"request_id": f"e{i}", "cost": 1}).json()
        assert r["allowed"] is True
    r = client.post("/decide", json={"request_id": "e-empty", "cost": 1}).json()
    assert r["allowed"] is False
    assert r["retry_possible"] is True
    assert r["retry_after_seconds"] == pytest.approx(0.1, abs=0.25)
    import time

    time.sleep(0.3)
    r = client.post("/decide", json={"request_id": "e-refill", "cost": 1}).json()
    assert r["allowed"] is True


def test_retry_time_is_max_across_buckets(client):
    # 先打一笔 cost=6：全局 6->0（缺 4 @4/s=1s），租户 8->2（缺 2 @2/s=1s）
    # 再请求 cost=4 被拒，整组最早可重试时间取最大值 1s
    put_policy(client, "global", 6, 4)
    put_policy(client, "tenant", 8, 2, tenant="t")
    client.post("/decide", json={"request_id": "a", "tenant_id": "t", "cost": 6})
    r = client.post(
        "/decide", json={"request_id": "b", "tenant_id": "t", "cost": 4}
    ).json()
    assert r["allowed"] is False
    assert r["retry_after_seconds"] == pytest.approx(1.0, abs=0.5)
    assert r["retry_at"] is not None


def test_cost_above_capacity_means_impossible(client):
    put_policy(client, "global", 3, 10)
    r = client.post("/decide", json={"request_id": "big", "cost": 5}).json()
    assert r["allowed"] is False
    assert r["retry_possible"] is False
    assert r["retry_at"] is None


# ------------------------------------------------------------ 幂等


def test_idempotent_replay_same_payload(client):
    put_policy(client, "global", 1, 0)
    payload = {"request_id": "id1", "cost": 1}
    first = client.post("/decide", json=payload).json()
    assert first["allowed"] is True
    second = client.post("/decide", json=payload).json()
    assert second["allowed"] is True
    assert second["replayed"] is True
    # 余额没有因为“再来一次”被扣两次
    assert second["buckets"][0]["balance"] == 0.0
    assert second["decided_at"] == first["decided_at"]


def test_idempotent_replay_denied(client):
    put_policy(client, "global", 1, 0)
    client.post("/decide", json={"request_id": "n1", "cost": 1})
    denied = client.post(
        "/decide", json={"request_id": "n2", "cost": 1}
    ).json()
    assert denied["allowed"] is False
    replay = client.post(
        "/decide", json={"request_id": "n2", "cost": 1}
    ).json()
    assert replay["allowed"] is False
    assert replay["replayed"] is True


def test_payload_conflict_returns_409(client):
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "k1", "cost": 1})
    r = client.post("/decide", json={"request_id": "k1", "cost": 2})
    assert r.status_code == 409


# ------------------------------------------------------------ 撤销


def test_revoke_refunds_and_is_idempotent(client):
    put_policy(client, "global", 10, 0)
    put_policy(client, "tenant", 10, 0, tenant="t")
    client.post(
        "/decide", json={"request_id": "rv", "tenant_id": "t", "cost": 4}
    )
    r = client.post("/decide/rv/revoke")
    assert r.status_code == 200
    body = r.json()
    assert body["revoked"] is True
    assert all(b["balance"] == 10.0 for b in body["buckets"])

    # 重复撤销幂等
    r2 = client.post("/decide/rv/revoke")
    assert r2.status_code == 200
    assert r2.json()["revoked"] is True

    # 令牌已补回，新的裁决可以通过；不会因为撤销两次而多出令牌
    ok = client.post(
        "/decide", json={"request_id": "rv-next", "tenant_id": "t", "cost": 10}
    ).json()
    assert ok["allowed"] is True


def test_revoke_refund_capped_at_capacity(client):
    put_policy(client, "global", 10, 100)  # 高补充率：撤销前桶可能早已回满
    client.post("/decide", json={"request_id": "c1", "cost": 3})
    body = client.post("/decide/c1/revoke").json()
    assert all(b["balance"] <= b["capacity"] for b in body["buckets"])
    assert body["buckets"][0]["balance"] == 10.0


def test_revoke_denied_decision_conflicts(client):
    put_policy(client, "global", 1, 0)
    client.post("/decide", json={"request_id": "ok", "cost": 1})
    client.post("/decide", json={"request_id": "no", "cost": 1})
    r = client.post("/decide/no/revoke")
    assert r.status_code == 409


def test_revoke_unknown_404(client):
    assert client.post("/decide/nope/revoke").status_code == 404


# ------------------------------------------------------------ 策略更新


def test_policy_update_clamps_balance(client):
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "p1", "cost": 10})
    # 容量缩小到 5：余额 90 被截断到 5
    put_policy(client, "global", 5, 0)
    r = client.post("/decide", json={"request_id": "p2", "cost": 6}).json()
    assert r["allowed"] is False  # 5 < 6，且 rate=0
    assert r["buckets"][0]["balance"] == 5.0


# ------------------------------------------------------------ 并发不超发


def test_concurrent_decisions_never_overspend(client):
    capacity = 50
    put_policy(client, "global", capacity, 0)

    n_threads, per_thread = 20, 5
    results: list[bool] = []
    lock = threading.Lock()

    def worker(idx):
        for i in range(per_thread):
            body = client.post(
                "/decide",
                json={"request_id": f"c-{idx}-{i}", "cost": 1},
            ).json()
            with lock:
                results.append(body["allowed"])

    threads = [threading.Thread(target=worker, args=(j,)) for j in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好 capacity 个通过，其余拒绝；余额精确为 0，即无超发也无少发
    assert sum(results) == capacity
    buckets = client.get("/buckets").json()
    assert buckets[0]["balance"] == 0


def test_concurrent_multilevel_never_overspend(client):
    put_policy(client, "global", 30, 0)
    put_policy(client, "tenant", 12, 0, tenant="t")
    results: list[bool] = []
    lock = threading.Lock()

    def worker(idx):
        for i in range(5):
            body = client.post(
                "/decide",
                json={"request_id": f"m-{idx}-{i}", "tenant_id": "t", "cost": 1},
            ).json()
            with lock:
                results.append(body["allowed"])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(results) == 12  # 租户桶上限 12，不可能更多
    buckets = {b["scope"]: b["balance"] for b in client.get("/buckets").json()}
    assert buckets["tenant"] == 0
    assert buckets["global"] == 30 - 12


def test_validation_errors(client):
    # route 必须带 tenant
    r = client.post("/decide", json={"request_id": "v", "route_id": "r", "cost": 1})
    assert r.status_code == 422
    # 无策略可命中
    r = client.post("/decide", json={"request_id": "v2", "cost": 1})
    assert r.status_code == 404
    # cost 必须为正
    r = client.post("/decide", json={"request_id": "v3", "cost": 0})
    assert r.status_code == 422


# ------------------------------------------------------------ 低余额事件


def test_threshold_validation(client):
    # 负数、超过 capacity 都非法
    assert put_policy(client, "global", 10, 0, threshold=-0.1).status_code == 422
    assert put_policy(client, "global", 10, 0, threshold=10.5).status_code == 422
    # null（关闭）、0、等于 capacity 都合法
    assert put_policy(client, "global", 10, 0, threshold=None).status_code == 200
    assert put_policy(client, "global", 10, 0, threshold=0).status_code == 200
    r = put_policy(client, "global", 10, 0, threshold=10)
    assert r.status_code == 200
    assert r.json()["low_balance_threshold"] == 10.0
    # PUT 全量替换：不传字段即关闭
    r = put_policy(client, "global", 10, 0)
    assert r.json()["low_balance_threshold"] is None
    listed = client.get("/policies").json()
    assert listed[0]["low_balance_threshold"] is None


def test_low_balance_event_on_crossing(client):
    put_policy(client, "global", 10, 0, threshold=4)
    # 10 -> 7：仍在阈值之上，无事件
    client.post("/decide", json={"request_id": "lb1", "cost": 3})
    assert client.get("/events").json() == []
    # 7 -> 4：恰好降到阈值（不高于阈值），产生唯一事件
    client.post("/decide", json={"request_id": "lb2", "cost": 3})
    events = client.get("/events").json()
    assert len(events) == 1
    ev = events[0]
    assert ev["id"] == 1
    assert ev["scope"] == "global"
    assert ev["tenant_id"] is None
    assert ev["route_id"] is None
    assert ev["balance_before"] == 7.0
    assert ev["balance_after"] == 4.0
    assert ev["threshold"] == 4.0
    assert ev["request_id"] == "lb2"
    assert ev["created_at"]


def test_no_event_when_already_below_threshold(client):
    put_policy(client, "global", 10, 0, threshold=4)
    client.post("/decide", json={"request_id": "a", "cost": 6})  # 10 -> 4，越线
    client.post("/decide", json={"request_id": "b", "cost": 2})  # 4 -> 2，不越线
    client.post("/decide", json={"request_id": "c", "cost": 2})  # 2 -> 0，不越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["a"]


def test_no_event_on_deny_or_replay(client):
    put_policy(client, "global", 5, 0, threshold=2)
    client.post("/decide", json={"request_id": "ok", "cost": 4})  # 5 -> 1，事件
    denied = client.post("/decide", json={"request_id": "no", "cost": 4}).json()
    assert denied["allowed"] is False
    replay = client.post("/decide", json={"request_id": "ok", "cost": 4}).json()
    assert replay["replayed"] is True
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["ok"]


def test_event_rearms_after_refill(client):
    put_policy(client, "global", 10, 100, threshold=5)  # 100 令牌/秒，迅速回满
    client.post("/decide", json={"request_id": "r1", "cost": 8})  # 10 -> 2，事件
    import time

    time.sleep(0.2)  # 时间补充把余额重新抬到阈值之上
    client.post("/decide", json={"request_id": "r2", "cost": 8})  # 再次越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["r1", "r2"]
    assert events[0]["id"] < events[1]["id"]


def test_event_rearms_after_revoke(client):
    put_policy(client, "global", 10, 0, threshold=5)
    client.post("/decide", json={"request_id": "v1", "cost": 8})  # 10 -> 2，事件
    client.post("/decide/v1/revoke")  # 撤销补回 -> 10
    client.post("/decide", json={"request_id": "v2", "cost": 8})  # 再次越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["v1", "v2"]


def test_event_rearms_after_threshold_lowered(client):
    put_policy(client, "global", 10, 0, threshold=5)
    client.post("/decide", json={"request_id": "t1", "cost": 8})  # 10 -> 2，事件
    # 策略更新把阈值调到 1：余额 2 重新高于阈值
    put_policy(client, "global", 10, 0, threshold=1)
    client.post("/decide", json={"request_id": "t2", "cost": 2})  # 2 -> 0，再越线
    events = client.get("/events").json()
    assert [e["request_id"] for e in events] == ["t1", "t2"]
    assert events[1]["threshold"] == 1.0


def test_multilevel_crossing_emits_one_event_per_bucket(client):
    put_policy(client, "global", 10, 0, threshold=5)
    put_policy(client, "tenant", 8, 0, tenant="acme", threshold=3)
    put_policy(client, "route", 6, 0, tenant="acme", route="/r")  # 阈值关闭
    client.post(
        "/decide",
        json={"request_id": "m1", "tenant_id": "acme", "route_id": "/r", "cost": 6},
    )
    # global 10->4 越线、tenant 8->2 越线、route 未配置阈值 -> 恰好 2 个事件
    events = client.get("/events").json()
    assert [(e["scope"], e["request_id"]) for e in events] == [
        ("global", "m1"),
        ("tenant", "m1"),
    ]
    by_scope = {e["scope"]: e for e in events}
    assert by_scope["global"]["balance_after"] == 4.0
    assert by_scope["tenant"]["balance_after"] == 2.0
    assert by_scope["tenant"]["tenant_id"] == "acme"


def test_events_pagination_stable(client):
    put_policy(client, "global", 3, 0, threshold=0)
    # 打空 -> 撤销补回 -> 再打空，循环 3 次，产生 3 个事件
    for i in range(3):
        client.post("/decide", json={"request_id": f"p{i}", "cost": 3})
        client.post(f"/decide/p{i}/revoke")

    page1 = client.get("/events", params={"limit": 2}).json()
    assert [e["id"] for e in page1] == [1, 2]
    page2 = client.get(
        "/events", params={"after_id": page1[-1]["id"], "limit": 2}
    ).json()
    assert [e["id"] for e in page2] == [3]
    page3 = client.get(
        "/events", params={"after_id": page2[-1]["id"], "limit": 2}
    ).json()
    assert page3 == []
    # 参数校验
    assert client.get("/events", params={"after_id": -1}).status_code == 422
    assert client.get("/events", params={"limit": 0}).status_code == 422


def test_old_database_is_migrated(tmp_path):
    """老库（policies 无 low_balance_threshold 列）init_db 后读写正常。"""
    import sqlite3

    from app import database, quota

    db_file = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        "CREATE TABLE policies ("
        "  scope TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',"
        "  route_id TEXT NOT NULL DEFAULT '', capacity REAL NOT NULL,"
        "  refill_rate REAL NOT NULL, updated_at TEXT NOT NULL,"
        "  PRIMARY KEY (scope, tenant_id, route_id)) STRICT"
    )
    conn.execute(
        "INSERT INTO policies VALUES ('global', '', '', 10, 1, '2026-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    database.init_db(str(db_file))
    conn = database.connect(str(db_file))
    try:
        policies = quota.list_policies(conn)
        # 老数据阈值视为关闭；新列可正常 upsert
        quota.upsert_policy(
            conn, scope="global", tenant_id=None, route_id=None,
            capacity=10, refill_rate=1, low_balance_threshold=5,
        )
        updated = quota.list_policies(conn)
    finally:
        conn.close()
    assert policies[0]["low_balance_threshold"] is None
    assert updated[0]["low_balance_threshold"] == 5.0


# ------------------------------------------------------------ 策略定时切换


def test_policy_change_lifecycle_and_conflicts(client):
    put_policy(client, "global", 100, 0)

    # 成功登记
    r = post_change(client, "ch1", "global", 50, 10, threshold=5,
                    effective_in=3600)
    assert r.status_code == 200
    body = r.json()
    assert body["change_id"] == "ch1"
    assert body["status"] == "pending"
    assert body["capacity"] == 50.0
    assert body["refill_rate"] == 10.0
    assert body["low_balance_threshold"] == 5.0
    assert body["effective_at"].endswith("Z")
    assert body["applied_at"] is None and body["cancelled_at"] is None

    # 同一策略第二条 pending -> 409
    assert post_change(client, "ch2", "global", 10, 0,
                       effective_in=3600).status_code == 409
    # change_id 重复 -> 409（即使指向另一策略）
    put_policy(client, "tenant", 5, 0, tenant="t")
    r = post_change(client, "ch1", "tenant", 5, 0, tenant="t",
                    effective_in=3600)
    assert r.status_code == 409
    # pending 期间 PUT 更新该策略 -> 409
    assert put_policy(client, "global", 200, 0).status_code == 409
    # 未涉及 pending 变更的其他策略仍可更新
    assert put_policy(client, "tenant", 6, 0, tenant="t").status_code == 200

    # 列表 / 查询 / 过滤
    assert client.get("/policy-changes").json()[0]["change_id"] == "ch1"
    pending = client.get("/policy-changes", params={"status": "pending"}).json()
    assert [c["change_id"] for c in pending] == ["ch1"]
    assert client.get(
        "/policy-changes", params={"scope": "global", "status": "applied"}
    ).json() == []
    assert client.get("/policy-changes/ch1").status_code == 200
    assert client.get("/policy-changes/missing").status_code == 404

    # 取消后：PUT 恢复、可再提交；重复取消幂等
    r = client.delete("/policy-changes/ch1")
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert r.json()["cancelled_at"]
    assert client.delete("/policy-changes/ch1").json()["status"] == "cancelled"
    assert put_policy(client, "global", 200, 0).status_code == 200
    r = post_change(client, "ch1b", "global", 50, 10, effective_in=3600)
    assert r.status_code == 200


def test_policy_change_validation(client):
    put_policy(client, "global", 100, 0)
    put_policy(client, "tenant", 10, 0, tenant="t")

    # 目标策略不存在 -> 404
    r = post_change(client, "x", "tenant", 5, 0, tenant="nope",
                    effective_in=10)
    assert r.status_code == 404
    # 非法 scope 组合 / 越界阈值 -> 422
    r = post_change(client, "x", "tenant", 5, 0, effective_in=10)
    assert r.status_code == 422
    r = post_change(client, "x", "global", 10, 0, threshold=11,
                    effective_in=10)
    assert r.status_code == 422
    # effective_at 必须是未来
    r = client.post("/policy-changes", json={
        "change_id": "x", "scope": "global", "capacity": 10, "refill_rate": 0,
        "effective_at": _iso(datetime.now(timezone.utc) - timedelta(seconds=5)),
    })
    assert r.status_code == 422
    # effective_at 必须显式带时区
    r = client.post("/policy-changes", json={
        "change_id": "x", "scope": "global", "capacity": 10, "refill_rate": 0,
        "effective_at": "2026-10-01T00:00:00",
    })
    assert r.status_code == 422
    # 未知 status 过滤值 -> 422
    assert client.get("/policy-changes", params={"status": "wat"}).status_code == 422


def test_decide_before_effective_keeps_old_policy(client):
    put_policy(client, "global", 100, 0)
    post_change(client, "ch1", "global", 50, 0, threshold=10, effective_in=3600)
    # 未到生效时刻：容量/速率/阈值全按旧策略走
    r = client.post("/decide", json={"request_id": "d1", "cost": 100}).json()
    assert r["allowed"] is True
    assert r["buckets"][0]["capacity"] == 100.0
    assert r["buckets"][0]["balance"] == 0.0
    assert client.get("/policy-changes/ch1").json()["status"] == "pending"
    # 桶观察接口同样仍是旧容量
    assert client.get("/buckets").json()[0]["capacity"] == 100.0


def test_change_application_scales_balance_proportionally(client):
    # 旧容量 100、速率 0；花掉 20 -> 剩余比例 0.8
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "d1", "cost": 20})
    post_change(client, "ch1", "global", 50, 0, effective_in=1)

    import time
    time.sleep(1.2)
    # 到期首次裁决：余额应换算为 0.8*50 = 40；速率仍为 0
    r = client.post("/decide", json={"request_id": "d2", "cost": 40}).json()
    assert r["allowed"] is True
    b = r["buckets"][0]
    assert b["capacity"] == 50.0
    assert b["refill_rate"] == 0.0
    assert b["balance"] == pytest.approx(0.0, abs=1e-9)
    ch = client.get("/policy-changes/ch1").json()
    assert ch["status"] == "applied" and ch["applied_at"]
    # applied 后可以提交新的 pending，也可以直接 PUT
    assert post_change(client, "ch2", "global", 10, 0,
                       effective_in=3600).status_code == 200
    assert client.delete("/policy-changes/ch2").status_code == 200
    assert put_policy(client, "global", 50, 0).status_code == 200


def test_change_application_refills_old_then_new_rate(client):
    # 旧容量 100、速率 10；花掉 80（余 20）。变更在 1s 后生效 -> 旧速率先补满
    # 到 30（20+10*1）；新容量 200 -> 余额 60；随后按新速率 20/s 继续补充。
    put_policy(client, "global", 100, 10)
    client.post("/decide", json={"request_id": "d1", "cost": 80})
    post_change(client, "ch1", "global", 200, 20, effective_in=1)

    import time
    time.sleep(1.3)
    r = client.post("/decide", json={"request_id": "d2", "cost": 1}).json()
    assert r["allowed"] is True
    b = r["buckets"][0]
    assert b["capacity"] == 200.0
    assert b["refill_rate"] == 20.0
    # 60 + 20*（裁决时刻-effective_at 约 0.3s+）- 1，区间断言避开计时抖动
    assert 63.0 < b["balance"] < 75.0, b["balance"]


def test_change_from_zero_capacity_balance_starts_zero(client):
    # 旧容量 0：被拒裁决也会把余额 0 的桶行落库；规格规定旧容量为 0 时余额为 0，
    # 故切换后余额从 0 开始按「新速率」补充（而不是凭空按新容量满额）
    put_policy(client, "global", 0, 0)
    r = client.post("/decide", json={"request_id": "d1", "cost": 1}).json()
    assert r["allowed"] is False
    assert client.get("/buckets").json()[0]["balance"] == 0.0
    post_change(client, "ch1", "global", 10, 100, effective_in=1)

    import time
    time.sleep(1.15)
    # 生效瞬间余额 0，新速率 100/s 补充到裁决时刻，已回满到新容量 10
    r = client.post("/decide", json={"request_id": "d2", "cost": 10}).json()
    assert r["allowed"] is True
    assert r["buckets"][0]["capacity"] == 10.0
    assert r["buckets"][0]["balance"] == 0.0


def test_change_with_old_capacity_zero_and_existing_bucket(client):
    # 旧容量 0 但桶行已存在（先非零打空再 PUT 成 0）：换算后余额必须为 0
    put_policy(client, "global", 5, 0)
    client.post("/decide", json={"request_id": "d1", "cost": 5})
    put_policy(client, "global", 0, 0)
    post_change(client, "ch1", "global", 100, 0, effective_in=1)

    import time
    time.sleep(1.2)
    r = client.post("/decide", json={"request_id": "d2", "cost": 1}).json()
    assert r["allowed"] is False
    assert r["buckets"][0]["balance"] == 0.0


def test_change_threshold_applies_after_effective(client):
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "d1", "cost": 50})  # 余 50，旧阈值关闭
    post_change(client, "ch1", "global", 100, 0, threshold=10, effective_in=1)

    import time
    time.sleep(1.2)
    # 50 -> 45：仍高于新阈值 10，无事件
    client.post("/decide", json={"request_id": "d2", "cost": 5})
    assert client.get("/events").json() == []
    # 45 -> 5：越过新阈值，产生事件，事件记录的是新阈值
    client.post("/decide", json={"request_id": "d3", "cost": 40})
    events = client.get("/events").json()
    assert len(events) == 1
    assert events[0]["threshold"] == 10.0
    assert events[0]["balance_before"] == 45.0
    assert events[0]["balance_after"] == 5.0


def test_change_application_is_multilevel_and_atomic(client):
    put_policy(client, "global", 100, 0)
    put_policy(client, "tenant", 100, 0, tenant="t")
    client.post("/decide", json={"request_id": "d1", "tenant_id": "t", "cost": 50})
    post_change(client, "cg", "global", 10, 0, effective_in=1)
    post_change(client, "ct", "tenant", 10, 0, tenant="t", effective_in=1)

    import time
    time.sleep(1.2)
    # 同一裁决事务同时应用两条变更：两桶余额都按 0.5 比例缩到 5
    r = client.post(
        "/decide", json={"request_id": "d2", "tenant_id": "t", "cost": 5}
    ).json()
    assert r["allowed"] is True
    by_scope = {b["scope"]: b["balance"] for b in r["buckets"]}
    assert by_scope == {"global": 0.0, "tenant": 0.0}
    statuses = {c["change_id"]: c["status"] for c in client.get(
        "/policy-changes").json()}
    assert statuses == {"cg": "applied", "ct": "applied"}


def test_concurrent_decisions_apply_change_consistently(client):
    # 旧容量 100 满额 -> 2s 后切换为容量 50（满额比例 1 => 50），速率始终 0。
    # 阶段一在生效前打 60 个：全部按旧策略放行，余 40（无任何请求看到新策略）；
    # 等切换到期后阶段二并发打 60 个：新容量余额 50/100*50=20，只能再放 20。
    # 每个请求看到的要么全旧要么全新，总扣减恰好 80，绝不超发。
    put_policy(client, "global", 100, 0)
    post_change(client, "cg", "global", 50, 0, effective_in=2)

    results: list[bool] = []
    caps: list[float] = []
    lock = threading.Lock()

    def worker(prefix, n):
        for i in range(n):
            body = client.post(
                "/decide", json={"request_id": f"{prefix}-{i}", "cost": 1}
            ).json()
            with lock:
                results.append(body["allowed"])
                caps.append(body["buckets"][0]["capacity"])

    t1 = [threading.Thread(target=worker, args=(f"b{j}", 15)) for j in range(4)]
    for t in t1:
        t.start()
    for t in t1:
        t.join()
    assert all(cap == 100.0 for cap in caps), caps
    assert sum(results) == 60

    import time
    time.sleep(2.2)  # 等待生效

    t2 = [threading.Thread(target=worker, args=(f"a{j}", 15)) for j in range(4)]
    for t in t2:
        t.start()
    for t in t2:
        t.join()

    # 生效后的请求全部看到新容量（不会出现旧状态/中间态）
    assert all(cap == 50.0 for cap in caps[60:]), caps[60:]
    assert sum(results) == 80
    buckets = client.get("/buckets").json()
    assert buckets[0]["capacity"] == 50.0
    assert buckets[0]["balance"] == 0.0
    assert client.get("/policy-changes/cg").json()["status"] == "applied"


def test_revoke_applies_due_change_before_refund(client):
    # 旧容量 100 速率 0：花掉 80（余 20）；1s 后切到容量 50、速率 100。
    # 到期后先撤销：变更应在撤销事务内先应用（余额 20/100*50=10），
    # 再按新速率补充并补回 80，最终以新容量 50 截断 -> 恰好 50
    put_policy(client, "global", 100, 0)
    client.post("/decide", json={"request_id": "d1", "cost": 80})
    post_change(client, "ch1", "global", 50, 100, effective_in=1)

    import time
    time.sleep(1.15)
    r = client.post("/decide/d1/revoke")
    assert r.status_code == 200
    b = r.json()["buckets"][0]
    assert b["capacity"] == 50.0
    assert b["refill_rate"] == 100.0
    assert b["balance"] == 50.0
    assert client.get("/policy-changes/ch1").json()["status"] == "applied"


def test_cancel_after_effective_time_conflicts(client):
    put_policy(client, "global", 100, 0)
    post_change(client, "ch1", "global", 50, 0, effective_in=1)
    assert client.delete("/policy-changes/ch1").status_code == 200
    # 取消后已无 pending，可以重新提交一条
    post_change(client, "ch2", "global", 50, 0, effective_in=1)

    import time
    time.sleep(1.2)
    # 到期后取消 -> 409；触发一次裁决令其应用
    assert client.delete("/policy-changes/ch2").status_code == 409
    client.post("/decide", json={"request_id": "d1", "cost": 1})
    assert client.get("/policy-changes/ch2").json()["status"] == "applied"
    assert client.delete("/policy-changes/ch2").status_code == 409


# ------------------------------------------------------------ 策略版本审计与回滚


def test_put_appends_monotonic_versions_and_history(client):
    r = put_policy(client, "global", 100, 0, threshold=10)
    assert r.status_code == 200
    put_policy(client, "global", 50, 1, threshold=5)
    put_policy(client, "tenant", 8, 2, tenant="t", threshold=1)

    h = history(client, "global").json()
    # 新到旧：version 2 在前
    assert [v["version"] for v in h] == [2, 1]
    v2, v1 = h
    assert v1["source"] == "put"
    assert v1["capacity"] == 100.0 and v1["refill_rate"] == 0.0
    assert v1["low_balance_threshold"] == 10.0
    assert v1["change_id"] is None and v1["restored_from_version"] is None
    assert v1["created_at"]
    assert v2["capacity"] == 50.0 and v2["refill_rate"] == 1.0
    assert v2["low_balance_threshold"] == 5.0
    # 历史只含该策略：tenant 策略不在 global 历史里
    assert all(v["scope"] == "global" for v in h)
    th = history(client, "tenant", tenant_id="t").json()
    assert [v["version"] for v in th] == [1]
    assert th[0]["capacity"] == 8.0
    assert th[0]["tenant_id"] == "t" and th[0]["route_id"] is None
    # PUT 为全量替换：不传阈值即关闭，也应有独立版本
    put_policy(client, "global", 50, 1)
    assert history(client, "global").json()[0]["low_balance_threshold"] is None


def test_history_filters_by_policy_key(client):
    put_policy(client, "global", 10, 0)
    put_policy(client, "tenant", 10, 0, tenant="t1")
    put_policy(client, "route", 10, 0, tenant="t1", route="rA")
    # 不同策略键各自独立计数
    assert history(client, "global").json()[0]["version"] == 1
    assert history(client, "tenant", tenant_id="t1").json()[0]["version"] == 1
    assert history(client, "route", tenant_id="t1", route_id="rA").json()[0][
        "version"
    ] == 1
    # scope 必传 / 非法组合 -> 422
    assert client.get("/policies/history").status_code == 422
    assert client.get(
        "/policies/history", params={"scope": "wat"}
    ).status_code == 422
    assert history(client, "tenant").status_code == 422
    assert history(client, "route", tenant_id="t1").status_code == 422
    # 从未创建过的策略：历史为空（不报错）
    assert history(client, "tenant", tenant_id="nope").json() == []


def test_rollback_restores_parameters_and_appends_version(client):
    put_policy(client, "global", 100, 0, threshold=10)  # v1
    put_policy(client, "global", 50, 1, threshold=5)    # v2
    put_policy(client, "global", 20, 2)                 # v3

    r = rollback(client, "global", 1)
    assert r.status_code == 200
    nv = r.json()
    assert nv["version"] == 4
    assert nv["source"] == "rollback"
    assert nv["restored_from_version"] == 1
    assert nv["capacity"] == 100.0
    assert nv["refill_rate"] == 0.0
    assert nv["low_balance_threshold"] == 10.0
    # 策略本身已恢复
    p = client.get("/policies").json()[0]
    assert p["capacity"] == 100.0 and p["refill_rate"] == 0.0
    assert p["low_balance_threshold"] == 10.0
    # 历史新到旧，回滚版本在最前
    h = history(client, "global").json()
    assert [v["version"] for v in h] == [4, 3, 2, 1]
    assert h[0]["source"] == "rollback"
    assert h[1]["source"] == "put"
    # 回滚后裁决按恢复后的策略走：容量 100
    d = client.post("/decide", json={"request_id": "d1", "cost": 100}).json()
    assert d["allowed"] is True
    assert d["buckets"][0]["balance"] == 0.0


def test_rollback_clamps_existing_bucket_to_target_capacity(client):
    # v1 容量 20、速率 0：花掉 5 -> 余 15；v2 容量 100、速率 100。
    # v2 生效后桶被补满到 100；回滚到 v1（容量 20）时先按当前策略补充，
    # 再截断回 20。
    put_policy(client, "global", 20, 0)
    client.post("/decide", json={"request_id": "d1", "cost": 5})
    put_policy(client, "global", 100, 100)

    import time
    time.sleep(1.0)
    # 一次裁决把补充后的高余额落库：补满到 100 再扣 1 -> 99
    d = client.post("/decide", json={"request_id": "d2", "cost": 1}).json()
    assert d["allowed"] is True
    assert d["buckets"][0]["balance"] == 99.0

    assert rollback(client, "global", 1).status_code == 200
    b = client.get("/buckets").json()[0]
    assert b["capacity"] == 20.0
    assert b["balance"] == 20.0


def test_rollback_refills_with_current_policy_before_clamp(client):
    # 当前容量 100、速率 100，回滚目标容量 200（放大）：
    # 先按当前策略补充（上限 100），再截到 200 -> 100（不是目标容量 200）。
    put_policy(client, "global", 200, 0)                    # v1
    client.post("/decide", json={"request_id": "d1", "cost": 150})  # 余 50
    put_policy(client, "global", 100, 100)                  # v2，落库余额 50
    import time
    time.sleep(0.7)  # 50 + 100*0.7 = 120 -> 按当前容量封顶 100
    r = rollback(client, "global", 1)
    assert r.status_code == 200
    assert client.get("/buckets").json()[0]["balance"] == 100.0


def test_rollback_errors(client):
    put_policy(client, "global", 10, 0)
    # 未知策略 -> 404
    assert rollback(client, "tenant", 1, tenant="nope").status_code == 404
    # 未知版本 -> 404
    assert rollback(client, "global", 99).status_code == 404
    assert rollback(client, "global", 0).status_code == 422
    # 非法 scope 组合 -> 422
    assert rollback(client, "tenant", 1).status_code == 422


def test_rollblock_blocked_by_pending_change(client):
    put_policy(client, "global", 100, 0)  # v1
    put_policy(client, "global", 50, 0)   # v2
    post_change(client, "ch1", "global", 20, 0, effective_in=3600)
    # 有 pending：回滚 409，策略与历史不变
    r = rollback(client, "global", 1)
    assert r.status_code == 409
    p = client.get("/policies").json()[0]
    assert p["capacity"] == 50.0
    assert [v["version"] for v in history(client, "global").json()] == [2, 1]
    # 取消后可以回滚
    assert client.delete("/policy-changes/ch1").status_code == 200
    assert rollback(client, "global", 1).status_code == 200


def test_scheduled_change_application_appends_version(client):
    put_policy(client, "global", 100, 0)  # v1 put
    post_change(client, "ch1", "global", 50, 0, effective_in=1)
    assert [v["version"] for v in history(client, "global").json()] == [1]

    import time
    time.sleep(1.2)
    client.post("/decide", json={"request_id": "d1", "cost": 1})
    h = history(client, "global").json()
    assert [v["version"] for v in h] == [2, 1]
    v2 = h[0]
    assert v2["source"] == "scheduled"
    assert v2["change_id"] == "ch1"
    assert v2["restored_from_version"] is None
    assert v2["capacity"] == 50.0


def test_rollback_after_scheduled_change(client):
    put_policy(client, "global", 100, 0)  # v1 put
    post_change(client, "ch1", "global", 50, 0, effective_in=1)
    import time
    time.sleep(1.2)
    client.post("/decide", json={"request_id": "d1", "cost": 1})  # v2 scheduled
    # 回到变更前的 v1
    r = rollback(client, "global", 1)
    assert r.status_code == 200
    assert r.json()["source"] == "rollback"
    assert r.json()["restored_from_version"] == 1
    assert client.get("/policies").json()[0]["capacity"] == 100.0
    h = history(client, "global").json()
    assert [v["version"] for v in h] == [3, 2, 1]
    assert [v["source"] for v in h] == ["rollback", "scheduled", "put"]
    # 已应用的变更不受影响，且 applied 后无 pending，回滚不被阻挡
    assert client.get("/policy-changes/ch1").json()["status"] == "applied"


def test_old_database_is_backfilled_with_baseline_versions(tmp_path):
    """老库升级：现有策略补登 version=1 baseline，随后变更从 version 2 递增。"""
    import sqlite3

    from app import database, quota

    db_file = tmp_path / "oldv.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        "CREATE TABLE policies ("
        "  scope TEXT NOT NULL, tenant_id TEXT NOT NULL DEFAULT '',"
        "  route_id TEXT NOT NULL DEFAULT '', capacity REAL NOT NULL,"
        "  refill_rate REAL NOT NULL, updated_at TEXT NOT NULL,"
        "  PRIMARY KEY (scope, tenant_id, route_id)) STRICT"
    )
    conn.execute(
        "INSERT INTO policies VALUES ('global', '', '', 10, 1, '2026-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    database.init_db(str(db_file))
    conn = database.connect(str(db_file))
    try:
        from app.database import write_tx

        with write_tx(conn):
            h = quota.list_policy_versions(conn, scope="global")
            assert len(h) == 1
            assert h[0]["version"] == 1
            assert h[0]["source"] == "baseline"
            assert h[0]["capacity"] == 10.0
            assert h[0]["created_at"] == "2026-01-01T00:00:00Z"
            # baseline 可直接作为回滚目标
            rolled = quota.rollback_policy(
                conn, scope="global", tenant_id=None, route_id=None, version=1
            )
            assert rolled["version"] == 2
            assert rolled["source"] == "rollback"
            assert rolled["restored_from_version"] == 1
        # 再次 init_db 幂等：不重复补登
        database.init_db(str(db_file))
        with write_tx(conn):
            h2 = quota.list_policy_versions(conn, scope="global")
        assert [v["version"] for v in h2] == [2, 1]
        assert sum(v["source"] == "baseline" for v in h2) == 1
    finally:
        conn.close()


def test_policy_versions_are_append_only(tmp_path):
    """审计表在数据库层拒绝 UPDATE / DELETE（触发器）。"""
    import sqlite3

    from app import database, quota

    db_file = tmp_path / "imm.db"
    database.init_db(str(db_file))
    conn = database.connect(str(db_file))
    try:
        with database.write_tx(conn):
            quota.upsert_policy(
                conn, scope="global", tenant_id=None, route_id=None,
                capacity=10, refill_rate=1,
            )
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("UPDATE policy_versions SET capacity=999 WHERE version=1")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("DELETE FROM policy_versions WHERE version=1")
        # 记录完好无损，且仍可继续追加
        with database.write_tx(conn):
            quota.upsert_policy(
                conn, scope="global", tenant_id=None, route_id=None,
                capacity=11, refill_rate=1,
            )
        rows = conn.execute(
            "SELECT version, capacity FROM policy_versions ORDER BY version"
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [(1, 10.0), (2, 11.0)]
    finally:
        conn.close()

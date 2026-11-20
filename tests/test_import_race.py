"""首批建档竞争、失败不占首版、旧库冲突态的回归测试。

- 两窗口同时首批导入：仅一批 200，另一批 409；现势/流水只收录胜出批；
- 胜出批登记分割后版本到 v2，再次导入仍 409，版本不退回；
- 校验失败的导入不占用 v1、不留流水；
- 旧库若遗留多笔初始导入：/api/status 报 conflict，现势/历史/快照/分合
  一律 409 并给出冲突流水号，原始流水仍可查，再导入也被拒；
- 只有一笔初始导入的正常旧库可直接继续使用。
"""
import os
import threading

import pytest
from fastapi.testclient import TestClient

from app import db
from app.main import app


def fc(*items):
    feats = []
    for pid, ring in items:
        feats.append({
            "type": "Feature",
            "properties": {"parcel_id": pid},
            "geometry": {"type": "Polygon", "coordinates": [ring]},
        })
    return {"type": "FeatureCollection", "features": feats}


SQUARE = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
LEFT = [[0, 0], [5, 0], [5, 10], [0, 10], [0, 0]]
RIGHT = [[5, 0], [10, 0], [10, 10], [5, 10], [5, 0]]

# 两批竞争数据：几何相同、宗地号不同，便于判定谁胜出
BATCH_A = fc(("P_A", SQUARE))
BATCH_B = fc(("P_B", SQUARE))

WKT = {
    "P1": "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
    "P1A": "POLYGON ((0 0, 5 0, 5 10, 0 10, 0 0))",
    "P1B": "POLYGON ((5 0, 10 0, 10 10, 5 10, 5 0))",
    "Q1": "POLYGON ((100 0, 110 0, 110 10, 100 10, 100 0))",
}


def change_req(version, retired, new_items, reason=None):
    return {
        "base_version": version,
        "retired_ids": retired,
        "new_parcels": [
            {"parcel_id": pid, "geometry": {"type": "Polygon", "coordinates": [ring]}}
            for pid, ring in new_items
        ],
        "reason": reason,
    }


@pytest.fixture()
def fresh_db(tmp_path, monkeypatch):
    path = str(tmp_path / "case.db")
    monkeypatch.setenv("CADASTRE_DB", path)
    yield path


def test_concurrent_first_import_only_one_wins(fresh_db, monkeypatch):
    """两窗口同时提交首批：校验阶段互不阻塞，写事务串行，仅一批胜出。"""
    barrier = threading.Barrier(2)
    real_parse = db.parse_feature_collection

    def synced_parse(payload):
        # 让两线程都通过预检、同时处于几何校验中，再一起走向写事务
        barrier.wait(timeout=15)
        return real_parse(payload)

    monkeypatch.setattr(db, "parse_feature_collection", synced_parse)

    results = {}

    def worker(key, payload):
        with TestClient(app) as c:
            r = c.post("/api/import", json=payload)
            results[key] = (r.status_code, r.json())

    threads = [
        threading.Thread(target=worker, args=("a", BATCH_A)),
        threading.Thread(target=worker, args=("b", BATCH_B)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(v[0] for v in results.values()) == [200, 409]
    loser = [body for code, body in results.values() if code == 409][0]
    assert "建档" in loser["detail"] or "现势" in loser["detail"]

    with TestClient(app) as c:
        # 现势与流水只收录胜出的首批，版本为 v1
        st = c.get("/api/state").json()
        assert st["current_version"] == 1
        winner_ids = [p["parcel_id"] for p in st["parcels"]]
        assert winner_ids in (["P_A"], ["P_B"])
        txns = c.get("/api/txns").json()["txns"]
        assert len(txns) == 1
        assert txns[0]["kind"] == "import" and txns[0]["new_version"] == 1

        # 胜出批登记分割 → v2；再次导入仍被拒，版本不退回 v1
        split = change_req(1, winner_ids, [("N1", LEFT), ("N2", RIGHT)])
        r = c.post("/api/changes/commit", json=split)
        assert r.status_code == 200 and r.json()["new_version"] == 2
        r2 = c.post("/api/import", json=BATCH_B)
        assert r2.status_code == 409
        st2 = c.get("/api/state").json()
        assert st2["current_version"] == 2
        assert sorted(p["parcel_id"] for p in st2["parcels"]) == ["N1", "N2"]
        assert len(c.get("/api/txns").json()["txns"]) == 2


def test_failed_import_does_not_occupy_v1(fresh_db):
    """校验失败的导入回滚：不占首版、不留流水，后续合格导入仍是 v1。"""
    bad = fc(("X1", [[0, 0], [1.5, 0], [1.5, 2], [0, 2], [0, 0]]))  # 非整数坐标
    with TestClient(app) as c:
        assert c.post("/api/import", json=bad).status_code == 422
        r = c.post("/api/import", json=BATCH_A)
        assert r.status_code == 200 and r.json()["new_version"] == 1
        txns = c.get("/api/txns").json()["txns"]
        assert len(txns) == 1 and txns[0]["txn_id"] == 1


def _build_legacy_conflicted_db(path):
    """模拟旧版竞态留下的库：两笔 import 流水、第二批混入 v1、版本被打回 v1。"""
    conn = db.connect(path)
    db.init_db(conn)
    now = "2026-01-01T00:00:00+00:00"

    def add_txn(kind, base, new):
        cur = conn.execute(
            "INSERT INTO txns (kind, base_version, new_version, reason, report_json, created_at) "
            "VALUES (?, ?, ?, ?, NULL, ?)",
            (kind, base, new, "初始导入" if kind == "import" else "宗地分割", now),
        )
        return cur.lastrowid

    def add_pv(pid, ver, status, wkt, area, created, retired=None):
        conn.execute(
            "INSERT INTO parcel_versions "
            "(parcel_id, version, status, geom_wkt, area, created_txn_id, retired_txn_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (pid, ver, status, wkt, area, created, retired),
        )

    def add_item(txn, role, pid, wkt, area):
        conn.execute(
            "INSERT INTO txn_items (txn_id, role, parcel_id, geom_wkt, area) "
            "VALUES (?, ?, ?, ?, ?)",
            (txn, role, pid, wkt, area),
        )

    t1 = add_txn("import", 0, 1)          # 首批：P1
    add_pv("P1", 1, "current", WKT["P1"], 100.0, t1)
    add_item(t1, "new", "P1", WKT["P1"], 100.0)
    t2 = add_txn("change", 1, 2)          # 已登记分割：P1 → P1A/P1B
    add_pv("P1", 2, "retired", WKT["P1"], 100.0, t1, t2)
    add_pv("P1A", 2, "current", WKT["P1A"], 50.0, t2)
    add_pv("P1B", 2, "current", WKT["P1B"], 50.0, t2)
    add_item(t2, "retired", "P1", WKT["P1"], 100.0)
    add_item(t2, "new", "P1A", WKT["P1A"], 50.0)
    add_item(t2, "new", "P1B", WKT["P1B"], 50.0)
    t3 = add_txn("import", 0, 1)          # 旧 bug：第二批混入 v1
    add_pv("Q1", 1, "current", WKT["Q1"], 100.0, t3)
    add_item(t3, "new", "Q1", WKT["Q1"], 100.0)
    # 旧 bug 的签名：现势版号被第二批导入从 v2 打回 v1
    conn.execute("INSERT INTO meta (key, value) VALUES ('current_version', '1')")
    conn.commit()
    conn.close()
    return [t1, t2, t3]


def test_legacy_conflicted_db_is_quarantined(fresh_db):
    """多笔初始导入的旧库：查询与分合暂停、冲突流水号可见、流水仍可查。"""
    _build_legacy_conflicted_db(fresh_db)
    with TestClient(app) as c:
        st = c.get("/api/status").json()
        assert st["status"] == "conflict"
        assert st["conflict_txn_ids"] == [1, 3]

        # 现势 / 宗地历史 / 版本快照：一律 409 并给出冲突流水号
        for path in ["/api/state", "/api/history/parcel/P1", "/api/history/version/1"]:
            r = c.get(path)
            assert r.status_code == 409, path
            body = r.json()
            assert body["conflict_txn_ids"] == [1, 3]
            assert "核对" in body["detail"]

        # 分合登记（预核验与确认）同样暂停
        req = change_req(1, ["P1A"], [("Z1", LEFT)])
        assert c.post("/api/changes/plan", json=req).status_code == 409
        assert c.post("/api/changes/commit", json=req).status_code == 409

        # 再导入也被拒（库已建档）
        assert c.post("/api/import", json=BATCH_A).status_code == 409

        # 原始流水仍可查：列表与详情（含落败批次的痕迹）
        txns = c.get("/api/txns").json()["txns"]
        assert [t["txn_id"] for t in txns] == [1, 2, 3]
        assert [t["kind"] for t in txns] == ["import", "change", "import"]
        detail = c.get("/api/txns/3").json()
        assert detail["kind"] == "import"
        assert [i["parcel_id"] for i in detail["items"]] == ["Q1"]


def test_normal_existing_db_keeps_working(fresh_db):
    """正常旧库（单笔初始导入 + 已登记分割）：重启进程后可直接继续使用。"""
    with TestClient(app) as c1:
        assert c1.post("/api/import", json=BATCH_A).status_code == 200
        split = change_req(1, ["P_A"], [("P_A1", LEFT), ("P_A2", RIGHT)])
        assert c1.post("/api/changes/commit", json=split).status_code == 200

    # 模拟另一进程打开同一库文件
    with TestClient(app) as c2:
        st = c2.get("/api/status").json()
        assert st["status"] == "ok" and st["current_version"] == 2
        assert st["conflict_txn_ids"] == []
        state = c2.get("/api/state").json()
        assert state["current_version"] == 2
        assert sorted(p["parcel_id"] for p in state["parcels"]) == ["P_A1", "P_A2"]
        # 可继续登记分合
        merge = change_req(2, ["P_A1", "P_A2"], [("P_AC", SQUARE)])
        r = c2.post("/api/changes/commit", json=merge)
        assert r.status_code == 200 and r.json()["new_version"] == 3
        hist = c2.get("/api/history/parcel/P_A").json()
        assert [(v["version"], v["status"]) for v in hist["versions"]] == [
            (1, "current"), (2, "retired"),
        ]


def test_empty_db_status(fresh_db):
    with TestClient(app) as c:
        st = c.get("/api/status").json()
        assert st == {"status": "empty", "current_version": 0, "conflict_txn_ids": []}

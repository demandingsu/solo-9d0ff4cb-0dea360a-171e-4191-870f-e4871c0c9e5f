# 地籍变更系统（分割 / 合并）

FastAPI + SQLite + Shapely 实现的网页应用：导入单一平面坐标系下、整数坐标、
无洞、互不重叠的 GeoJSON 宗地，支持宗地分割/合并变更的预核验、问题区域预览、
原子确认提交，以及现势与历史边界、变更流水查询。

## 启动（Docker Compose，双进程共享同一库）

```bash
docker compose up --build
```

Compose 会启动 **两个** 服务进程，挂载同一个 `./data` 目录、共用
`/data/cadastre.db`：

- <http://localhost:8001>（cadastre-a）
- <http://localhost:8002>（cadastre-b）

**竞争建档演示**：在两个浏览器窗口分别打开 8001 与 8002，都粘贴
`samples/initial.geojson`（可改一处的宗地号以区分），近似同时点击
「导入并建立现势库」——只有一批成功（200），另一窗口收到 409
「数据库已完成初始建档」，输入保留在文本框中，页面自动刷新为胜出批的现势。
随后任一窗口登记分割/合并，另一窗口刷新即可看到相同现势。

本地无 Docker 时（单进程）：

```bash
pip install -r requirements.txt
CADASTRE_DB=./data/cadastre.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 业务规则

1. **导入**：FeatureCollection，每个 Feature 的 `properties.parcel_id` 为宗地号；
   几何必须是 Polygon、整数坐标、无内环（无洞）、面积大于 0；初始宗地之间不得
   面积重叠，共边允许。仅允许在空库导入一次。
2. **首批建档竞争**：几何校验不持锁——先提交的批次还在校验时，另一批合格
   数据可以先完成建档；真正的判定在 `BEGIN IMMEDIATE` 写事务内复核库仍为空。
   共享同一库文件的进程**仅一批**首批导入生效；落败方整体回滚、不占用首版，
   收到 409，现势/历史/流水只收录胜出的首批及随后已登记的分合。
3. **变更请求**：指定 `retired_ids`（退出的现势宗地号）与 `new_parcels`（接替新面）。
   - 新面之间不得面积重叠（共边允许）；
   - 新面不得侵占任何未退出的现势宗地；
   - 新面并集须**恰好**等于退出旧面并集：少了即「缺口」，多了即「越界」；
   - 新宗地号不得与现势宗地号冲突。
4. **两阶段操作**：先 `POST /api/changes/plan` 预核验，返回缺口/侵占/越界/互叠的
   GeoJSON 几何，网页在 SVG 地图上以红/橙/紫/粉色叠加预览；核验通过后才能
   `POST /api/changes/commit` 确认。
5. **乐观并发**：每个变更请求携带 `base_version`。提交在 `BEGIN IMMEDIATE`
   事务内复核当前版本，版本不一致返回 409。基于同一版本的两笔提交只有一笔生效。
6. **失败原子性**：任何核验或写入失败都回滚整个事务，现势与历史保持原状，
   失败的首批导入不占用 v1、不留流水。
7. **历史保留**：宗地版本行 append-only——退出时追加 `retired` 行（旧行不改），
   新设时追加 `current` 行；可按宗地号查全版本边界，也可按版本号查当时的现势快照。
8. **旧库冲突隔离**：若库中遗留多笔初始导入流水（旧版竞态后遗症），
   `GET /api/status` 返回 `conflict` 及冲突流水号，网页顶部列出可点击的冲突
   流水号供档案员核对；现势、宗地历史、版本快照查询及分合登记一律 409 暂停，
   原始流水（`/api/txns`）仍可查询。只有一笔初始导入的正常旧库可直接继续使用。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/status` | 档案健康状态：empty / ok / conflict（含冲突流水号） |
| GET | `/api/state` | 当前版本与全部现势宗地（冲突库返回 409） |
| POST | `/api/import` | 初始导入 GeoJSON（竞争落败/重复导入返回 409） |
| POST | `/api/changes/plan` | 预核验（不落库），返回问题区域几何 |
| POST | `/api/changes/commit` | 确认提交（原子写入，版本冲突返回 409） |
| GET | `/api/history/parcel/{id}` | 单宗地全历史版本边界（冲突库 409） |
| GET | `/api/history/version/{v}` | 截至 v 版本的现势快照（冲突库 409） |
| GET | `/api/txns` | 变更流水列表（冲突库仍可查） |
| GET | `/api/txns/{id}` | 单笔流水详情（退出/新设明细 + 核验报告） |

## 样例数据（`samples/`）

- `initial.geojson`：P1、P2 两宗共边相邻宗地，用于初始导入；
- `split_p1.geojson`：P1 拆分为 P1A/P1B（共边 x=5）；
- `merge_p1.geojson`：P1A/P1B 合并回 P1C；
- `invalid_change.geojson`：只覆盖 P1 左部的问题面，用于预览缺口。

## 测试

```bash
pip install -r requirements.txt pytest httpx
pytest -v
```

覆盖：导入校验（非整数/带洞/重叠/重复导入）、缺口与侵占预核验、预核验不落库、
非法提交被拒且数据不变、拆分、合并、历史与版本快照、多线程竞争提交（200+409）、
失败回滚、**双进程竞争首批建档（仅一批 200，另一批 409，版本不退回）**、
**失败导入不占首版**、**多笔初始导入的旧库隔离（查询与分合 409、流水可查）**、
正常旧库继续可用。

# 燃气管线泄漏检测与隔离协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8333`。

- `app.py`：参数、依赖和服务生命周期。
- `src/domain.py`：管段、传感值和来源记录校验。
- `src/rules.py`：泄漏评分、阀门顺序、修复、试压、恢复状态机。
- `src/repository.py`：SQLite、重复保护、乐观版本和审计链。
- `src/service.py`：角色权限和业务编排。
- `src/http_api.py`：JSON 接口与首页。
- `src/audit.py`：可校验的审计事件。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8333
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。测试覆盖完整抢修流程、重复事件、阀门顺序、试压阈值、现场危险条件、权限和版本冲突。模型不替代 SCADA、管网水力计算或正式应急预案。

## 片区台账与隔离作业

管网按片区分开后，隔离任务、阀门指令与片区台账打通，全部落库到 SQLite，服务重启后容量占用、待复核记录和未完成指令照旧接得上。

台账（中心调度维护）：

- `POST /api/districts`、`GET /api/districts`：片区与容量（`capacity` 为可同时开展的隔离作业数，`active_count` 为当前占用）。
- `POST /api/segments`、`GET /api/segments`：管段归属片区。
- `POST /api/valves`、`GET /api/valves`：阀门台账；`is_boundary` 边界阀通过 `shared_with` 登记相邻共用片区。

隔离任务（调度员开本片区管段的单，越权开单返回 `region_overstep` 403）：

- `POST /api/isolation-tasks`：按 `X-Region` 校验管段归属；跨片区（动边界阀或多片区阀门）进入 `pending_center`，需中心调度确认。
- `POST /api/isolation-tasks/<id>/confirm`：中心调度确认跨片区联合作业（`X-Role: center`）。
- 容量满时新任务排队（`queued`），完工或取消后自动补位最早排队任务。
- `POST /api/isolation-tasks/<id>/complete`、`.../cancel`：完工/取消并释放容量。

阀门指令（按顺序下发，失败从断点重试，重复回执不重复执行）：

- `POST /api/isolation-tasks/<id>/issue`：下发关阀指令。
- `POST /api/valve-commands/<id>/ack`：回执 `succeeded`/`failed`；已成功的指令重复回执不重复执行，跳序回执返回 `command_out_of_order`。
- `POST /api/valve-commands/<id>/retry`：失败后从断点重试。

断网阀位合并（回网后合并，同阀两份记录并排列出等复核，不能直接推进）：

- `POST /api/isolation-tasks/<id>/positions`：批量提交阀位记录；同一阀门出现冲突位置时生成复核记录（`record_a`/`record_b` 并列）。
- `GET /api/isolation-tasks/<id>/reviews`：待复核记录。
- `POST /api/isolation-tasks/<id>/reviews/<review_id>/resolve`：复核确认 `a`/`b` 后才能继续下发指令或完工。

身份通过请求头 `X-User-Id`、`X-Role`、`X-Region` 传入；角色含 `dispatcher`、`supervisor`、`technician`、`center`、`regulator`。

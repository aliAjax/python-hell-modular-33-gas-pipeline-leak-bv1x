# 燃气管网隔离与阀门调度协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8333`，数据全部持久化在 SQLite（WAL）。

- `app.py`：参数、依赖和服务生命周期；启动时执行重启续跑（排队准入 + 未完成指令/待复核恢复）。
- `src/domain.py`：管段、传感值、片区台账、阀门序列、离线阀位与回执的校验。
- `src/rules.py`：泄漏评分、角色权限、片区归属、修复/试压/恢复状态机。
- `src/repository.py`：SQLite 表（任务/来源/指令/回执/离线阀位/复核冲突/片区台账）、容量与界阀判定、FIFO 排队准入、重复保护、乐观版本、审计链。
- `src/service.py`：角色权限与业务编排（越权、跨片区确认、容量排队、断网合并、断点重试、回执幂等）。
- `src/gateway`：阀门命令网关抽象 `CommandGateway`；内置脚本网关 `StaticGateway`，生产可替换为 SCADA/RTU 实现。
- `src/http_api.py`：JSON 接口与首页。
- `src/audit.py`：可校验的审计事件。

## 启动

```bash
python3 app.py --init --seed --db ./data.db     # 初始化并播种演示片区台账（EAST/WEST，容量各 1，含界阀 V-B1）
python3 app.py --db ./data.db --port 8333
python3 -m unittest discover -s tests -v
```

## 业务规则

- **台账联动/越权**：管段归属片区台账；调度员只能开本片区管段的单，替别的片区开单返回 `out_of_region`(403)。阀门也登记在片区台账，界阀用 `shared_with` 标注。
- **跨片区联合作业**：隔离序列触及属片区以外的阀门即跨片区；片区调度员发起返回 `joint_confirmation_required`，须由中心调度（`regulator`）发起并 `joint_confirm` 后才占用容量、开始执行。
- **容量与排队**：每个片区有并发隔离容量。容量满、待复核未清或界阀正被相邻片区占用时，新任务进入 `queued`（FIFO）；容量释放/界阀解锁/复核完成后自动准入并下发指令。
- **断网阀位合并**：`POST /api/valve-readings` 批量回网，按 `client_id` 去重；同阀同阀位直接合并，同阀不同阀位把多份记录**并排列出**为待复核（`open`），不直接推进隔离；复核 `resolve` 写入权威阀位后放行。
- **断点重试/回执幂等**：阀门按顺序下发，某步失败停在该断点（`failed`），后续指令不下发；`retry` 从断点重发。同一 `receipt_id` 重放或对已 `acked` 指令重复上报均返回 `duplicate`，不重复执行；失败后重试携带的新回执编号正常更新。
- **持久化与重启**：容量占用由活动任务实时算出，指令、回执、离线记录、复核冲突全部落盘。重启后 `resume_on_startup` 报告排队自动准入与未完成指令（`await_retry`/`await_receipt`），待复核记录原样保留。

## 接口

`GET /health`、`GET /api/state`、`GET /api/queue`、`GET /api/conflicts?status=open`、`GET /api/ledger`，
`POST /api/ledger/regions|segments|valves`，
`POST /api/items`、`POST /api/items/{id}/sources`、`POST /api/items/{id}/actions`，
`POST /api/items/{id}/commands/retry`、`POST /api/items/{id}/commands/{cid}/receipt`，
`POST /api/valve-readings`、`POST /api/conflicts/{id}/resolve`，以及审计查询 `GET /api/items/{id}/audit`。

身份请求头：`X-User-Id`、`X-Role`（dispatcher / supervisor / regulator / technician / responder / patrol）、`X-Region`。隔离、确认、复核、恢复、取消等动作需带 `expected_version`。

测试覆盖完整抢修流程、容量排队与自动放行、界阀两边并发、跨片区确认、越权、离线阀位并列复核、失败断点重试、重复回执幂等，以及关服务再启动后的容量/冲突/指令恢复。模型不替代 SCADA、管网水力计算或正式应急预案。

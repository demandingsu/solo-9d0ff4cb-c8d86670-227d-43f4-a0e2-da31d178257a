# 实验室天平采集系统

FastAPI + SQLite + TCP 天平的称量采集网页。浏览器选择样品、查看实时重量 / 连接状态 /
已存称量；多个页面共用**同一个服务端采集任务与状态机单例**，避免手抄出错和重复记数。

## 一、目录结构

```
.
├── app/                     # FastAPI 采集服务
│   ├── main.py              # REST API、模拟器控制代理、静态网页、生命周期
│   ├── balance_service.py   # 唯一的 TCP 采集后台任务（自动重连、驱动状态机、入库）
│   ├── state_machine.py     # 采集状态机（纯逻辑，无 IO）
│   ├── line_buffer.py       # TCP 报文拆分/粘包的增量换行解码器
│   ├── database.py          # SQLite（aiosqlite，WAL），样品预置 + 称量记录
│   ├── config.py            # 环境变量配置
│   └── web/                 # 原生 HTML/CSS/JS 前端（采集页 + 记录页）
├── simulator/               # 本地天平模拟器（仅标准库）
│   ├── tcp_server.py        # 9000 端口按行推送 JSON，支持拆包/断线/背压
│   ├── control_server.py    # 9001 端口 HTTP 控制接口
│   ├── state.py             # 共享状态：放样重量、抖动、拆包、连接开关
│   └── __main__.py
├── tests/                   # 22 个测试（单元 + 真实 TCP 端到端）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 二、一键启动（Docker Compose）

```bash
docker compose up --build
```

* 采集网页：<http://localhost:8000/>
* 称量记录页：<http://localhost:8000/static/records.html>
* 交互式 API 文档：<http://localhost:8000/docs>
* SQLite 数据保存在命名卷 `balance-data`（容器内 `/data/weighings.db`），重启不丢。

> 本机没有 Docker 时可本地运行：
> ```bash
> pip install -r requirements.txt
> python -m simulator                      # 终端 1
> BALANCE_HOST=127.0.0.1 SIMULATOR_CONTROL_URL=http://127.0.0.1:9001 \
>   DB_PATH=./data/demo.db uvicorn app.main:app --port 8000   # 终端 2
> ```

## 三、天平协议

模拟器（或真实天平）通过 TCP 长连接，以 `\n` 分隔连续发送 JSON：

```json
{"weight_g": 12.5000, "stable": true, "unit": "g", "ts": 1794587930.737}
```

* `weight_g`：克重；`stable`：稳定标志。
* 应用用增量 `LineDecoder` 处理字节流：一行被拆成多次 recv、一次 recv 含多行
  （粘包）都能正确重组；单条畸形报文只告警、不杀死采集循环。
* 判零阈值 `ZERO_EPSILON`（默认 0.0005 g）：`|weight| <= eps` 视为零位。

## 四、采集规则（状态机）

| 阶段 | 含义 | 关键行为 |
| --- | --- | --- |
| `WAIT_ZERO` | 启动 / 断线重连后，尚未重新观察到**稳定零位** | 不允许采集；选择样品会保留但不锁定 |
| `READY` | 已观察稳定零位（0 g），待命 | 可选择/更换样品 |
| `HOLDING` | 重量离开零位，样品已锁定 | 等首个正重量稳定读数入库；回零前拒绝换样、禁止重复记 |

规则逐条对应需求：

1. **离开零位即锁定**当前选中样品（离开瞬间无论稳定与否）。
2. **首次出现正重量稳定读数才记一条**；不稳定读数、负重量稳定读数都不记。
3. 入库成功后 `recorded=true`，**后续稳定读数一律忽略，绝不重复记**。
4. **稳定回零前拒绝换样**（API 返回 409，前端禁用下拉框）；稳定回零后解锁回 `READY`。
5. **入库失败不改变“未记录”事实**：样品保持锁定，后续每次正重量稳定读数都重试，
   成功前库里没有该记录、前端显示“等待/重试入库”。可用页面上的故障注入按钮演示。
6. **断线或应用重启 → 回 `WAIT_ZERO`**，清除锁定；必须重新观察到稳定零位才能下一次采集，
   SQLite 中已有记录全部保留。

## 五、浏览器演示步骤

打开 <http://localhost:8000/>，在“天平模拟器控制”卡片操作（也可用 `curl` 调
`/api/simulator/control`）：

1. 页面顶部显示 **TCP 已连接 / 零位就绪 / 实时 0.0000 g**（连接后自动等稳定零位）。
2. 选择样品（如 `S-001`），点“确认选择样品”。
3. 点“**拆包模式 开**”，再点“**放样**”（默认 12.5 g）——JSON 被切成多段发送，
   后端仍能重组；状态变为“样品锁定中”，出现一条稳定记录，事件日志显示入库 id。
4. 保持放样数秒：记录始终只有 1 条（防重复）；此时换样会被拒绝（409）。
5. 点“**开抖动**”：重量波动、稳定标志变“不稳定”，不产生新记录；“关抖动”恢复稳定。
6. 点“**回零**”：稳定 0 g 后锁定解除，可换下一个样品继续采集。
7. 打开“称量记录”页查询（可按样品筛选、自动刷新）；顶栏状态与采集页完全一致，
   证明多页面共享同一服务端状态。
8. **故障演示**：点“接下来 3 次写入强制失败”后放样，观察“入库失败→自动重试→成功”，
   且失败期间没有半成品记录。
9. 点“**断线**”：连接状态变红、阶段回到“等待稳定零位”；点“**恢复连接**”后，
   模拟器以稳定零位重新上线，应用重新观察零位后才就绪，历史记录仍在记录页。

控制动作一览：`place`（放样）、`jitter_on/off`（抖动）、`zero`（回零）、
`fragment_on/off`（拆包）、`disconnect`（断线）、`reconnect`（恢复连接）。

## 六、HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/status` | 采集状态机快照（阶段、重量、稳定、锁定、事件环等） |
| GET | `/api/samples` | 可选样品列表（预置 6 个） |
| POST | `/api/samples/select` | 选择样品；锁定中返回 409 |
| GET | `/api/weighings?limit=&sample_code=` | 已存称量查询 |
| POST | `/api/simulator/control` | 转发模拟器控制动作 |
| GET | `/api/simulator/state` | 模拟器原始状态 |
| POST | `/api/dev/fail-next-writes` | 故障注入：让接下来 n 次 DB 写入失败 |

## 七、测试

```bash
pip install -r requirements.txt
pytest          # 22 passed
```

覆盖：半包/粘包/CRLF 解码、状态机全部规则、SQLite 增查与持久化、
以及真实 TCP 下的拆包采集、抖动、断线重连、入库失败重试端到端流程。

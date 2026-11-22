# 故障日志检索 · 按需冷启动后端

一个 FastAPI 网页应用：平时不占用后端进程，有查询时自动拉起一个本地 HTTP
子进程检索样例日志，空闲 10 秒后自动回收。同一时刻最多只有一个后端进程。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

本地（不使用 Docker）：

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m uvicorn app.main:app --port 8000
```

## 行为约定

| 需求 | 实现 |
| --- | --- |
| 有查询才启动 | 首次检索时由 `BackendManager` 以子进程方式启动 `app/backend.py`，就绪前返回 503 |
| 并发查询共用一次启动 | 所有等待者挂在同一个 `Round` 上；`starts` 计数只加一次 |
| 每次可设就绪等待上限 | 请求体/查询串 `ready_timeout_ms`（100–60000，缺省取设置里的 `default_ready_timeout_ms`），从提交时开始计时 |
| 超时只结束本次查询 | 超时的调用者收到 504 `ready_timeout`；共享启动继续，其他等候者不受影响 |
| 启动失败或 5 秒未就绪 | 本轮所有仍在等待的查询收到 503 `startup_failed` / 504 `startup_timeout`，进程被回收 |
| 失败后可再次唤醒 | 回收完成后状态回到 `stopped`，下一次查询重新启动 |
| 连续 10 秒无等候/在途才回收 | 只要有 waiter 或 in-flight，空闲计时器就取消；最后一个查询结束后重新计时 10 秒 |
| 回收开始后新查询须等旧进程退出 | 状态进入 `stopping` 时新查询计入 `exit_waiters`，等 `exited` 后才可能新建 Round |
| 任何时候最多一个后端进程 | 子进程由唯一的 round 任务创建、终止和回收；新 Round 仅在旧 Round 完成后才创建 |
| 网页显示结果和进程状态 | `GET /`；右侧面板每 0.8s 刷新状态与生命周期事件 |
| 可调启动和应答延迟 | 启动延迟（下次冷启动生效，上限 4s，受 5s 就绪上限约束）/ 应答延迟（写入运行时配置文件，对运行中的后端立即生效） |

额外的健壮性：后端进程意外死亡时，在途查询收到 503 `backend_exited`，随后状态
回到 `stopped` 可再次唤醒；服务关闭时会终止并 `wait()` 回收子进程，不留僵尸。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页 |
| GET | `/api/search?q=&limit=&ready_timeout_ms=` | 检索（GET 便于直接 curl） |
| POST | `/api/search` | 同上，JSON `{query, limit, ready_timeout_ms}` |
| GET | `/api/status` | 状态、计数、最近生命周期事件 |
| GET/PUT | `/api/settings` | 读取/保存启动延迟、应答延迟、缺省等待上限 |
| GET | `/api/health` | 容器健康检查 |

错误体统一为 `{"error": "<kind>", "detail": "..."}`，
`ready_timeout` / `startup_timeout` → 504，其余 → 503。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `BACKEND_STARTUP_TIMEOUT_S` | 5 | 冷启动硬上限，超时即本轮失败 |
| `BACKEND_IDLE_TIMEOUT_S` | 10 | 空闲回收窗口 |
| `BACKEND_DATA_DIR` | `./data` | 持久化设置与运行时配置的目录 |
| `BACKEND_LOG_FILE` | `<data>/sample_logs.jsonl` | 检索的日志文件 |

延迟等用户设置持久化在 `BACKEND_DATA_DIR/settings.json`，运行时应答延迟写在
`runtime_config.json` 并由后端每次请求重新读取。

## 测试

```bash
python3 -m pytest            # 26 项单元/集成测试（含真实子进程）
python3 scripts/e2e_check.py # 真实 uvicorn + HTTP 端到端检查
python3 scripts/stress_interleave.py --waves 25 --concurrency 16
```

`scripts/stress_interleave.py` 用 1 秒空闲窗口和随机就绪预算制造回收/唤醒交错，
以 10ms 粒度采样 `/proc`，断言存活的 `app.backend` 子进程数恒 ≤ 1。

## 已知取舍

- 就绪等待上限只约束"拿到可用后端"，不包含检索本身的耗时；应答延迟很大时查询
  可能久于该上限返回。
- 回收使用 SIGTERM，3 秒后仍未退出则 SIGKILL；这段时间里新查询会计入
  `exit_waiters` 并计入自己的等待预算。
- 设置里的启动延迟上限为 4 秒（低于 5 秒硬上限），避免配置出必然失败的值。
- 样例日志与检索均为行级子串匹配，未做分词或索引；数据量按样例规模设计。

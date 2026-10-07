# Poly_data_to_everyone

**PMXT 原版 feed-only → WAL → Parquet/ZSTD → Hugging Face。**

为低配置 Debian/Ubuntu VPS 制作的采集包装项目。保留 PMXT 全市场发现、双 WebSocket 冗余与全部四类事件；不修改、不复制发布 PMXT 源码。上游固定版本：`cb0f6631556bf460d03594fe20f9bbd020b47d19`。

> **1 GiB RAM 是实验性配置，不保证全市场高峰期不丢数据。** 不启动 ClickHouse、R2 exporter、Dozzle。采集范围不裁剪；盘口校验采用有预算的完整盘口缓存，未校验部分明确计数。阅读 [可靠性边界](docs/RELIABILITY.md)。

## 一键部署

在 Debian 13 x86_64 服务器以 root 执行：

```bash
apt-get update && apt-get install -y git ca-certificates
git clone https://github.com/zer946/Poly_data_to_everyone.git
cd Poly_data_to_everyone
bash deploy.sh
```

脚本检查资源、安装缺失的 Docker、拉取固定版本 PMXT、逐个构建镜像、验证 HF 写入权限并启动服务。交互输入 **HF Token**，输入不可见，不进入命令历史。

默认目标：`你的HF用户名/polymarket-l2`。

**推荐先在 HF 网页创建该 Dataset，再生成 Fine-grained Token，只授权这个仓库 Read + Write。** 仓库级 Token 不一定有创建新仓库的权限，脚本不会要求扩大到全账号 Write。使用其他仓库名：

```bash
HF_REPO_ID='你的HF用户名/已有Dataset名字' bash deploy.sh
```

新建 Dataset 默认 Public；已有仓库可见性、README 不会被覆盖。Token 保存在 `.secrets/hf_token`，权限 `0600`，通过 Docker secret 文件仅挂载给 worker；不提交到 Git、不进入镜像、不作命令参数。

首次构建要求至少 **10 GiB 空闲磁盘**。内存小于约 2 GiB 且 Swap 不足时，脚本在项目目录创建 **2 GiB `.build-swap`** 并加入 `/etc/fstab`，不删除原有 Swap。构建使用独立 Buildx builder 和 Rust 单任务编译，结束后清理自己的构建缓存，不清理其他项目。构建失败可重跑，保留已有数据。

## 实现功能

| 功能 | 实现及边界 |
|---|---|
| 全量采集 | 原版 PMXT feed-only；保留 `book / price_change / last_trade_price / tick_size_change`，不筛选市场、不截断深度；实际范围继承上游 discovery 规则。 |
| 先落盘 | Go 接收 Redis 原文，gzip WAL，每帧 CRC32、接收时间、本地序号，默认每秒 flush/fsync。 |
| 崩溃恢复 | 接收器文件锁；重启处理孤立 `.open`；损坏时保留有效前缀并记录缺口，坏源文件不自动删除。 |
| HF 归档 | 小批 Arrow → Parquet/ZSTD；稳定路径、幂等重试；按目标 commit 验证大小及 SHA256，不将 Git/Xet ID 当成文件 SHA256。 |
| 安全清理 | 未验证上传成功的文件不删；源 WAL 还需对应审计文件与 manifest 全部验证。磁盘紧张也不删未上传数据。 |
| 缺口审计 | 解析 PMXT 既有 JSON 日志，区分单连接 degraded 与双连接 asset_down；记录 Redis 断线、进程重启、发布丢弃与损坏 WAL。 |
| 盘口检查 | 数值/结构检查；缓存盘口 BBO 检查；诊断 REST 快照仅在状态可对齐时整本比较，不能对齐记 inconclusive。 |
| 订阅检查 | 上游订阅数量与目录数量、目录 token 与近期观测 token 对照；数量相等不能证明集合相等，冷门标的沉默不直接算缺口。 |
| 资源检查 | 接收/写入/fsync 计数、上游队列日志、磁盘余量、积压、上传失败；审计表随数据上传 HF。 |
| 人工补洞 | 输出 `needs_manual_backfill` 等证据；不自动获取第三方数据，不用新快照冒充遗漏历史。 |

```text
PMXT discovery → Redis 市场缓存/生命周期 stream
Polymarket WS → PMXT 原版 Rust feed → Redis Pub/Sub
                                         ↓
                                Go receiver → gzip WAL
                                         ↓
                              Arrow 小批转换与校验
                                         ↓
                            Parquet/ZSTD + 质量表
                                         ↓
                            HF 上传 → SHA256 验证
                                         ↓
                               本地滚动缓存清理
```

Go `tap` 仅包装上游进程并转存其日志，不修改采集逻辑。上传与实时接收分离。Redis 只在私有 Compose 网络内可见，不开放公网，也不挂载 Docker socket。

## 管理

```bash
bash manage.sh status   # 容器、资源、待上传量、错误计数
bash manage.sh logs
bash manage.sh verify   # 核验本地已登记文件的 SHA256
bash manage.sh stop     # 先停 feed，再停 receiver
bash manage.sh start
```

修改 `.env` 后用 `docker compose up -d --no-build` 应用。接收器重建期间可能出现缺口。不要用 `docker compose down -v` 清理数据。

## 数据目录

```text
raw/date=YYYY-MM-DD/hour=HH/*.parquet
upstream_logs/date=YYYY-MM-DD/hour=HH/*.parquet
health/collector_gaps/date=YYYY-MM-DD/hour=HH/*.parquet
health/integrity_checks/date=YYYY-MM-DD/hour=HH/*.parquet
metadata/date=YYYY-MM-DD/hour=HH/markets-*.json
manifests/date=YYYY-MM-DD/hour=HH/*.json
```

`payload` 保存 **PMXT Redis 消息原始 bytes**，不是原始 WebSocket 帧：上游已拆开 price_change 并去重。`local_seq` 是接收器序号，不是交易所 sequence。`receive_ns` 是接收 Redis 时的时间；交易所时间另存 `exchange_ms`。

质量表按分区追加，不不停覆写一个巨大文件。gap 按 `gap_id` 和 `details_json.revision_ns` 取最新修订。详见 [schema 与人工补洞](docs/DATA.md)。

## 低内存配置

| 参数 | 默认 | 含义 |
|---|---:|---|
| PMXT_QUEUE_SIZE | 8192 | 降低上游队列容量；不减少采集范围。压力过大上游可能退出，必须观察。 |
| WAL_ROTATE_SECONDS | 300 | 5 分钟封段；大小阈值达到也封段。 |
| WAL_SEGMENT_BYTES | 33554432 | 压缩 WAL 的软大小阈值。 |
| DISK_RESERVE_BYTES | 2147483648 | 约 2 GiB 安全余量，非硬配额。 |
| LOCAL_RETENTION_SECONDS | 21600 | 验证成功后保留约 6 小时；压力时可提前删已验证文件。 |
| CHECK_MAX_BOOKS | 64 | 同时校验的完整盘口数；不是只抓 64 个市场。 |
| CHECK_MAX_LEVELS | 20000 | 校验缓存价格档总预算；超限淘汰整本，不截断深度。 |
| PROBE_INTERVAL_SECONDS | 30 | 一次诊断 REST 请求，不保证固定时间覆盖全市场。 |
| UPLOAD_INTERVAL_SECONDS | 600 | 上传等待阈值；待上传体积较大时提前传。 |

**校验在封段后进行，有分钟级延迟；不是实时全市场逐事件验证。** 容器内存上限之和不等于稳态占用。若持续 Swap、重启、队列积压或丢弃，应升级服务器，而不是降低保存深度后声称完整。

HF 免费容量和再分发权限需自行确认。长时间运行会积累大量分片：定期查看仓库容量、文件数和提交数量。本版不自动创建月度仓库、不合并或清理 HF 历史；更换目标仓库前需创建并授权新仓库。

## 测试

```bash
python -m pip install -r requirements.txt pytest
go test -race ./...
python -m pytest -q
go build -o /tmp/polyspool ./cmd/spool
python tests/smoke.py /tmp/polyspool
bash -n deploy.sh manage.sh
```

GitHub Actions 包含单元测试、TCP 断线与 SIGKILL 恢复、Parquet/离线镜像验证和容器构建。测试不使用你的 Token、不部署到 VPS。离线测试通过不等于真实 HF 权限或全市场峰值吞吐已验证；这两项分别由部署预检和服务器运行观测确认。

上游：https://github.com/pmxt-dev/polymarket-orderbook-collector 。MIT 仅覆盖本仓库新增代码，不授予上游代码或市场数据的额外权利。

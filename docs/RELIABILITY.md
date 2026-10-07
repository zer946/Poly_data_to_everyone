# 可靠性与已知边界

## 不能保证的事

1. 没有交易所连续序号，下游无法证明每个中间事件都收到。本地 sequence 只检查本接收器开始的处理连续性。快照相同，也可能中间发生挂单后撤单。
2. Redis Pub/Sub 是 at-most-once，不是持久消息队列。接收器离线时错过的发布无法由 WAL 恢复。保持 PMXT 源码不改意味着保留这一边界；WAL 只保护已经收到的数据。
3. PMXT 双 WebSocket 冗余不是两台独立服务器。单连接 down 记 degraded；双连接 down 形成资产可能缺口。主机故障和发布失败仍能丢失数据。
4. REST /book 是当前状态，不能补历史。请求期间盘口继续变动，不能直接比较两个不同时刻状态就认定漏包。这里只在上游 hash 可对齐时比较价格/数量；无法对齐为 inconclusive，不强判失败、不注入历史。
5. 1 GiB 下不保证所有盘口同时驻留内存。默认校验缓存 64 本、共 20,000 档，淘汰整本只影响校验，不改变原始数据采集范围。
6. 上游 active binary markets、去重、字段解析规则全部继承 PMXT。下游既不是 MBO/L3，也不能统计上游未订阅或未成功发布的全部事件。

## 数据落地

Go 将 Redis payload 原样装入 CRC32 帧，gzip 低压缩级别，默认每秒 flush/fsync；突然掉电仍可能损失最后一次 fsync 后的消息。先写 .open，封闭 gzip、fsync 后原子改名 .ready。Python 仅处理封段文件。

接收器重启取得 flock 后接管旧 .open 为 .recovered。CRC/gzip 损坏时保留有效前缀、写异常记录，并保留原件待人工处理。只有上传到确定 commit 且校验大小与 SHA256 的文件允许清理，源 WAL 还要等关联质量表和 manifest 验证。

磁盘安全余量不足会记录异常、停收或触发重启，不通过删除未上传数据维持假正常。Docker restart 策略会重启退出进程，但仅 unhealthy 不会自动重启；应检查 manage.sh status。

质量检查在封段后进行，因此 gap 上传有封段、转换、上传延迟。上游日志也归档，方便独立复核。

## 状态解释

| 状态 | 意义 |
|---|---|
| state_match | 某个可对齐状态一致，不证明全区间无遗漏 |
| count_match_only | 数量一致，未验证 ID 集合 |
| suspect | 可能乱序、处理错误或丢失，需复核 |
| inconclusive | 无法对齐或预算不足 |
| known_gap | 本地序号或明确发布丢弃等证据，时间边界可能近似 |
| possible_gap | 断线、重启、资源异常，期间可能缺失 |
| transport_restored | 连接恢复，不等于历史恢复或全体快照已收到 |
| state_reanchored | 观察到缓存资产新快照；l2_backfilled 仍为 false |

未缓存资产的 BBO 计入 bbo_unchecked_unanchored，绝不记 PASS。上游日志计数是采样观察，不是每个发布操作都有确认的端到端账本。

参考：
- https://github.com/pmxt-dev/polymarket-orderbook-collector
- https://redis.io/docs/latest/develop/pubsub/
- https://huggingface.co/docs/huggingface_hub/v0.36.0/en/guides/upload
- https://huggingface.co/docs/hub/security-tokens

本项目不绕过权限、不执行交易、不索取钱包私钥。

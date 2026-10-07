# 数据与人工补洞

| 原始列 | 含义 |
|---|---|
| source_session | receiver 启动时随机 session |
| local_seq | session 内记录序号，包含控制记录，不是交易所序号 |
| receive_ns | 接收 Redis payload 的 UTC 纳秒时间，受本机时钟影响 |
| exchange_ms | PMXT payload 交易所毫秒时间；无法解析时为 null |
| event_type | 四类事件；异常及控制记录可能为 null |
| asset_id / market | 字符串，避免大整数精度损失 |
| record_type | pmxt_event、collector_control 或 upstream_log |
| payload | 保留全部原样 bytes、未知字段、尾零和异常 JSON |

按 UTC 封段开始时间分区。文件可能跨小时，查询应留边界余量并按 receive_ns/exchange_ms 过滤。回放需先拿到有效 book，再应用 price_change 绝对数量；size=0 删除档位。不同 session 的 local_seq 不能混为全局交易所顺序。

```python
import json
import pyarrow.parquet as pq
for batch in pq.ParquetFile('downloaded.parquet').iter_batches(batch_size=1000):
    for row in batch.to_pylist():
        if row['record_type'] == 'pmxt_event':
            event = json.loads(row['payload'])
```

质量表通用列：time_ns、reason、status、asset_id、gap_id、start_ns、end_ns、needs_manual_backfill、details_json。其他字段如 snapshot_recovered、l2_backfilled、revision_ns、统计计数保存在 details_json。按 gap_id 取最新 revision_ns；无 gap_id 的全局告警独立保留。

manifest 包含源 WAL SHA256、输出文件 SHA256/大小、首尾本地序号、行数、数据行数、上游 commit、是否恢复/损坏及质量文件列表。history_complete=false 表示不能证明完整，不代表每条数据都错误。

## 人工补洞（程序不代办）

1. 根据 collector_gaps 找资产、时间和原因；查看 upstream_logs，近似区间留重叠。
2. 选择确有对应区间的第三方数据集，核查来源、时区、粒度、同源风险及许可；别人也可能同期缺失。
3. 第三方数据放入独立 backfill/source=... 目录，不覆盖 raw；记录 source_repo、source_commit、原文件 checksum 和字段转换规则。
4. 用重叠区间去重，并检查首尾盘口。没有交易所 sequence 就不宣称绝对证明事件全序完整。
5. 追加人工质量修订，记录 gap_id、补入路径、reviewed_by、reviewed_at、验证方法，以及全部/部分补齐状态。

本版不自动下载或合并第三方数据，也不提供覆写原历史文件的操作。

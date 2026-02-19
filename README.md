# 币安 USDT 永续合约 aggtrade → Kafka

从币安 **全部 USDT 本位（USD-M）永续合约** WebSocket 拉取 aggtrade 数据，并异步写入 Kafka。不包含币本位（COIN-M）合约。WebSocket 使用 [python-binance](https://github.com/sammchardy/python-binance)，Kafka 使用 `aiokafka`，所有 I/O 为 asyncio。

## 环境

- Python 3.10+
- 可访问 `wss://fstream.binance.com`
- Kafka 集群（如本地 `localhost:9092`）

## 安装

```bash
pip install -r requirements.txt
```

## 配置

使用 [Dynaconf](https://www.dynaconf.com/) 管理配置，按优先级生效：

1. **settings.toml**：默认值（已纳入仓库）
2. **.secrets.toml**：本地/敏感配置（可选，已 git 忽略，格式同 settings.toml 的 `[default]`）
3. **环境变量**：如 `KAFKA_BOOTSTRAP_SERVERS`，或使用 `.env`（需 `load_dotenv=True`，已默认开启）

常用项：

| 配置项 | 说明 | 默认 |
|--------|------|------|
| `ws_reconnect_delay` | WebSocket 断线重连间隔（秒） | `5.0` |
| `kafka_bootstrap_servers` | Kafka 地址 | `localhost:9092` |
| `kafka_topic_aggtrade` | 写入的 topic | `binance-futures-aggtrade` |
| `kafka_linger_ms` | 批处理等待时间（毫秒），适当增大可提高吞吐 | `5` |
| `kafka_max_in_flight` | 最大未确认条数，超过时对上游背压 | `10000` |
| `kafka_compression_type` | 压缩类型：`lz4` / `gzip` / `snappy` / `zstd` 或空 | `lz4` |

采集启动时会从币安拉取当前所有 USDT 永续交易对并订阅其 aggtrade 流，无需配置交易对列表。全量合约下事件量较大，Kafka 侧使用「异步 send + 批处理 + 背压」避免单条 wait 成为瓶颈，详见配置表后三项。

## 运行

```bash
python main.py
```

Ctrl+C 退出。程序会持续从 WebSocket 收全部 USDT 永续的 aggtrade、序列化为 JSON 并发送到 Kafka。

## 数据格式

Kafka 中的 value 为 JSON，字段与币安 [Aggregate Trade Streams](https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams) 一致，例如：`e`, `E`, `s`, `a`, `p`, `q`, `f`, `l`, `T`, `m`, `M` 等。其中 `s` 为交易对（如 BTCUSDT）。

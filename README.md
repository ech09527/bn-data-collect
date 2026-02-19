# 币安 USDT 永续合约 aggtrade → Kafka

从币安 **USDT 本位（USD-M）永续合约** WebSocket 拉取 aggtrade 数据，并异步写入 Kafka。不包含币本位（COIN-M）合约。WebSocket 使用 [python-binance](https://github.com/sammchardy/python-binance)，Kafka 使用 `aiokafka`，所有 I/O 为 asyncio。

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
3. **环境变量**：如 `SYMBOL`、`KAFKA_BOOTSTRAP_SERVERS`，或使用 `.env`（需 `load_dotenv=True`，已默认开启）

常用项：

| 配置项 | 说明 | 默认 |
|--------|------|------|
| `symbol` | USDT 永续交易对，小写（如 btcusdt、ethusdt） | `btcusdt` |
| `ws_reconnect_delay` | WebSocket 断线重连间隔（秒） | `5.0` |
| `kafka_bootstrap_servers` | Kafka 地址 | `localhost:9092` |
| `kafka_topic_aggtrade` | 写入的 topic | `binance-futures-aggtrade` |

## 运行

```bash
python main.py
```

Ctrl+C 退出。程序会持续从 WebSocket 收 aggtrade、序列化为 JSON 并发送到 Kafka。

## 数据格式

Kafka 中的 value 为 JSON，字段与币安 [Aggregate Trade Streams](https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams) 一致，例如：`e`, `E`, `s`, `a`, `p`, `q`, `f`, `l`, `T`, `m`, `M` 等。

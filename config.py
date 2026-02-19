"""使用 Dynaconf 管理配置，支持 settings.toml、.secrets.toml 与环境变量。"""
from dynaconf import Dynaconf

settings = Dynaconf(
    environments=True,
    settings_files=["settings.toml", ".secrets.toml"],
    envvar_prefix=False,
    load_dotenv=True,
    merge_enabled=True,
)

# 兼容原有导入：from config import SYMBOL, ...
SYMBOL = str(settings.get("symbol", "btcusdt")).lower()
WS_RECONNECT_DELAY = float(settings.get("ws_reconnect_delay", 5.0))
KAFKA_BOOTSTRAP_SERVERS = str(settings.get("kafka_bootstrap_servers", "localhost:9092"))
KAFKA_TOPIC_AGGTRADE = str(settings.get("kafka_topic_aggtrade", "binance-futures-aggtrade"))

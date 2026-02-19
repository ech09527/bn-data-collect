"""使用 Dynaconf 管理配置，支持 settings.toml、.secrets.toml 与环境变量。"""
from dynaconf import Dynaconf

settings = Dynaconf(
    environments=True,
    settings_files=["settings.toml", ".secrets.toml"],
    envvar_prefix=False,
    load_dotenv=True,
    merge_enabled=True,
)

WS_RECONNECT_DELAY = float(settings.get("ws_reconnect_delay", 5.0))
KAFKA_BOOTSTRAP_SERVERS = str(settings.get("kafka_bootstrap_servers", "localhost:9092"))
KAFKA_TOPIC_AGGTRADE = str(settings.get("kafka_topic_aggtrade", "binance-futures-aggtrade"))
KAFKA_LINGER_MS = int(settings.get("kafka_linger_ms", 5))
KAFKA_MAX_IN_FLIGHT = int(settings.get("kafka_max_in_flight", 10000))
_KAFKA_COMPRESSION = str(settings.get("kafka_compression_type", "lz4")).strip().lower()
KAFKA_COMPRESSION_TYPE = _KAFKA_COMPRESSION if _KAFKA_COMPRESSION else None

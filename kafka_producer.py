"""使用 aiokafka 异步将 aggtrade 数据写入 Kafka。"""
import json
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_AGGTRADE

logger = logging.getLogger(__name__)


async def create_producer() -> AIOKafkaProducer:
    """创建并启动异步 Kafka 生产者。"""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logger.info("Kafka 生产者已连接: %s", KAFKA_BOOTSTRAP_SERVERS)
    return producer


async def send_aggtrade(producer: AIOKafkaProducer, payload: dict) -> None:
    """将单条 aggtrade 发送到配置的 topic。"""
    await producer.send_and_wait(KAFKA_TOPIC_AGGTRADE, value=payload)


async def stop_producer(producer: Optional[AIOKafkaProducer]) -> None:
    """安全关闭生产者。"""
    if producer is not None:
        await producer.stop()
        logger.info("Kafka 生产者已关闭")

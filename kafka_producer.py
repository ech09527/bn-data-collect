"""使用 aiokafka 异步将 aggtrade 数据写入 Kafka。"""
import asyncio
import json
import logging
from collections import deque
from typing import Deque, Optional

from aiokafka import AIOKafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_COMPRESSION_TYPE,
    KAFKA_LINGER_MS,
    KAFKA_MAX_IN_FLIGHT,
    KAFKA_TOPIC_AGGTRADE,
)

logger = logging.getLogger(__name__)


async def create_producer() -> AIOKafkaProducer:
    """创建并启动异步 Kafka 生产者（启用批处理与压缩以应对高吞吐）。"""
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=KAFKA_LINGER_MS,
        compression_type=KAFKA_COMPRESSION_TYPE or None,
    )
    await producer.start()
    logger.info(
        "Kafka 生产者已连接: %s (linger_ms=%s, compression=%s)",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_LINGER_MS,
        KAFKA_COMPRESSION_TYPE or "none",
    )
    return producer


async def send_aggtrade(
    producer: AIOKafkaProducer,
    payload: dict,
    pending_futures: Optional[Deque[asyncio.Future]] = None,
) -> None:
    """
    将单条 aggtrade 发送到配置的 topic。
    使用 send() 不等待 ack，由 producer 内部批处理以提高吞吐。
    若传入 pending_futures，当未确认条数达到 kafka_max_in_flight 时会等待最旧的一笔完成，
    对上游施加背压，避免内存与 Kafka 侧堆积。
    """
    if pending_futures is not None:
        while len(pending_futures) >= KAFKA_MAX_IN_FLIGHT:
            oldest = pending_futures.popleft()
            try:
                await oldest
            except Exception as e:
                logger.warning("Kafka 发送结果异常: %s", e)

    future = await producer.send(KAFKA_TOPIC_AGGTRADE, value=payload)
    if pending_futures is not None:
        pending_futures.append(future)


async def stop_producer(
    producer: Optional[AIOKafkaProducer],
    pending_futures: Optional[Deque[asyncio.Future]] = None,
) -> None:
    """安全关闭生产者；若提供 pending_futures 会先等待所有未确认发送完成。"""
    if producer is None:
        return
    if pending_futures:
        while pending_futures:
            try:
                await pending_futures.popleft()
            except Exception as e:
                logger.warning("等待未完成发送时异常: %s", e)
    await producer.stop()
    logger.info("Kafka 生产者已关闭")

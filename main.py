"""
从币安永续合约 WebSocket 拉取 aggtrade 并异步写入 Kafka。
所有 I/O 均为 asyncio（WebSocket + Kafka）。
"""
import asyncio
import logging
import sys

from aiokafka import AIOKafkaProducer

from binance_ws import stream_aggtrade
from config import KAFKA_TOPIC_AGGTRADE, SYMBOL
from kafka_producer import create_producer, send_aggtrade, stop_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def run() -> None:
    producer: AIOKafkaProducer | None = None
    try:
        producer = await create_producer()
        logger.info(
            "开始消费 %s aggtrade 并写入 Kafka topic=%s", SYMBOL, KAFKA_TOPIC_AGGTRADE
        )
        count = 0
        async for payload in stream_aggtrade():
            await send_aggtrade(producer, payload)
            count += 1
            if count % 100 == 0:
                logger.info("已推送 %d 条 aggtrade", count)
    except asyncio.CancelledError:
        logger.info("任务被取消")
    finally:
        await stop_producer(producer)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，退出")


if __name__ == "__main__":
    main()

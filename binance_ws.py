"""从币安 USDT 本位永续合约 WebSocket 异步接收 aggtrade 数据（使用 python-binance）。"""
import asyncio
import logging
from typing import AsyncIterator

from binance import AsyncClient, BinanceSocketManager
from binance.enums import FuturesType

from config import SYMBOL, WS_RECONNECT_DELAY

logger = logging.getLogger(__name__)

# 仅监听 USDT 本位（USD-M）永续，不包含币本位（COIN-M）
FUTURES_TYPE = FuturesType.USD_M


async def stream_aggtrade() -> AsyncIterator[dict]:
    """
    使用 python-binance 连接 USDT 本位永续 aggtrade 流，持续 yield 消息。
    断线时自动重连（先依赖库内重连，异常时外层重连）。
    """
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    try:
        while True:
            try:
                async with bsm.aggtrade_futures_socket(
                    SYMBOL, futures_type=FUTURES_TYPE
                ) as socket:
                    logger.info("WebSocket 已连接: USDT 永续 %s aggtrade", SYMBOL)
                    while True:
                        msg = await socket.recv()
                        if isinstance(msg, dict) and msg.get("e") == "error":
                            logger.warning("WebSocket 错误: %s", msg)
                            continue
                        yield msg
            except Exception as e:
                logger.exception(
                    "WebSocket 异常，%s 秒后重连: %s", WS_RECONNECT_DELAY, e
                )
                await asyncio.sleep(WS_RECONNECT_DELAY)
    finally:
        await client.close_connection()

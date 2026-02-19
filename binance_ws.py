"""从币安 USDT 本位永续合约 WebSocket 异步接收 aggtrade 数据（使用 python-binance）。"""
import asyncio
import logging
from typing import AsyncIterator

from binance import AsyncClient, BinanceSocketManager
from binance.enums import FuturesType

from config import WS_RECONNECT_DELAY

logger = logging.getLogger(__name__)

# 仅监听 USDT 本位（USD-M）永续，不包含币本位（COIN-M）
FUTURES_TYPE = FuturesType.USD_M

# 单连接最大 stream 数（避免 URL 过长或超出交易所限制）
MAX_STREAMS_PER_CONNECTION = 400


async def get_usdt_perpetual_symbols(client: AsyncClient) -> list[str]:
    """从币安获取当前所有 USDT 本位永续、且状态为 TRADING 的交易对（大写）。"""
    info = await client.futures_exchange_info()
    symbols = [
        s["symbol"]
        for s in info["symbols"]
        if s["symbol"].endswith("USDT") and s.get("status") == "TRADING"
    ]
    return symbols


def build_aggtrade_stream_names(symbols: list[str]) -> list[str]:
    """构建 aggtrade 的 combined stream 名称列表（小写）。"""
    return [f"{s.lower()}@aggTrade" for s in symbols]


async def stream_aggtrade() -> AsyncIterator[dict]:
    """
    订阅全部 USDT 本位永续的 aggtrade 流（combined stream），持续 yield 消息。
    断线时自动重连；重连前会重新拉取交易对列表以包含新上合约。
    """
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    try:
        while True:
            try:
                symbols = await get_usdt_perpetual_symbols(client)
                stream_names = build_aggtrade_stream_names(symbols)
                logger.info(
                    "已拉取 USDT 永续交易对: %d 个，建立 combined aggtrade 连接",
                    len(symbols),
                )

                # 若数量过多则拆成多连接，避免 URL 过长
                if len(stream_names) <= MAX_STREAMS_PER_CONNECTION:
                    chunks = [stream_names]
                else:
                    chunks = [
                        stream_names[i : i + MAX_STREAMS_PER_CONNECTION]
                        for i in range(0, len(stream_names), MAX_STREAMS_PER_CONNECTION)
                    ]
                    logger.info("stream 数量超过 %d，拆成 %d 个连接", MAX_STREAMS_PER_CONNECTION, len(chunks))

                if len(chunks) == 1:
                    async with bsm.futures_multiplex_socket(
                        chunks[0], futures_type=FUTURES_TYPE
                    ) as socket:
                        while True:
                            msg = await socket.recv()
                            payload = _unwrap_multiplex_message(msg)
                            if payload is None:
                                continue
                            yield payload
                else:
                    queue: asyncio.Queue[dict | None] = asyncio.Queue()
                    stop = asyncio.Event()

                    async def consume_chunk(stream_list: list[str]) -> None:
                        try:
                            async with bsm.futures_multiplex_socket(
                                stream_list, futures_type=FUTURES_TYPE
                            ) as socket:
                                while not stop.is_set():
                                    msg = await socket.recv()
                                    payload = _unwrap_multiplex_message(msg)
                                    if payload is not None:
                                        await queue.put(payload)
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.exception("multiplex 连接异常: %s", e)
                            await queue.put(None)  # 通知主循环有连接掉线

                    tasks = [asyncio.create_task(consume_chunk(c)) for c in chunks]
                    try:
                        while True:
                            payload = await queue.get()
                            if payload is None:
                                break  # 有连接掉线，外层重连
                            yield payload
                    finally:
                        stop.set()
                        for t in tasks:
                            t.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)

            except Exception as e:
                logger.exception(
                    "WebSocket 异常，%s 秒后重连: %s", WS_RECONNECT_DELAY, e
                )
                await asyncio.sleep(WS_RECONNECT_DELAY)
    finally:
        await client.close_connection()


def _unwrap_multiplex_message(msg: dict) -> dict | None:
    """
    Combined stream 消息格式: {"stream": "<streamName>", "data": <rawPayload>}。
    返回 data 部分；若为错误或无效则返回 None（调用方跳过）。
    """
    if not isinstance(msg, dict):
        return None
    # 错误帧可能在最外层
    if msg.get("e") == "error":
        logger.warning("WebSocket 错误: %s", msg)
        return None
    data = msg.get("data")
    if not isinstance(data, dict):
        return None
    if data.get("e") == "error":
        logger.warning("WebSocket 错误(data): %s", data)
        return None
    return data

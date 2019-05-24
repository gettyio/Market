# -*— coding:utf-8 -*-

"""
OKEx 现货行情
https://www.okex.com/docs/zh

Author: HuangTao
Date:   2018/05/21
"""

import zlib
import json
import copy

from quant.utils import tools
from quant.utils import logger
from quant.config import config
from quant.const import OKEX
# from quant.event.ticker import EventTicker
# from quant.event.trade import EventTrade
from quant.event import EventOrderbook
from quant.utils.websocket import Websocket


class OKEx(Websocket):
    """ OKEx 现货行情
    """

    def __init__(self):
        self._platform = OKEX

        self._wss = config.platforms.get(self._platform).get("wss", "wss://real.okex.com:10442")
        self._symbols = list(set(config.platforms.get(self._platform).get("symbols")))
        self._channels = config.platforms.get(self._platform).get("channels")

        self._orderbooks = {}  # 订单薄数据 {"symbol": {"bids": {"price": quantity, ...}, "asks": {...}}}
        self._length = 20  # 订单薄数据推送长度

        url = self._wss + "/ws/v3"
        super(OKEx, self).__init__(url)
        self.heartbeat_msg = "ping"

    async def connected_callback(self):
        """ 建立连接之后，订阅事件 ticker
        """
        for ch in self._channels:
            if ch == "orderbook":  # 订阅orderbook行情
                chs = []
                for symbol in self._symbols:
                    ch = "spot/depth:{s}".format(s=symbol.replace("/", '-'))
                    chs.append(ch)
                msg = {
                    "op": "subscribe",
                    "args": chs
                }
                await self.ws.send_json(msg)
                logger.info("subscribe orderbook success.", caller=self)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def process_binary(self, raw):
        """ 处理websocket上接收到的消息
        @param raw 原始的压缩数据
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":  # 心跳返回
            return
        msg = json.loads(msg)
        # logger.debug("msg:", msg, caller=self)

        table = msg.get("table")
        if table == "spot/depth":  # 订单薄
            if msg.get("action") == "partial":  # 首次返回全量数据
                for d in msg["data"]:
                    await self.deal_orderbook_partial(d)
            elif msg.get("action") == "update":  # 返回增量数据
                for d in msg["data"]:
                    await self.deal_orderbook_update(d)
            else:
                logger.warn("unhandle msg:", msg, caller=self)
        else:
            logger.warn("unhandle msg:", msg, caller=self)

    async def deal_orderbook_partial(self, data):
        """ 处理全量数据
        """
        symbol = data.get("instrument_id").replace("-", "/")
        if symbol not in self._symbols:
            return
        asks = data.get("asks")
        bids = data.get("bids")
        self._orderbooks[symbol] = {"asks": {}, "bids": {}, "timestamp": 0}
        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])
            self._orderbooks[symbol]["asks"][price] = quantity
        for bid in bids:
            price = float(bid[0])
            quantity = float(bid[1])
            self._orderbooks[symbol]["bids"][price] = quantity
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))
        self._orderbooks[symbol]["timestamp"] = timestamp

    async def deal_orderbook_update(self, data):
        """ 处理orderbook增量数据
        """
        symbol = data.get("instrument_id").replace("-", "/")
        asks = data.get("asks")
        bids = data.get("bids")
        timestamp = tools.utctime_str_to_mts(data.get("timestamp"))

        if symbol not in self._orderbooks:
            return
        self._orderbooks[symbol]["timestamp"] = timestamp

        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])
            if quantity == 0 and price in self._orderbooks[symbol]["asks"]:
                self._orderbooks[symbol]["asks"].pop(price)
            else:
                self._orderbooks[symbol]["asks"][price] = quantity

        for bid in bids:
            price = float(bid[0])
            quantity = float(bid[1])
            if quantity == 0 and price in self._orderbooks[symbol]["bids"]:
                self._orderbooks[symbol]["bids"].pop(price)
            else:
                self._orderbooks[symbol]["bids"][price] = quantity

        await self.publish_orderbook()

    async def publish_orderbook(self, *args, **kwargs):
        """ 推送orderbook数据
        """
        for symbol, data in self._orderbooks.items():
            ob = copy.copy(data)
            if not ob["asks"] or not ob["bids"]:
                logger.warn("symbol:", symbol, "asks:", ob["asks"], "bids:", ob["bids"], caller=self)
                continue

            ask_keys = sorted(list(ob["asks"].keys()))
            bid_keys = sorted(list(ob["bids"].keys()), reverse=True)
            if ask_keys[0] <= bid_keys[0]:
                logger.warn("symbol:", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
                continue

            # 卖
            asks = []
            for k in ask_keys[:self._length]:
                price = "%.8f" % k
                quantity = "%.8f" % ob["asks"].get(k)
                asks.append([price, quantity])

            # 买
            bids = []
            for k in bid_keys[:self._length]:
                price = "%.8f" % k
                quantity = "%.8f" % ob["bids"].get(k)
                bids.append([price, quantity])

            # 推送订单薄数据
            orderbook = {
                "platform": self._platform,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": ob["timestamp"]
            }
            EventOrderbook(**orderbook).publish()
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

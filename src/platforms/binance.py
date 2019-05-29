# -*— coding:utf-8 -*-

"""
Binance 行情数据
https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md

Author: HuangTao
Date:   2018/07/04
"""

from quant.utils import tools
from quant.utils import logger
from quant.config import config
from quant.const import BINANCE
from quant.utils.websocket import Websocket
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.event import EventTrade, EventKline, EventOrderbook


class Binance(Websocket):
    """ Binance 行情数据
    """

    def __init__(self):
        self._platform = BINANCE
        self._url = config.platforms.get(self._platform).get("wss", "wss://stream.binance.com:9443")
        self._symbols = list(set(config.platforms.get(self._platform).get("symbols")))
        self._channels = config.platforms.get(self._platform).get("channels")

        self._c_to_s = {}  # {"channel": "symbol"}
        self._tickers = {}  # 最新行情 {"symbol": price_info}

        self._make_url()
        super(Binance, self).__init__(self._url)
        self.initialize()

    def _make_url(self):
        """ 拼接请求url
        """
        cc = []
        for ch in self._channels:
            if ch == "kline":  # 订阅K线 1分钟
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "kline_1m")
                    cc.append(c)
            elif ch == "orderbook":  # 订阅订单薄 深度为5
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "depth20")
                    cc.append(c)
            elif ch == "trade":  # 订阅实时交易
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "trade")
                    cc.append(c)
            else:
                logger.error("channel error! channel:", ch, caller=self)
        self._url += "/stream?streams=" + "/".join(cc)

    async def process(self, msg):
        """ 处理websocket上接收到的消息
        """
        # logger.debug("msg:", msg, caller=self)
        if not isinstance(msg, dict):
            return

        channel = msg.get("stream")
        if channel not in self._c_to_s:
            logger.warn("unkown channel, msg:", msg, caller=self)
            return

        symbol = self._c_to_s[channel]
        data = msg.get("data")
        e = data.get("e")  # 事件名称

        # 保存数据到数据库
        if e == "kline":  # K线
            kline = {
                "platform": self._platform,
                "symbol": symbol,
                "open": data.get("k").get("o"),  # 开盘价
                "high": data.get("k").get("h"),  # 最高价
                "low": data.get("k").get("l"),  # 最低价
                "close": data.get("k").get("c"),  # 收盘价
                "volume": data.get("k").get("q"),  # 收盘价
                "timestamp": data.get("k").get("t"),  # 时间戳
            }
            EventKline(**kline).publish()
            logger.info("symbol:", symbol, "kline:", kline, caller=self)
        elif channel.endswith("depth20"):  # 订单薄
            bids = []
            asks = []
            for bid in data.get("bids"):
                bids.append(bid[:2])
            for ask in data.get("asks"):
                asks.append(ask[:2])
            orderbook = {
                "platform": BINANCE,
                "symbol": symbol,
                "asks": asks,
                "bids": bids,
                "timestamp": tools.get_cur_timestamp_ms()
            }
            EventOrderbook(**orderbook).publish()
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)
        elif e == "trade":  # 实时成交信息
            trade = {
                "platform": self._platform,
                "symbol": symbol,
                "action":  ORDER_ACTION_SELL if data["m"] else ORDER_ACTION_BUY,
                "price": data.get("p"),
                "quantity": data.get("q"),
                "timestamp": data.get("T")
            }
            EventTrade(**trade).publish()
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
        else:
            logger.error("event error! msg:", msg, caller=self)

    def _symbol_to_channel(self, symbol, channel_type="ticker"):
        """ symbol转换到channel
        @param symbol symbol名字
        @param channel_type 频道类型 kline K线 / ticker 行情
        """
        channel = "{x}@{y}".format(x=symbol.replace("/", "").lower(), y=channel_type)
        self._c_to_s[channel] = symbol
        return channel

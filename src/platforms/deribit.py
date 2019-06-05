# -*— coding:utf-8 -*-

"""
deribit外盘行情
https://www.deribit.com/main#/pages/docs/api
https://www.deribit.com/api/v1/public/getinstruments

Author: HuangTao
Date:   2018/10/08
"""

import base64
import hashlib

from quant.utils import tools
from quant.utils import logger
from quant.config import config
from quant.const import DERIBIT
from quant.event import EventOrderbook
from quant.utils.websocket import Websocket


class Deribit(Websocket):
    """ deribit外盘行情
    """

    def __init__(self):
        self._platform = DERIBIT
        self._url = config.platforms.get(self._platform).get("wss")
        self._symbols = list(set(config.platforms.get(self._platform).get("symbols")))
        self._access_key = config.platforms.get(self._platform).get("access_key")
        self._secret_key = config.platforms.get(self._platform).get("secret_key")
        self._last_msg_ts = tools.get_cur_timestamp() # 上次接收到消息的时间戳

        super(Deribit, self).__init__(self._url)
        self.heartbeat_msg = {"action": "/api/v1/public/ping"}
        self.initialize()

    async def connected_callback(self):
        """ 建立连接之后，订阅事件
        """
        nonce = tools.get_cur_timestamp_ms()
        uri = "/api/v1/private/subscribe"
        params = {
            "instrument": self._symbols,
            "event": ["order_book"]
        }
        sign = self.deribit_signature(nonce, uri, params, self._access_key, self._secret_key)
        data = {
            "id": "huangtao",
            "action": uri,
            "arguments": params,
            "sig": sign
        }
        await self.ws.send_json(data)
        logger.info("subscribe orderbook success.", caller=self)

    async def process(self, msg):
        """ 处理websocket上接收到的消息
        """
        # logger.debug("msg:", msg, caller=self)
        if tools.get_cur_timestamp() <= self._last_msg_ts:
            return
        if not isinstance(msg, dict):
            return
        notifications = msg.get("notifications")
        if not notifications:
            return
        message = notifications[0].get("message")
        if message != "order_book_event":
            return

        symbol = notifications[0].get("result").get("instrument")
        bids = []
        for item in notifications[0].get("result").get("bids")[:10]:
            b = [item.get("price"), item.get("quantity")]
            bids.append(b)
        asks = []
        for item in notifications[0].get("result").get("asks")[:10]:
            a = [item.get("price"), item.get("quantity")]
            asks.append(a)
        self._last_msg_ts = tools.get_cur_timestamp()
        orderbook = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": self._last_msg_ts
        }
        EventOrderbook(**orderbook).publish()
        logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    def deribit_signature(self, nonce, uri, params, access_key, access_secret):
        """ 生成signature
        """
        sign = "_=%s&_ackey=%s&_acsec=%s&_action=%s" % (nonce, access_key, access_secret, uri)
        for key in sorted(params.keys()):
            sign += "&" + key + "=" + "".join(params[key])
        return "%s.%s.%s" % (access_key, nonce, base64.b64encode(hashlib.sha256(sign.encode()).digest()).decode())

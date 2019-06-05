# -*- coding:utf-8 -*-

"""
行情服务

Author: HuangTao
Date:   2018/05/04
"""

import sys

from quant.quant import quant
from quant.config import config
from quant.const import OKEX, OKEX_FUTURE, BINANCE, DERIBIT


def initialize():
    """ 初始化
    """

    for platform in config.platforms:
        if platform == OKEX:
            from platforms.okex import OKEx as Market
        elif platform == OKEX_FUTURE:
            from platforms.okex_ftu import OKExFuture as Market
        elif platform == BINANCE:
            from platforms.binance import Binance as Market
        elif platform == DERIBIT:
            from platforms.deribit import Deribit as Market
        else:
            from quant.utils import logger
            logger.error("platform error! platform:", platform)
            continue
        Market()


def main():
    config_file = sys.argv[1]  # 配置文件 config.json
    quant.initialize(config_file)
    initialize()
    quant.start()


if __name__ == "__main__":
    main()

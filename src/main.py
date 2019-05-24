# -*- coding:utf-8 -*-

"""
行情服务

Author: HuangTao
Date:   2018/05/04
"""

import sys

from quant.quant import quant
from quant.config import config
from quant.const import OKEX, OKEX_FUTURE


def initialize():
    """ 初始化
    """

    for platform in config.platforms:
        if platform == OKEX:
            from platforms.okex import OKEx
            okex = OKEx()
            okex.initialize()
        elif platform == OKEX_FUTURE:
            from platforms.okex_ftu import OKExFuture
            OKExFuture().initialize()
        else:
            from quant.utils import logger
            logger.warn("platform error! platform:", platform)


def main():
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = "config.json"
    quant.initialize(config_file)
    initialize()
    quant.start()


if __name__ == "__main__":
    main()

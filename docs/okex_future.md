
## OKEx Future行情

OKEx Future的行情数据根据 [OKEx Future官方文档](https://www.okex.com/docs/zh) 提供的方式，
通过websocket协议，订阅OKEx Future官方实时推送的行情数据。然后程序将源数据经过适当打包处理，并通过行情事件的形式发布到事件中心。

当前行情服务器能够收集OKEx Future的行情数据包括：Orderbook(订单薄)。

##### 1. 服务配置

配置文件需要指定一些基础参数来告诉行情服务器应该做什么，一个完整的配置文件大致如下:

```json
{
    "LOG": {
        "console": true,
        "level": "DEBUG",
        "path": "/data/logs/servers/Market",
        "name": "market.log",
        "clear": true,
        "backup_count": 5
    },
    "RABBITMQ": {
        "host": "127.0.0.1",
        "port": 5672,
        "username": "test",
        "password": "213456"
    },
    "PROXY": "http://127.0.0.1:1087",

    "PLATFORMS": {
        "okex_future": {
            "symbols": [
                "BTC-USD-190524"
            ],
            "channels": [
                "orderbook"
            ]
        }
    }
}
```
以上配置表示：订阅 `okex_future` 交易所里，交易对 `BTC/USDT` 和 `LTC/USDT` 的 `orderbook 订单薄` 行情数据。

> 配置文件可以参考 [配置文件说明](https://github.com/TheNextQuant/thenextquant/blob/master/docs/configure/README.md)。
> 此处对 `PLATFORMS` 下的关键配置做一下说明:
- PLATFORMS `dict` 需要配置的交易平台，key为交易平台名称，value为对应的行情配置
- okex_future `dict` 交易平台行情配置
- symbols `list` 需要订阅行情数据的交易对，注意此处配置的交易对都需要大写字母，交易对之间包含斜杠
- channels `list` 需要订阅的行情类型，其中： orderbook 订单薄


> 其它：
- [orderbook 数据结构](https://github.com/TheNextQuant/thenextquant/blob/master/docs/market.md#21-%E8%AE%A2%E5%8D%95%E8%96%84orderbook)


## 行情服务
行情服务根据各个交易所当前提供的不同方式，通过REST API或Websocket方式实现了对各大交易所平台实时行情数据的获取及推送。


#### 安装
需要安装 `thenextquant` 量化交易框架，使用 `pip` 可以简单方便安装:
```text
pip install thenextquant
```

#### 运行
```text
git clone https://github.com/TheNextQuant/Market.git  # 下载项目
cd Market  # 进入项目目录
vim config.json  # 编辑配置文件

python src/main.py config.json  # 启动之前请修改配置文件
```
> 配置请参考 [配置文件说明](https://github.com/TheNextQuant/thenextquant/blob/master/docs/configure/README.md)。


#### 各大交易所行情

- [Binance](docs/binance.md)
- [OKEx](docs/okex.md)
- [OKEx Future](docs/okex_future.md)

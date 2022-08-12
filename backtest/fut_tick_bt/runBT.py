from wtpy import WtBtEngine,EngineType
from wtpy.apps import WtBtAnalyst

from Strategies.MyTickStra import MyTickStra


if __name__ == "__main__":
    #创建一个运行环境，并加入策略
    '''
    WtBtEngine构造函数
    @eType  引擎类型
    @logCfg 日志模块配置文件，也可以直接是配置内容字符串
    @isFile 是否文件，如果是文件，则将logCfg当做文件路径处理，如果不是文件，则直接当成json格式的字符串进行解析
    @bDumpCfg   回测的实际配置文件是否落地
    @outDir 回测数据输出目录
    '''

    engine = WtBtEngine(EngineType.ET_CTA, bDumpCfg=True)

    '''
    cfgfile用于基础信息配置
    回测引擎在init时会首先读取cfgfile文件的配置信息，然后再根据init的其他参数更新配置信息
    cfgfile需手动指定路径和文件名，不会到folder目录下查找

    folder目录下必须有contractfile和sessionfile参数指定的文件
    '''
    engine.init(folder='../../common/', cfgfile="configbt.json", commfile="fut_comms.json", contractfile="fut_contracts.json", sessionfile='sessions.json')
    # 配置回测起止时间
    engine.configBacktest(202204180930,202204181000)

    engine.configBTStorage(mode="csv", path="/mnt/data/wtdata/storage_new")
    # 向wt内核提交配置文件，如果回测引擎设置bDumpCfg为True，则会在本地也生成配置文件
    engine.commitBTConfig()

    # 创建策略，注意：日线的时期需要写成d1，code需要符合wt的格式
    straInfo = MyTickStra('tst', 'CFE.IF.2209', 'CFE.IF.2212')

    # 挂载策略
    engine.set_cta_strategy(straInfo)

    engine.run_backtest()

    # 绩效分析
    analyst = WtBtAnalyst()
    analyst.add_strategy("tst", folder="./outputs_bt/", init_capital=5000000, rf=0.02, annual_trading_days=240)
    analyst.run()

    # kw = input('press any key to exit\n')
    engine.release_backtest()
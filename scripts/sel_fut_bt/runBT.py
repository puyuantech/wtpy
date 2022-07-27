from wtpy import WtBtEngine,EngineType

from Strategies.DualThrust_Sel import StraDualThrustSel

# from Strategies.XIM import XIM

if __name__ == "__main__":
    #创建一个运行环境，并加入策略
    engine = WtBtEngine(EngineType.ET_SEL, bDumpCfg=True)
    engine.init('', "configbt.json")
    engine.commitBTConfig()

    straInfo = StraDualThrustSel(name='DT_COMM_SEL', codes=["SZSE.STK.000002", "SZSE.STK.000005"], barCnt=50, period="d1", days=30, k1=0.1, k2=0.1)
    engine.set_sel_strategy(straInfo, time=1, period="d")

    engine.run_backtest()


    engine.release_backtest()
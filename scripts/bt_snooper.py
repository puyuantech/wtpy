from wtpy.monitor import WtBtSnooper
from wtpy import WtDtServo

# BtSnooper是一个回测数据可视化的网页
def testBtSnooper():    

    dtServo = WtDtServo()
    dtServo.setBasefiles(folder="../common", commfile='stk_commons.json', contractfile='stk_contracts.json', sessionfile='sessions.json')
    dtServo.setStorage(path='/mnt/data/wtdata/storage')
    # bars = dtServo.get_bars("SZSE.STK.000005", "m1", fromTime=202109100930, endTime=202110101500).to_df()
    # bars.to_csv("SZSE.STK.000005_m1.csv")
    snooper = WtBtSnooper(dtServo)
    snooper.run_as_server(host='0.0.0.0', port=8099)

testBtSnooper()
# 运行了服务以后，在浏览器打开以下网址即可使用
# http://127.0.0.1:8099/backtest/backtest.html

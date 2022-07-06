
from wtpy import BaseCtaStrategy
from wtpy import CtaContext

class MyStraFut(BaseCtaStrategy):
    
    def __init__(self, name:str, code:str, barCnt:int, period:str, days:int, k1:float, k2:float):
        BaseCtaStrategy.__init__(self, name)

        self.__days__ = days
        self.__k1__ = k1
        self.__k2__ = k2

        self.__period__ = period
        self.__bar_cnt__ = barCnt
        self.__code__ = code

    def on_init(self, context:CtaContext):
        code = self.__code__    #品种代码

        context.stra_get_bars(code, self.__period__, self.__bar_cnt__, isMain = True)
        context.stra_log_text("DualThrust inited")

    
    def on_calculate(self, context:CtaContext):
        code = self.__code__    #品种代码

        #读取当前仓位
        curPos = context.stra_get_position(code)

        if curPos == 0:
            context.stra_enter_long(code, 1, 'enterlong')
            context.stra_log_text("***********%.2f" % (curPos))
        elif curPos > 0:
            context.stra_exit_long(code, curPos, 'exitlong')
            context.stra_log_text("-----------%.2f" % (curPos))


    def on_tick(self, context:CtaContext, stdCode:str, newTick:dict):
        #context.stra_log_text ("on tick fired")
        return
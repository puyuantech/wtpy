from wtpy import BaseCtaStrategy
from wtpy import CtaContext

class MyTickStra(BaseCtaStrategy):
    def __init__(self, name: str, code1: str, code2: str, a: int=0, b:int = 1):
        super().__init__(name)
        self.code1 = code1
        self.code2 = code2
        self.a = a
        self.b = b
        self.block_z2c = False
        self.block_f2c = False
        self.code1_last_tick = None
        self.code2_last_tick = None

    def on_init(self, context: CtaContext):
        
        # context.stra_get_ticks(self.code1, 100)
        # context.stra_get_ticks(self.code2, 100)
        context.stra_sub_ticks(self.code1)
        context.stra_sub_ticks(self.code2)
        context.stra_log_text("on init finished")

    def on_tick(self, context: CtaContext, stdCode: str, newTick: dict):
        if stdCode == self.code1:
            self.code1_last_tick = newTick
        elif stdCode == self.code2:
            self.code2_last_tick = newTick

        if self.code1_last_tick is None or self.code2_last_tick is None:
            return

        context.stra_log_text(f"[BEGIN ONE ON_TICK]{newTick['code']}: date is {newTick['action_date']}, time is {newTick['action_time']}")
        
        z = self.code1_last_tick['price'] - self.code2_last_tick['price'] - 21

        var = 1

        context.stra_log_text(f"z=code1_price({self.code1_last_tick['price']})-code2_price({self.code2_last_tick['price']})={z}, var    ={var}")

        hold1 = context.stra_get_position(self.code1)

        hold2 = context.stra_get_position(self.code2)

        context.stra_log_text(f'current code1 hold is {hold1}, code2 hold is {hold2}')

        if hold1 == 0:        
            # code1比code2值钱时，z > 0，价差过大，认为code1会贬值，code2会升值
       
            if z > var and self.block_z2c == False:
                context.stra_log_text("[trade] open [0,-1] while code1 hold is 0, enter short code1, enter long code2")
                context.stra_enter_short(self.code1, 1)
                context.stra_enter_long(self.code2, 1)

            elif z < var and z > 0:
                context.stra_log_text("restore the block of +2c")
                self.block_z2c == False

            # code2比code1值钱时，z < 0，价差过大，认为code2会贬值，code1会升值
            elif z < - var and self.block_f2c == False:
                context.stra_log_text("[trade] open [0,1]while code1 hold is 0, enter long code1, enter short code2")
                context.stra_enter_long(self.code1, 1)
                context.stra_enter_short(self.code2, 1)

            elif z > -var and z < 0:
                context.stra_log_text("restore the block of -2c")
                self.block_f2c = False
        
        elif hold1 == -1:
            if z <= 0:
                context.stra_log_text("[trade] win [-1,0]while code1 hold is -1 & z <= 0, stop surplus")
                # context.stra_set_position(self.code1, 0)
                # context.stra_set_position(self.code2, 0)
                context.stra_exit_short(self.code1, 1)
                context.stra_exit_long(self.code2, 1)
            elif z > 2 * var:
                context.stra_log_text("[trade] lose [-1,0]while code1 hold is -1 & z > 2 * var, stop loss, set block_z2c = True")
                # context.stra_set_position(self.code1, 0)
                # context.stra_set_position(self.code2, 0)
                context.stra_exit_short(self.code1, 1)
                context.stra_exit_long(self.code2, 1)
                self.block_z2c = True

        elif hold1 == 1:
            if z >= 0:
                context.stra_log_text("[trade] win [1,0]while code1 hold is 1 & z >= 0, stop surplus")
                # context.stra_set_position(self.code1, 0)
                # context.stra_set_position(self.code2, 0)
                context.stra_exit_long(self.code1, 1)
                context.stra_exit_short(self.code2, 1)
            elif z < -2 * var:
                context.stra_log_text("[trade] lose [1,0] while code1 hold is 1 & z < -2 * var, stop loss")
                # context.stra_set_position(self.code1, 0)
                # context.stra_set_position(self.code2, 0)
                context.stra_exit_long(self.code1, 1)
                context.stra_exit_short(self.code2, 1)
                self.block_z2c = True

    def on_bar(self, context: CtaContext, stdCode: str, period: str, newBar: dict):
        return
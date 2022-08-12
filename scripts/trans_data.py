import sys
import os
import shutil
from qdb import QdbApi
import datetime as dt
import pandas as pd
from enum import Enum
from typing import Optional, List, Union
import traceback

from wtpy.wrapper import WtDataHelper
from wtpy.WtCoreDefs import WTSTickStruct

# export QDB_CONFIG_PATH=~/.qdb/config.yml


class SecType(Enum):
    STK_TICK = 0
    STK_MIN = 1
    STK_DAILY = 2
    FUT_TICK = 3
    FUT_MIN = 4
    FUT_DAILY = 5


class DataLoaderFromQdb():

    def __init__(self) -> None:
        self.__cli = QdbApi()
        self.__sec_id = ''      # 证券代码
        self.__sec_type = None  # 证券类型
        self.__suffix = ''      # 存储证券时的文件名后缀，例如m1/m5/d

    def get_data(self, sec_id: str, sec_type: SecType, start_date: Optional[dt.date]=None, end_date: Optional[dt.date]=None, fields: List[str]=None) -> Optional[pd.DataFrame]:
        '''
        获取单只证券的历史数据

        @sec_id     证券代码
        @sec_type   证券类型
        @start_date 证券历史数据的起始时间
        @end_data   证券历史数据的结束时间
        @fields     证券历史数据的可选字段
        '''
        self.__sec_id = sec_id

        if sec_type == SecType.STK_TICK:
            self.__sec_type = SecType.STK_TICK
            df = self.__cli.get_stk_tick(sec_id, start_date, end_date, fields)

        elif sec_type == SecType.STK_MIN:
            self.__suffix = 'm1'
            self.__sec_type = SecType.STK_MIN
            df = self.__cli.get_stk_min(sec_id, start_date, end_date, fields)

        elif sec_type == SecType.STK_DAILY:
            self.__suffix = 'd'
            self.__sec_type = SecType.STK_DAILY
            df =  self.__cli.get_stk_daily(sec_id, start_date, end_date, fields)

        elif sec_type == SecType.FUT_TICK:
            self.__sec_type = SecType.FUT_TICK
            df =  self.__cli.get_fut_tick(sec_id, start_date, end_date, fields)

        elif sec_type == SecType.FUT_MIN:
            self.__suffix = 'm1'
            self.__sec_type = SecType.FUT_MIN
            df =  self.__cli.get_fut_min(sec_id, start_date, end_date, fields)

        elif sec_type == SecType.FUT_DAILY:
            self.__suffix = 'd'
            self.__sec_type = SecType.FUT_DAILY
            df =  self.__cli.get_fut_daily(sec_id, start_date, end_date, fields)

        else:
            print("非法的证券类型!")
            return None

        print(f'    Stage1: 已获得{self.__sec_id}的DataFrame')

        return df

    def trans_tick(self, df: Optional[pd.DataFrame], path:str):
        '''
        转储tick数据为dsb文件到path路径
        因为wt的设计，每次只能转一天的tick

        @df     dataframe数据
        @path   存储路径
        '''
        
        if df is None or len(df) == 0:
            print("    该日无数据，return")
            return

        df = df.reset_index()
        BUFFER = WTSTickStruct*len(df)
        buffer = BUFFER()
        
        self.trans_sec_id()
        exchg = bytes(self.__sec_id.split('.')[0], encoding='UTF8')
        code = bytes(self.__sec_id, encoding='UTF8')

        udate = df.iloc[-1]['trading_date'].strftime('%Y%m%d')
        print(f"df len is {len(df)}, trading_date is {udate}")
        
        for i in range(len(df)):
            # if i % 1000 == 1:
            #     print(f'before df[{i}], handled')

            curTick = buffer[i]

            curTick.exchg = exchg
            curTick.code = code

            df_f = df.iloc[i]

            curTick.price = df_f["last"]
            curTick.open = df_f['open']
            curTick.high = df_f["high"]
            curTick.low = df_f["low"]
            curTick.settle = 0.0
            
            curTick.total_volume = df_f["volume"]
            curTick.total_turnover = df_f["amount"]
            curTick.open_interest = 0.0

            curTick.trading_date = int(df_f["trading_date"].strftime('%Y%m%d'))
            curTick.action_date = int(df_f["datetime"].strftime('%Y%m%d'))
            curTick.action_time = int(df_f["datetime"].strftime('%H%M%S%f')[:-3])

            curTick.pre_close = df_f["prev_close"]
            curTick.pre_settle = df_f["prev_settlement"]
            curTick.pre_interest = 0.0

            curTick.bid_price_0 = df_f['b1']
            curTick.bid_qty_0 = df_f['b1_v']
            curTick.ask_price_0 = df_f['a1']
            curTick.ask_qty_0 = df_f[f'a1_v']

            curTick.bid_price_1 = df_f['b2']
            curTick.bid_qty_1 = df_f['b2_v']
            curTick.ask_price_1 = df_f['a2']
            curTick.ask_qty_1 = df_f[f'a2_v']

            curTick.bid_price_2 = df_f['b3']
            curTick.bid_qty_2 = df_f['b3_v']
            curTick.ask_price_2 = df_f['a3']
            curTick.ask_qty_2 = df_f[f'a3_v']

            curTick.bid_price_3 = df_f['b4']
            curTick.bid_qty_3 = df_f['b4_v']
            curTick.ask_price_3 = df_f['a4']
            curTick.ask_qty_3 = df_f[f'a4_v']

            curTick.bid_price_4 = df_f['b5']
            curTick.bid_qty_4 = df_f['b5_v']
            curTick.ask_price_4 = df_f['a5']
            curTick.ask_qty_4 = df_f[f'a5_v']

            
        path = os.path.join(path, 'bin', 'ticks')
        filename = os.path.join(path, self.__sec_id + '_tick_' + udate + '.dsb')
        print(filename)
        dtHelper = WtDataHelper()
        dtHelper.store_ticks(filename, buffer, len(buffer))

    def handle_stk_min(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        df = df.reset_index()

        reserved_fields = ['datetime', 'open', 'low', 'volume', 'close', 'high', 'amount']
        df = df.loc[:, reserved_fields]

        df = df.rename(columns={'datetime': '<Time>', 
                                'volume' : '<Vol>', 
                                'amount' : '<Money>',
                                'open' : '<Open>',
                                'close' : '<Close>',
                                'high' : '<High>',
                                'low' : '<Low>'})

        df['<Date>'] = df['<Time>'].astype('datetime64').dt.strftime('%Y/%m/%d')
        df['<Time>'] = df['<Time>'].map(lambda t: t.time())

        return df

    def handle_stk_daily(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        df = df.reset_index()

        reserved_fields = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount']
        df = df.loc[:, reserved_fields]

        df = df.rename(columns={'date': '<Date>', 
                                'volume' : '<Vol>', 
                                'amount' : '<Money>',
                                'open' : '<Open>',
                                'close' : '<Close>',
                                'high' : '<High>',
                                'low' : '<Low>',})

        df['<Date>'] = df['<Date>'].astype('datetime64').dt.strftime('%Y/%m/%d')

        return df

    def handle_fut_min(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        df = df.reset_index()

        reserved_fields = ['datetime', 'trading_date', 'amount', 'open', 'close', 'high', 'low', 'volume', 'hold']
        df = df.loc[:, reserved_fields]

        df = df.rename(columns={'datetime': '<Time>', 
                                'trading_date' : '<Date>', 
                                'volume' : '<Vol>', 
                                'amount' : '<Money>',
                                'open' : '<Open>',
                                'close' : '<Close>',
                                'high' : '<High>',
                                'low' : '<Low>',
                                'hold' : '<Hold>'})

        df['<Date>'] = df['<Time>'].astype('datetime64').dt.strftime('%Y/%m/%d')
        df['<Time>'] = df['<Time>'].map(lambda t: t.time())

        return df

    def handle_fut_daily(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        df = df.reset_index()

        reserved_fields = ['date', 'open', 'close', 'high', 'low', 'settle', 'volume', 'amount', 'hold']
        df = df.loc[:, reserved_fields]

        df = df.rename(columns={'date': '<Date>', 
                                'volume' : '<Vol>', 
                                'amount' : '<Money>',
                                'open' : '<Open>',
                                'close' : '<Close>',
                                'high' : '<High>',
                                'low' : '<Low>',
                                'hold' : '<Hold>',
                                'settle' : '<Settle>'})

        df['<Date>'] = df['<Date>'].astype('datetime64').dt.strftime('%Y/%m/%d')

        return df
    
    def trans_data(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        '''
        将单只证券的DataFrame格式转化为wt支持的格式

        @df 从get_data()方法获取的DataFrame
        '''
        if df is None:
            print("传入trans_data()的DataFrame为None")
            return None


        if self.__sec_type == SecType.STK_TICK:
            df = self.handle_stk_tick(df)
        
        elif self.__sec_type == SecType.STK_MIN:
            df = self.handle_stk_min(df)

        elif self.__sec_type == SecType.STK_DAILY:
            df = self.handle_stk_daily(df)

        elif self.__sec_type == SecType.FUT_TICK:
            df = self.handle_fut_tick(df)
        
        elif self.__sec_type == SecType.FUT_MIN:
            df = self.handle_fut_min(df)

        elif self.__sec_type == SecType.FUT_DAILY:
            df = self.handle_fut_daily(df)

        else:
            print("非法的证券类型!")
            return None
        
        print(f'    Stage2: 处理{self.__sec_id}的DataFrame格式完毕')
        
        return df
 
    def trans_sec_id(self):
        '''
        将证券代码存储为wt格式
        '''
        split_sec_id = self.__sec_id.split('.') # 000005.SZ or A2209.DCE 
        left = split_sec_id[0]
        right = split_sec_id[1]

        if self.__sec_type == SecType.FUT_TICK or self.__sec_type == SecType.FUT_MIN or self.__sec_type == SecType.FUT_DAILY:      
            sub = ''
            code = ''
            flag = True
            for ch in left:
                if ch.isalpha() and flag == True:
                    sub += ch
                else:
                    flag = False
                    code += ch

            self.__sec_id = right + '.' + sub + '.' + code

        elif self.__sec_type == SecType.STK_TICK or self.__sec_type == SecType.STK_MIN or self.__sec_type == SecType.STK_DAILY:
            if right == 'SZ':
                right = 'SZSE'
            elif right == 'SH':
                right = 'SSE'
            elif right == 'BJ':
                right = 'BSE'
            
            self.__sec_id = right + '.STK.' + left

        return
   
    def save_csv_data(self, df: Optional[pd.DataFrame], path:str):
        '''
        存储DataFrame数据到指定路径的CSV文件中

        @df     从trans_data()方法获取的DataFrame
        @path   CSV文件存储路径
        '''
        if df is None:
            print("传入save_csv_data()的DataFrame为None")
            return

        path = os.path.join(path, 'csv')
        # 修改sec_id为wt格式
        self.trans_sec_id()

        filename = self.__sec_id + '_' + self.__suffix + '.csv'
        df.to_csv(os.path.join(path, filename), index=False)

        print(f'    Stage3: 以CSV格式存储{self.__sec_id}的DataFrame完毕')

    def save_bin_data(self, csvFolder: str, binFolder: str):
        '''
        把csv文件夹内的所有csv文件转化为dsb文件（支持不同频率的数据），并移动到正确的位置下

        @csvFolder  csv文件夹
        @binFolder  dsb文件夹
        '''
        # 创建分类文件夹
        if not os.path.exists(os.path.join(csvFolder, 'm1')):
            os.makedirs(os.path.join(csvFolder, 'm1'))
        if not os.path.exists(os.path.join(csvFolder, 'm5')):
            os.makedirs(os.path.join(csvFolder, 'm5'))
        if not os.path.exists(os.path.join(csvFolder, 'd')):
            os.makedirs(os.path.join(csvFolder, 'd'))

        # 对CSV文件夹内的所有CSV文件根据频率进行分类，分为分钟线、五分钟线、日线3类
        csv_dir = os.walk(csvFolder)
        files = []
        for p, dir_list, file_list in csv_dir:
            for filename in file_list:
                if filename.endswith('.csv'):
                    if filename.find('_m1') != -1:
                        new_filename = os.path.join(csvFolder, 'm1', filename)
                        filename = os.path.join(csvFolder, filename) 
                        tup = (filename, new_filename)
                        files.append(tup)    
                    elif filename.find('_m5') != -1:
                        new_filename = os.path.join(csvFolder, 'm5', filename)
                        filename = os.path.join(csvFolder, filename) 
                        tup = (filename, new_filename)
                        files.append(tup)
                    elif filename.find('_d') != -1:
                        new_filename = os.path.join(csvFolder, 'd', filename)
                        filename = os.path.join(csvFolder, filename) 
                        tup = (filename, new_filename)
                        files.append(tup)
        
        # 复制文件到分类文件夹，因为文件循环内复制文件到内部目录会使得该文件被再次读取，所以复制文件操作放到外面执行      
        for item in files:
            shutil.copy(item[0], item[1])
            print(f'copy file {item[0]} to {item[1]}')


        # 创建临时文件夹用于临时存储本次转储的dsb文件，这是因为trans_csv_bars()后dsb文件名、文件位置不对，需要重新命名和移动位置
        tmpFolder = os.path.join(binFolder, 'tmp')

        # 将所有类型csv转化为dsb
        dtHelper = WtDataHelper()
        dir_m1 = os.path.join(csvFolder, 'm1')
        dtHelper.trans_csv_bars(dir_m1, tmpFolder, 'm1')
        dir_m5 = os.path.join(csvFolder, 'm5')
        dtHelper.trans_csv_bars(dir_m5, tmpFolder, 'm5')
        dir_d = os.path.join(csvFolder, 'd')
        dtHelper.trans_csv_bars(dir_d, tmpFolder, 'd')
        
        # 删除分类文件夹
        shutil.rmtree(dir_m1)
        shutil.rmtree(dir_m5)
        shutil.rmtree(dir_d)

        print("Remove tmp dir in storage/csv")

        # 将dsb文件移动到正确的路径下
        dir = os.walk(tmpFolder)
        for p, dir_list, file_list in dir:
            for filename in file_list:
                # path为每个文件具体的导出路径
                path = binFolder

                if filename.endswith('.dsb'):
                    split_filename = filename.split('.') # eg: DCE.A.2209_m1.dsb

                    if split_filename[2].find('_m1') != -1:
                        path = os.path.join(path, 'min1')
                    elif split_filename[2].find('_m5') != -1:
                        path = os.path.join(path, 'min5')
                    elif split_filename[2].find('_d') != -1:
                        path = os.path.join(path, 'day')

                    path = os.path.join(path, split_filename[0])

                    if not os.path.exists(path):
                        os.makedirs(path)

                    # 重命名并移动文件
                    filename = os.path.join(tmpFolder, filename)
                    # 区分股票和期货
                    if split_filename[0] == 'BSE' or split_filename[0] == 'SSE' or split_filename[0] == 'SZSE':
                        new_filename = split_filename[2].split('_')[0] + '.dsb'
                    else:
                        new_filename = split_filename[1] + split_filename[2].split('_')[0] + '.dsb'
                    new_filename = os.path.join(path, new_filename)
                    shutil.move(filename, new_filename)
                    print(f'move file {filename} to {new_filename}')

        # 删除临时dsb文件夹
        os.removedirs(tmpFolder)

    def load_tick_data(self, sec_ids: Union[str, List[str]], sec_type: SecType, path:str='/mnt/data/wtdata/storage/'):
        for sec_id in sec_ids if isinstance(sec_ids, list) else [sec_ids]:
            print(f"处理证券{sec_id}:")
            period = self.get_data(sec_id, sec_type)

            if period is None or len(period) == 0:
                print("    {sec_id}数据为空，处理结束")
                continue
            
            period = period.reset_index()
            start = period.iloc[0]['datetime']
            end = period.iloc[-1]['datetime']
            start_date = dt.date(start.year, start.month, start.day)
            end_date = dt.date(end.year, end.month, end.day)
            print(f"    确定开始时间为{start_date},结束时间为{end_date}")

            try:
                cur_date = start_date
                while (cur_date <= end_date):
                    df = self.get_data(sec_id, sec_type, cur_date, cur_date)
                    self.trans_tick(df, path)
                    cur_date = cur_date + dt.timedelta(days=1)
                
            except Exception:
                traceback.print_exc()
                print("    Error: 处理该证券时发生错误")
    
    def load_bar_data(self, sec_ids: Union[str, List[str]], sec_type: SecType, start_date: Optional[dt.date]=None, end_date: Optional[dt.date]=None, fields: List[str]=None, path:str='/mnt/data/wtdata/storage/'):
        '''
        主力API，用于批量读取、转换格式、存储证券数据到CSV文件中

        @sec_ids：单只证券或证券列表
        @sec_type：证券类型
        @start_date：起始时间，默认为None
        @end_date：结束时间，默认为None
        @fields：证券各字段
        @path：证券数据存储路径
        '''
        for sec_id in sec_ids if isinstance(sec_ids, list) else [sec_ids]:
            print(f"处理证券{sec_id}:")
            try:
                df = self.get_data(sec_id, sec_type, start_date, end_date, fields)
                df = self.trans_data(df)
                self.save_csv_data(df, path=path)
            except:
                print("    Error: 处理该证券时发生错误")

            print(f"处理证券{sec_id}结束\n")

class StkDailyDataLoaderFromParquet:
    def __init__(self, path) -> None:
        self.__stk_daily = pd.read_parquet(path)
        self.__api = QdbApi()    
        self.__stk_id = ''      # 证券代码，用作每次循环的变量

        if self.__stk_daily is None:
            print('初始化数据失败，请检查路径')
            return None
        
    def get_stk_daily(self, ticker:str) -> Optional[pd.DataFrame]:
        self.__stk_id = ticker

        data = self.__stk_daily.loc[self.__stk_daily['stk_id'] == ticker]

        if data is None:
            print("get_stk_daily()得到的数据为None，请检查ticker")     
            return None   

        print(f"    Stage 1: get {ticker} daily data")

        return data

    def trans_stk_daily(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        if df is None:
            print("传入trans_stk_daily()的DataFrame为None")
            return None

        reserved_fields = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount']

        df = df.loc[:, reserved_fields]

        df = df.rename(columns={'date': '<Date>', 
                                'volume' : '<Vol>', 
                                'amount' : '<Money>',
                                'open' : '<Open>',
                                'close' : '<Close>',
                                'high' : '<High>',
                                'low' : '<Low>'})

        df['<Date>'] = df['<Date>'].astype('datetime64').dt.strftime('%Y/%m/%d')

        print(f"    Stage 2: trans stk daily data format")

        return df

    def trans_stk_id(self):
        split_stk_id = self.__stk_id.split('.') # eg: 000005.SZ 
        left = split_stk_id[0]
        right = split_stk_id[1]

        if right == 'SZ':
            right = 'SZSE'
        elif right == 'SH':
            right = 'SSE'
        elif right == 'BJ':
            right = 'BSE'
            
        self.__stk_id = right + '.STK.' + left

    def save_csv_data(self, df: Optional[pd.DataFrame], path:str):
        if df is None:
            print("传入save_csv_data()的DataFrame为None")
            return

        # 修改sec_id为wt格式
        self.trans_stk_id()

        path = os.path.join(path, 'csv')
        filename = self.__stk_id + '_d.csv'
        
        df.to_csv(os.path.join(path, filename), index=False)

        print(f'    Stage 3: 以CSV格式存储{self.__stk_id}的DataFrame完毕')

    def load_data(self, path:str):
        stk_list = list(self.__api.get_stk_list().reset_index()['stk_id'])
        for stk in stk_list:
            print(f'处理股票{stk}:')
            try:
                df = self.get_stk_daily(stk)
                df = self.trans_stk_daily(df)
                self.save_csv_data(df, path)
            except Exception as e:
                traceback.print_exc()
                print("    Error: 处理该证券时发生错误")






if __name__ == "__main__":

    # tick数据转储示例（直接转储为dsb文件）
    # dl = DataLoaderFromQdb()
    # api = QdbApi()
    # dl.load_tick_data(list(api.get_fut_list().reset_index()['fut_id']), SecType.FUT_TICK, path='/mnt/data/wtdata/storage_new')
    
    # 股票日线数据转储示例（转储为csv文件）
    dl = StkDailyDataLoaderFromParquet('/mnt/data/stock_daily.data')
    dl.load_data('/mnt/data/wtdata/storage/his/csv')

    
    # 分钟线、日线转储示例（转储为csv文件）
    # import sys
    # savedStdout = sys.stdout
    # savedStderr = sys.stderr
    # f = open('log_trans_dsb.txt', 'w')
    # sys.stdout = f
    # sys.stderr = f
    # dl = DataLoaderFromQdb()
    # api = QdbApi()
    # time1 = dt.datetime.now()
    # dl.load_data(list(api.get_fut_list().reset_index()['fut_id']), SecType.FUT_MIN, start_date=dt.date(1900,1,1), end_date=dt.date(2200,1,1), path='/mnt/data/wtdata/storage')
    # dl.load_data(list(api.get_fut_list().reset_index()['fut_id']), SecType.FUT_DAILY, start_date=dt.date(1900,1,1), end_date=dt.date(2200,1,1), path='/mnt/data/wtdata/storage')
    # time2 = dt.datetime.now()
    # print(f"程序执行时间为：{(time2-time1).seconds} seconds")
    # f.close()
    # sys.stdout = savedStdout
    # sys.stderr = savedStderr

    # csv文件转储为dsb文件示例
    # dl = DataLoaderFromQdb()
    # dl.save_bin_data("/mnt/data/wtdata/storage/his", "/mnt/data/wtdata/storage/his")
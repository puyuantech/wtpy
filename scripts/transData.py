from fileinput import filename
import sys
sys.path.append('..')

import os
import shutil
from qdb import QdbApi
import datetime as dt
import pandas as pd
from enum import Enum
from typing import Optional, List, Union

from wtpy.wrapper import WtDataHelper
from wtpy.WtCoreDefs import WTSBarStruct, WTSTickStruct, BarList, TickList
from wtpy.SessionMgr import SessionInfo, SessionMgr
from ctypes import POINTER

# export QDB_CONFIG_PATH=~/.qdb/config.yml

class SecType(Enum):
    STK_TICK = 0
    STK_MIN = 1
    STK_DAILY = 2
    FUT_TICK = 3
    FUT_MIN = 4
    FUT_DAILY = 5


class DataLoaderFromQdbToCSV():

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

    def handle_stk_tick(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        # todo@ronniehu
        pass

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
    
    def handle_fut_tick(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        # todo@ronniehu
        pass

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
        
        print(f'    Stage2:处理{self.__sec_id}的DataFrame格式完毕')
        
        return df
 
    def trans_sec_id(self):
        '''
        将证券代码存储为wt格式，股票的北交所暂未支持
        '''
        split_sec_id = self.__sec_id.split('.')
        left = split_sec_id[0]
        right = split_sec_id[1]

        if self.__sec_type == SecType.FUT_MIN or self.__sec_type == SecType.FUT_DAILY:      
            sub = ''
            code = ''
            for ch in left:
                if ch.isalpha():
                    sub += ch.lower()
                elif ch.isnumeric():
                    code += ch

            self.__sec_id = right + '.' + sub + '.' + code

        elif self.__sec_type == SecType.STK_MIN or self.__sec_type == SecType.STK_DAILY:
            if right == 'SZ':
                right = 'SZSE'
            elif right == 'SH':
                right = 'SSE'
            
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

        if not path.endswith('/'):
            path += '/'

        path += 'csv/'
        # 修改sec_id为wt格式
        self.trans_sec_id()

        filename = path + self.__sec_id + '_' + self.__suffix + '.csv'
        df.to_csv(filename, index=False)

        print(f'    Stage3: 以CSV格式存储{self.__sec_id}的DataFrame完毕')

    def save_bin_data(self, csvFolder: str, binFolder: str):
        '''
        把csv文件夹内的所有csv文件转化为dsb文件（支持不同频率的数据），并移动到正确的位置下

        @csvFolder  csv文件夹
        @binFolder  dsb文件夹
        '''
        # 创建分类文件夹
        if not os.path.exists(csvFolder + '/m1'):
            os.makedirs(csvFolder + '/m1')
        if not os.path.exists(csvFolder + '/m5'):
            os.makedirs(csvFolder + '/m5')
        if not os.path.exists(csvFolder + '/d'):
            os.makedirs(csvFolder + '/d')

        # 对CSV文件夹内的所有CSV文件根据频率进行分类，分为分钟线、五分钟线、日线3类
        csv_dir = os.walk(csvFolder)
        files = []
        for p, dir_list, file_list in csv_dir:
            for filename in file_list:
                if filename.endswith('.csv'):
                    if filename.find('_m1') != -1:
                        new_dir = os.path.join(csvFolder, 'm1/')
                        new_filename = new_dir + filename
                        filename = os.path.join(csvFolder, filename) 
                        tup = (filename, new_filename)
                        files.append(tup)    
                    elif filename.find('_m5') != -1:
                        new_dir = os.path.join(csvFolder, 'm5/')
                        new_filename = new_dir + filename
                        filename = os.path.join(csvFolder, filename) 
                        tup = (filename, new_filename)
                        files.append(tup)
                    elif filename.find('_d') != -1:
                        new_dir = os.path.join(csvFolder, 'd/')
                        new_filename = new_dir + filename
                        filename = os.path.join(csvFolder, filename) 
                        tup = (filename, new_filename)
                        files.append(tup)
        
        # 复制文件到分类文件夹，因为文件循环内复制文件到内部目录会使得该文件被再次读取，所以复制文件操作放到外面执行      
        for item in files:
            shutil.copy(item[0], item[1])
            print(f'copy file {item[0]} to {item[1]}')


        # 创建临时文件夹用于临时存储本次转储的dsb文件，这是因为trans_csv_bars()后dsb文件名、文件位置不对，需要重新命名和移动位置
        if not binFolder.endswith('/'):
            binFolder  += '/'
        tmpFolder = binFolder + 'tmp'

        # 将所有类型csv转化为dsb
        dtHelper = WtDataHelper()
        dir_m1 = os.path.join(csvFolder, 'm1/')
        dtHelper.trans_csv_bars(dir_m1, tmpFolder, 'm1')
        dir_m5 = os.path.join(csvFolder, 'm5/')
        dtHelper.trans_csv_bars(dir_m5, tmpFolder, 'm5')
        dir_d = os.path.join(csvFolder, 'd/')
        dtHelper.trans_csv_bars(dir_d, tmpFolder, 'd')
        
        # 删除分类文件夹
        shutil.rmtree(dir_m1)
        shutil.rmtree(dir_m5)
        shutil.rmtree(dir_d)

        # 将dsb文件移动到正确的路径下
        dir = os.walk(tmpFolder)
        for p, dir_list, file_list in dir:
            for filename in file_list:
                # path为每个文件具体的导出路径
                path = binFolder

                if filename.endswith('.dsb'):
                    split_filename = filename.split('.')

                    if split_filename[2].find('_m1') != -1:
                        if not path.endswith('/'):
                            path  += '/'
                        path += 'min1/'
                    elif split_filename[2].find('_m5') != -1:
                        if not path.endswith('/'):
                            path  += '/'
                        path += 'min5/'
                    elif split_filename[2].find('_d') != -1:
                        if not path.endswith('/'):
                            path  += '/'
                        path += 'day/'

                    path += split_filename[0] + '/'

                    if not os.path.exists(path):
                        os.makedirs(path)

                    # 重命名并移动文件
                    filename = os.path.join(tmpFolder, filename)
                    new_filename = split_filename[2].split('_')[0] + '.dsb'
                    new_filename = os.path.join(path, new_filename)
                    shutil.move(filename, new_filename)
                    print(f'move file {filename} to {new_filename}')

        # 删除临时dsb文件夹
        os.removedirs(tmpFolder)





    def resample_bars(self, barFile:str, period:str, times:int, fromTime:int, endTime:int, sessInfo:SessionInfo, csvname:str):
        '''
        [deprecated]
        # todo@ronniehu
        '''
        dtHelper = WtDataHelper()
        res = dtHelper.resample_bars(barFile, period, times, fromTime, endTime, sessInfo)
        df = res.to_pandas()
        df.to_csv(csvname)        


    def load_data(self, sec_ids: Union[str, List[str]], sec_type: SecType, start_date: Optional[dt.date]=None, end_date: Optional[dt.date]=None, fields: List[str]=None, path:str='/home/hujiaye/Wondertrader/storage/'):
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
                self.save_csv_data(df, path='/home/hujiaye/Wondertrader/storage')
            except:
                print("    Error: 处理该证券时发生错误")

            print(f"处理证券{sec_id}结束\n")





if __name__ == "__main__":


    # # 数据转储方法1
    # stks = api.get_stk_list().reset_index()

    # for stk in stks['stk_id']:
    #     dl.load_data(stk, SecType.STK_DAILY)
    #     # pass
    # # 数据转储方式2
    # dl.load_data(list(api.get_fut_list().reset_index()['fut_id']), SecType.FUT_DAILY)

    import sys
    savedStdout = sys.stdout
    savedStderr = sys.stderr
    f = open('log-d.txt', 'a')

    sys.stdout = f
    sys.stderr = f
 
    dl = DataLoaderFromQdbToCSV()
    api = QdbApi()

    time1 = dt.datetime.now()
    dl.load_data(list(api.get_stk_list().reset_index()['stk_id']), SecType.STK_DAILY, start_date=dt.date(1900, 1, 1), end_date=dt.date(2300, 1,1))
    time2 = dt.datetime.now()

    print(f"程序执行时间为：{(time2-time1).seconds} seconds")

    time1 = dt.datetime.now()
    dl.load_data(list(api.get_fut_list().reset_index()['fut_id']), SecType.FUT_DAILY, start_date=dt.date(1900, 1, 1), end_date=dt.date(2300, 1,1))
    time2 = dt.datetime.now()

    print(f"程序执行时间为：{(time2-time1).seconds} seconds")


    f.close()

    sys.stdout = savedStdout
    sys.stderr = savedStderr

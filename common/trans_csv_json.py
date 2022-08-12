import json
import pandas as pd

# 用于将config_data目录的csv配置文件转换为common目录的json配置文件的脚本

# 该部分可以将common目录的json配置文件转化为csv类型的配置文件
def trans_fut_comms_to_csv(src:str, tgt:str):
    res = "exch,types,covermode,pricemode,category,precision,pricetick,volscale,exchg,session,holiday,name\n"

    with open(src) as f:
        data = json.load(f)

        exchgs = {'SHF', 'SGE', 'CZC', 'CFE', 'INE', 'DCE', 'NYM'}

        for exch in exchgs:
            for type in data[exch]:
                res += str(exch) + ','
                res += str(type) + ","
                res += str(data[exch][type]['covermode']) + ","
                res += str(data[exch][type]['pricemode']) + ","
                res += str(data[exch][type]['category']) + ","
                res += str(data[exch][type]['precision']) + ","
                res += str(data[exch][type]['pricetick']) + ","
                res += str(data[exch][type]['volscale']) + ","
                res += str(data[exch][type]['exchg']) + ","
                res += str(data[exch][type]['session']) + ","
                res += str(data[exch][type]['holiday']) + ","
                res += str(data[exch][type]['name']) + "\n"

    with open(tgt, 'w') as f:
        f.write(res)   

def trans_stk_comms_to_csv(src:str, tgt:str):
    res = "exch,types,covermode,pricemode,category,precision,pricetick,volscale,exchg,session,holiday,name,trademode\n"

    with open(src) as f:
        data = json.load(f)

        exchgs = {'SSE', 'SZSE', 'BSE'}

        for exch in exchgs:
            for type in data[exch]:
                res += str(exch) + ','
                res += str(type) + ","
                res += str(data[exch][type]['covermode']) + ","
                res += str(data[exch][type]['pricemode']) + ","
                res += str(data[exch][type]['category']) + ","
                res += str(data[exch][type]['precision']) + ","
                res += str(data[exch][type]['pricetick']) + ","
                res += str(data[exch][type]['volscale']) + ","
                res += str(data[exch][type]['exchg']) + ","
                res += str(data[exch][type]['session']) + ","
                res += str(data[exch][type]['holiday']) + ","
                res += str(data[exch][type]['name']) + ","
                res += str(data[exch][type]['trademode']) + "\n"

    with open(tgt, 'w') as f:
        f.write(res)   

def trans_fut_contracts_to_csv(src:str, tgt:str):
    res = "exch,contract,name,code,product,maxlimitqty,maxmarketqty,exchg\n"

    with open(src) as f:
        data = json.load(f)

        exchgs = {'SHF', 'SGE', 'CZC', 'CFE', 'INE', 'DCE', 'NYM'}

        for exch in exchgs:
            for contract in data[exch]:
                res += str(exch) + ','
                res += str(contract) + ","
                res += str(data[exch][contract]['name']) + ","
                res += str(data[exch][contract]['code']) + ","
                res += str(data[exch][contract]['product']) + ","
                res += str(data[exch][contract]['maxlimitqty']) + ","
                res += str(data[exch][contract]['maxmarketqty']) + ","
                res += str(data[exch][contract]['exchg']) + "\n"
                
    with open(tgt, 'w') as f:
        f.write(res)   

def trans_stk_contracts_to_csv(src:str, tgt:str):
    res = "exch,contract,area,code,indust,name,product,exchg\n"

    with open(src) as f:
        data = json.load(f)

        exchgs = {'SSE', 'SZSE', 'BSE'}

        for exch in exchgs:
            for contract in data[exch]:
                res += str(exch) + ','
                res += str(contract) + ","
                res += str(data[exch][contract]['area']) + ","
                res += str(data[exch][contract]['code']) + ","
                res += str(data[exch][contract]['indust']) + ","
                res += str(data[exch][contract]['name']) + ","
                res += str(data[exch][contract]['product']) + ","
                res += str(data[exch][contract]['exchg']) + "\n"
                
    with open(tgt, 'w') as f:
        f.write(res)   

def trans_fees_to_csv(src:str, tgt:str):
    res = "types,open,close,closetoday,byvolume\n"

    with open(src) as f:
        data = json.load(f)

        for type in data:
            res += str(type) + ','
            res += str(data[type]['open']) + ','
            res += str(data[type]['close']) + ','
            res += str(data[type]['closetoday']) + ','
            res += str(data[type]['byvolume']) + '\n'

    with open(tgt, 'w') as f:
        f.write(res)   

# 该部分可以config_data目录的csv配置文件转换为common目录的json配置文件
def trans_fut_comms_csv_to_json(src:str, tgt:str):
    basic = {}
    basic['SHF'] = {}
    basic['SGE'] = {}
    basic['CZC'] = {}
    basic['CFE'] = {}
    basic['INE'] = {}
    basic['DCE'] = {}
    basic['NYM'] = {}

    df = pd.read_csv(src)

    for item in df.to_dict(orient='records'):
        unit = {}
        unit['covermode'] = item['covermode']
        unit['pricemode'] = item['pricemode']
        unit['category'] = item['category']
        unit['precision'] = item['precision']
        unit['pricetick'] = item['pricetick']
        unit['volscale'] = item['volscale']
        unit['exchg'] = item['exchg']
        unit['session'] = item['session']
        unit['holiday'] = item['holiday']
        unit['name'] = item['name']
        if item['exch'] == 'SHF':
            basic['SHF'][item['types']] = unit
        elif item['exch'] == 'SGE':
            basic['SGE'][item['types']] = unit
        elif item['exch'] == 'CZC':
            basic['CZC'][item['types']] = unit
        elif item['exch'] == 'CFE':
            basic['CFE'][item['types']] = unit
        elif item['exch'] == 'INE':
            basic['INE'][item['types']] = unit
        elif item['exch'] == 'DCE':
            basic['DCE'][item['types']] = unit
        elif item['exch'] == 'NYM':
            basic['NYM'][item['types']] = unit

    data_json = json.dumps(basic, indent=4, ensure_ascii=False)

    with open(tgt, 'w') as f:
        f.write(data_json)

def trans_stk_comms_csv_to_json(src:str, tgt:str):
    basic = {}
    basic['SSE'] = {}
    basic['SZSE'] = {}
    basic['BSE'] = {}

    df = pd.read_csv(src)

    for item in df.to_dict(orient='records'):
        unit = {}
        unit['covermode'] = item['covermode']
        unit['pricemode'] = item['pricemode']
        unit['category'] = item['category']
        unit['precision'] = item['precision']
        unit['pricetick'] = item['pricetick']
        unit['volscale'] = item['volscale']
        unit['exchg'] = item['exchg']
        unit['session'] = item['session']
        unit['holiday'] = item['holiday']
        unit['name'] = item['name']
        unit['trademode'] = item['trademode']
        if item['exch'] == 'SSE':
            basic['SSE'][item['types']] = unit
        if item['exch'] == 'SZSE':
            basic['SZSE'][item['types']] = unit
        if item['exch'] == 'BSE':
            basic['BSE'][item['types']] = unit

    data_json = json.dumps(basic, indent=4, ensure_ascii=False)

    with open(tgt, 'w') as f:
        f.write(data_json)

def trans_fut_contracts_csv_to_json(src:str, tgt:str):
    basic = {}
    basic['SHF'] = {}
    basic['SGE'] = {}
    basic['CZC'] = {}
    basic['CFE'] = {}
    basic['INE'] = {}
    basic['DCE'] = {}
    basic['NYM'] = {}

    df = pd.read_csv(src)

    for item in df.to_dict(orient='records'):
        unit = {}
        unit['name'] = item['name']
        unit['code'] = item['code']
        unit['product'] = item['product']
        unit['maxlimitqty'] = item['maxlimitqty']
        unit['maxmarketqty'] = item['maxmarketqty']
        unit['exchg'] = item['exchg']
        if item['exch'] == 'SHF':
            basic['SHF'][item['contract']] = unit
        elif item['exch'] == 'SGE':
            basic['SGE'][item['contract']] = unit
        elif item['exch'] == 'CZC':
            basic['CZC'][item['contract']] = unit
        elif item['exch'] == 'CFE':
            basic['CFE'][item['contract']] = unit
        elif item['exch'] == 'INE':
            basic['INE'][item['contract']] = unit
        elif item['exch'] == 'DCE':
            basic['DCE'][item['contract']] = unit
        elif item['exch'] == 'NYM':
            basic['NYM'][item['contract']] = unit

    data_json = json.dumps(basic, indent=4, ensure_ascii=False)

    with open(tgt, 'w') as f:
        f.write(data_json)

def trans_stk_contracts_csv_to_json(src:str, tgt:str):
    basic = {}
    basic['SSE'] = {}
    basic['SZSE'] = {}
    basic['BSE'] = {}

    df = pd.read_csv(src)

    for item in df.to_dict(orient='records'):
        unit = {}
        unit['name'] = item['name']
        unit['code'] = str(item['code']).rjust(6, '0')
        unit['product'] = item['product']
        unit['area'] = item['area']
        unit['indust'] = item['indust']
        unit['exchg'] = item['exchg']
        if item['exch'] == 'SSE':
            basic['SSE'][str(item['contract']).rjust(6, '0')] = unit
        elif item['exch'] == 'SZSE':
            basic['SZSE'][str(item['contract']).rjust(6, '0')] = unit
        elif item['exch'] == 'BSE':
            basic['BSE'][str(item['contract']).rjust(6, '0')] = unit
        
    data_json = json.dumps(basic, indent=4, ensure_ascii=False)

    with open(tgt, 'w') as f:
        f.write(data_json)

def trans_fees_csv_to_json(src:str, tgt:str):
    basic = {}

    df = pd.read_csv(src)

    for item in df.to_dict(orient='records'):
        unit = {}
        unit['open'] = item['open']
        unit['close'] = item['close']
        unit['closetoday'] = item['closetoday']
        unit['byvolume'] = item['byvolume']
        basic[item['types']] = unit

    data_json = json.dumps(basic, indent=4, ensure_ascii=False)

    with open(tgt, 'w') as f:
        f.write(data_json)


def trans_commons_csv_to_json():

    trans_fut_comms_csv_to_json('../config_data/fut_comms.csv', './fut_comms.json')
    trans_stk_comms_csv_to_json('../config_data/stk_comms.csv', './stk_comms.json')

    trans_fut_contracts_csv_to_json('../config_data/fut_contracts.csv', './fut_contracts.json')
    trans_stk_contracts_csv_to_json('../config_data/stk_contracts.csv', './stk_contracts.json')

    trans_fees_csv_to_json('../config_data/stk_fees.csv', './stk_fees.json')
    trans_fees_csv_to_json('../config_data/fut_fees.csv', './fut_fees.json')

if __name__ == "__main__":

    trans_commons_csv_to_json()
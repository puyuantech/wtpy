import os
os.environ['QDB_CONFIG_PATH']='/home/hujiaye/.qdb/config.yml'
import json
from qdb import QdbApi


def create_stk_contracts():
    api = QdbApi()
    stks = api.get_stk_list().reset_index()
    basic = {}
    basic['SSE'] = {}
    basic['SZSE'] = {}
    basic['BSE'] = {}

    for stk in stks.to_dict(orient='records'):
        element = {}
        element['area'] = stk['city']
        element['code'] = stk['sec_code']
        element['indust'] = stk['sw_industry_name'] or "0"
        element['name'] = stk['stk_name']
        element['product'] = 'STK'
        if stk['stk_id'].endswith('SZ'):
            element['exchg'] = 'SZSE'
            basic['SZSE'][stk['sec_code']] = element
        elif stk['stk_id'].endswith('SH'):
            element['exchg'] = 'SSE'
            basic['SSE'][stk['sec_code']] = element
        elif stk['stk_id'].endswith('BJ'):
            element['exchg'] = 'BSE'
            basic['BSE'][stk['sec_code']] = element

    return basic


def create_fut_contracts():
    api = QdbApi()
    futs = api.get_fut_list().reset_index()
    basic = {}
    basic['SHF'] = {}
    basic['SGE'] = {}
    basic['CZC'] = {}
    basic['CFE'] = {}
    basic['INE'] = {}
    basic['DCE'] = {}
    basic['NYM'] = {}

    for fut in futs.to_dict(orient='records'):
        element = {}
        element['name'] = fut['fut_name']
        element['code'] = fut['local_id']
        element['product'] = fut['prod_abbr']
        element['maxlimitqty'] = 500
        element['maxmarketqty'] = 500
        
        if fut['fut_id'].endswith('SHF'):
            element['exchg'] = 'SHF'
            basic['SHF'][fut['local_id']] = element
        elif fut['fut_id'].endswith('SGE'):
            element['exchg'] = 'SGE'
            basic['SGE'][fut['local_id']] = element
        elif fut['fut_id'].endswith('CZC'):
            element['exchg'] = 'CZC'
            basic['CZC'][fut['local_id']] = element
        elif fut['fut_id'].endswith('CFE'):
            element['exchg'] = 'CFE'
            basic['CFE'][fut['local_id']] = element
        elif fut['fut_id'].endswith('INE'):
            element['exchg'] = 'INE'
            basic['INE'][fut['local_id']] = element
        elif fut['fut_id'].endswith('DCE'):
            element['exchg'] = 'DCE'
            basic['DCE'][fut['local_id']] = element
        elif fut['fut_id'].endswith('NYM'):
            element['exchg'] = 'NYM'
            basic['NYM'][fut['local_id']] = element

    return basic


def create_fut_commons():
    basic = {}
    basic['SHF'] = {}
    basic['SGE'] = {}
    basic['CZC'] = {}
    basic['CFE'] = {}
    basic['INE'] = {}
    basic['DCE'] = {}
    basic['NYM'] = {}

    shf = {'pb', 'fu', 'sp', 'ni', 'bu', 'cu', 'zn', 'rb', 'wr', 'al', 'hc', 'ru', 'au', 'ag', 'sn', 'ss'}
    for item in shf:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'SHF'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'SHF.' + item.upper()
        basic['SHF'][item.upper()] = element

    sge = {'iau', 'pt', 'agtd', 'autd', 'au', 'ag', 'nyautn'}
    for item in sge:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'SGE'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'SGE.' + item.upper()
        basic['SGE'][item.upper()] = element

    czc = {'cy', 'rm', 'sm', 'cj', 'tc', 'ro', 'sr', 'ma', 'pm', 'me', 'sf', 'cf', 'ri', 'ws', 'sa', 'wt', 'pf', 'oi', 'pk', 'lr', 'ta', 'fg', 'wh', 'er', 'zc', 'ap', 'jr', 'rs', 'ur'}
    for item in czc:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'CZC'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'CZC.' + item.upper()
        basic['CZC'][item.upper()] = element


    cfe = {'ih', 'ts', 'tf', 'if', 'ic', 't'}
    for item in cfe:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'CFE'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'CFE.' + item.upper()
        basic['CFE'][item.upper()] = element

    ine = {'sc', 'bc', 'nr', 'lu'}
    for item in ine:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'INE'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'INE.' + item.upper()
        basic['INE'][item.upper()] = element

    dce = {'c', 'fb', 'cs', 'v', 'lh', 'pg', 'm', 'b', 'jm', 'pp', 'j', 'a', 'p', 'rr', 'i', 'bb', 'jd', 'eg', 'y', 'eb', 'l'}
    for item in dce:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'DCE'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'DCE.' + item.upper()
        basic['DCE'][item.upper()] = element

    nym = {'cl'}
    for item in nym:
        element = {}
        element['covermode'] = 1
        element['pricemode'] = 1
        element['category'] = 1
        element['precision'] = 0
        element['pricetick'] = 1.0
        element['volscale'] = 10
        element['exchg'] = 'NYM'
        element['session'] = "FN2300"
        element['holiday'] = "CHINA"
        element['name'] = 'NYM.' + item.upper()
        basic['NYM'][item.upper()] = element

    return basic



if __name__ == "__main__":
    data = create_fut_commons()
    data_json = json.dumps(data, indent=4, ensure_ascii=False)
    file = open('fut_commons.json', 'w', encoding='utf-8')
    file.write(data_json)
    file.close()

import datetime
import json
import os
import logging

import urllib.request
import io
import gzip
import xml.dom.minidom
from pyquery import PyQuery as pq
import re

def extractPID(code):
    
    for idx in range(0, len(code)):
        c = code[idx]
        if '0' <= c and c <= '9': 
            break
    
    return code[:idx]

def readFileContent(filename):
    if not os.path.exists(filename):
        return ""
    f = open(filename, 'r')
    content = f.read()
    f.close()
    return content

def httpGet(url, encoding='utf-8'):
    request = urllib.request.Request(url)
    request.add_header('Accept-encoding', 'gzip')
    request.add_header(
        'User-Agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')
    try:
        f = urllib.request.urlopen(request)
        ec = f.headers.get('Content-Encoding')
        if ec == 'gzip':
            cd = f.read()
            cs = io.BytesIO(cd)
            f = gzip.GzipFile(fileobj=cs)

        return f.read().decode(encoding)
    except:
        return ""

def httpPost(url, datas, encoding='utf-8'):
    headers = {
        'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
        'Accept-encoding': 'gzip'
    }
    data = urllib.parse.urlencode(datas).encode('utf-8')
    request = urllib.request.Request(url, data, headers)
    try:
        f = urllib.request.urlopen(request)
        ec = f.headers.get('Content-Encoding')
        if ec == 'gzip':
            cd = f.read()
            cs = io.BytesIO(cd)
            f = gzip.GzipFile(fileobj=cs)

        return f.read().decode(encoding)
    except:
        return ""

class DayData:
    '''
    每日行情数据
    '''

    def __init__(self):
        self.pid = ''
        self.month = 0
        self.code = ''  # 代码
        self.close = 0  # 今收盘(收盘价)
        self.volumn = 0  # 成交量(手)
        self.hold = 0  # 空盘量(总持？持仓量)

class WtCacheMon:
    '''
    缓存管理器基类
    '''
    def __init__(self):
        self.day_cache = dict()

    def get_cache(self, exchg, curDT:datetime.datetime):
        pass

class WtCacheMonExchg(WtCacheMon):
    '''
    交易所行情缓存器\n
    通过到交易所官网上拉取当日的行情快照，缓存当日行情数据\n
    '''

    @staticmethod
    def getCffexData(curDT:datetime.datetime) -> dict:
        '''
        读取CFFEX指定日期的行情快照\n

        @curDT  指定的日期
        '''

        dtStr = curDT.strftime('%Y%m%d')
        dtNum = int(dtStr)
        path = "http://www.cffex.com.cn/fzjy/mrhq/%d/%02d/index.xml" % (dtNum/100, dtNum % 100)
        content = httpGet(path)
        if len(content) == 0:
            return None

        try:
            dom = xml.dom.minidom.parseString(content)
        except:
            logging.info("[CFFEX]%s无数据，跳过" % (dtStr))
            return None

        root = dom.documentElement
        
        items = {}
        days = root.getElementsByTagName("dailydata")
        for day in days:
            item = DayData()
            item.code = day.getElementsByTagName("instrumentid")[
                0].firstChild.data.strip()
            item.pid = day.getElementsByTagName(
                "productid")[0].firstChild.data.strip()
            item.hold = float(day.getElementsByTagName(
                "openinterest")[0].firstChild.data)
            item.close = float(day.getElementsByTagName(
                "closeprice")[0].firstChild.data)
            item.volumn = int(day.getElementsByTagName(
                "volume")[0].firstChild.data)

            item.month = item.code[len(item.pid):]

            items[item.code] = item
        return items

    @staticmethod
    def getShfeData(curDT:datetime.datetime) -> dict:
        '''
        读取SHFE指定日期的行情快照\n

        @curDT  指定的日期
        '''

        dtStr = curDT.strftime('%Y%m%d')
        content = httpGet("http://www.shfe.com.cn/data/dailydata/kx/kx%s.dat" % (dtStr))
        if len(content) == 0:
            return None
        
        items = {}
        root = json.loads(content)
        for day in root['o_curinstrument']:
            pid = day['PRODUCTID'].strip().rstrip('_f')
            dm = day['DELIVERYMONTH']
            if pid > 'zz' or dm > '2100':
                continue
            if len(str(day['CLOSEPRICE']).strip()) == 0:
                continue

            code = pid + dm

            item = DayData()
            item.pid = pid
            item.code = code
            item.hold = int(day['OPENINTEREST'])
            if day['VOLUME'] != '':
                item.volumn = int(day['VOLUME'])
            item.close = float(day["CLOSEPRICE"])
            item.month = item.code[len(item.pid):]
            items[code] = item
        return items

    @staticmethod
    def getCzceData(curDT:datetime.datetime) -> dict:
        '''
        读取CZCE指定日期的行情快照\n

        @curDT  指定的日期
        '''

        dtStr = curDT.strftime('%Y%m%d')
        url = 'http://www.czce.com.cn/cn/DFSStaticFiles/Future/%s/%s/FutureDataDaily.htm' % (dtStr[0:4], dtStr)
        try:
            html = httpGet(url).strip()
        except urllib.error.HTTPError as httperror:
            print(httperror)
            return None

        if len(html) == 0:
            return None

        dataitems = {}
        doc = pq(html)
        # print(doc(#senfe .table  table))
        items = doc('#senfe')
        # 去掉第一行标题
        items.remove('tr.tr0')
        # 获取tr   items.find('tr')
        lis = items('tr')
        # print(lis)
        # tr行数
        trcount = len(lis)
        # 遍历行
        for tr in range(0, trcount-1):
            item = DayData()
            tdlis = doc(lis[tr])('td')

            item.code = doc(tdlis[0]).text()
            ay = re.compile('[A-Za-z]+').findall(item.code)
            if len(ay) == 0:
                continue

            item.pid = ay[0]    

            close = doc(tdlis[5]).text()
            if close != '':
                item.close = float(close.replace(",",""))

            volumn = doc(tdlis[9]).text()
            if volumn != '':
                item.volumn = int(volumn.replace(",",""))

            hold = doc(tdlis[10]).text()
            if hold != '':
                item.hold = int(hold.replace(",",""))

            item.month = item.code[len(item.pid):]
            if item.month[0] == '0':
                item.month = "2" + item.month
            else:
                item.month = "1" + item.month

            dataitems[item.code] = item
        # print(dataitems)
        return dataitems

    @staticmethod
    def getDceData(curDT:datetime.datetime) -> dict:
        '''
        读取DCE指定日期的行情快照\n

        @curDT  指定的日期
        '''

        pname_map = {
            "聚乙烯": "l",
            "鸡蛋": "jd",
            "焦煤": "jm",
            "豆二": "b",
            "胶合板": "bb",
            "玉米": "c",
            "豆粕": "m",
            "棕榈油": "p",
            "玉米淀粉": "cs",
            "纤维板": "fb",
            "铁矿石": "i",
            "焦炭": "j",
            "豆一": "a",
            "聚丙烯": "pp",
            "聚氯乙烯": "v",
            "豆油": "y",
            "乙二醇":"eg",
            "粳米":"rr",
            "苯乙烯":"eb"
        }

        url = 'http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html'
        try:
            data = {}
            data['dayQuotes.variety'] = 'all'
            data['dayQuotes.trade_type'] = 0
            data['year'] = curDT.year
            data['month'] = curDT.month - 1
            data['day'] = curDT.day
            html = httpPost(url, data)
        except urllib.error.HTTPError as httperror:
            print(httperror)
            return None

        dataitems = {}
        doc = pq(html)
        items = doc('.dataArea')  # doc('#printData')
        # # 获取tr   items.find('tr')
        lis = items('tr')
        trcount = len(lis)
        # 遍历行
        for tr in range(1, trcount):

            tdlis = doc(lis[tr])('td')
            # 商品名称
            pzname = doc(tdlis[0]).text()
            if pzname not in pname_map:
                if "小计" not in pzname and "总计" not in pzname:
                    logging.error("未知品种:" + pzname)
                continue

            # 交割月份
            item = DayData()
            item.pid = pname_map[pzname]
            item.code = item.pid + doc(tdlis[1]).text()
            # 收盘价
            spj = doc(tdlis[5]).text()
            item.close = float(spj if spj != '' else 0)
            # 成交量
            item.volumn = int(doc(tdlis[10]).text())
            # 持仓量
            item.hold = int(doc(tdlis[11]).text())
            item.month = item.code[len(item.pid):]
            dataitems[item.code] = item

        return dataitems

    @staticmethod
    def getIneData(curDT:datetime.datetime) -> dict:
        '''
        读取INE指定日期的行情快照\n

        @curDT  指定的日期
        '''
        dtStr = curDT.strftime('%Y%m%d')
        content = httpGet("http://www.ine.cn/data/dailydata/kx/kx%s.dat" % (dtStr))
        if len(content) == 0:
            return None

        items = {}
        root = json.loads(content)
        for day in root['o_curinstrument']:
            pid = day['PRODUCTID'].strip().rstrip('_f')
            dm = day['DELIVERYMONTH']
            if pid != 'sc' or dm == '' or dm == '小计':
                continue
            item = DayData()
            item.pid = pid
            item.code = pid + dm
            item.hold = int(day['OPENINTEREST'])
            item.close = day['CLOSEPRICE']
            item.volumn = day['VOLUME']
            item.month = item.code[len(item.pid):]
            items[item.code] = item
        return items


    def cache_by_date(self, exchg:str, curDT:datetime.datetime):
        '''
        缓存指定日期指定交易所的行数据\n

        @exchg  交易所代码\n
        @curDT  指定日期
        '''
        dtStr = curDT.strftime('%Y%m%d')

        if dtStr not in self.day_cache:
            self.day_cache[dtStr] = dict()

        cacheItem = self.day_cache[dtStr]
        if exchg == 'CFFEX':
            cacheItem[exchg] = WtCacheMonExchg.getCffexData(curDT)
        elif exchg  == 'SHFE':
            cacheItem[exchg] = WtCacheMonExchg.getShfeData(curDT)
        elif exchg  == 'DCE':
            cacheItem[exchg] = WtCacheMonExchg.getDceData(curDT)
        elif exchg  == 'CZCE':
            cacheItem[exchg] = WtCacheMonExchg.getCzceData(curDT)
        elif exchg  == 'INE':
            cacheItem[exchg] = WtCacheMonExchg.getIneData(curDT)
        else:
            raise Exception("未知交易所代码" + exchg)

    def get_cache(self, exchg:str, curDT:datetime.datetime):
        '''
        获取指定日期的某个交易所合约的快照数据\n

        @exchg  交易所代码\n
        @curDT  指定日期
        '''
        dtStr = curDT.strftime('%Y%m%d')
        if dtStr not in self.day_cache or exchg not in self.day_cache[dtStr]:
            self.cache_by_date(exchg, curDT)

        if dtStr not in self.day_cache:
            return None

        if exchg not in self.day_cache[dtStr]:
            return None
        return self.day_cache[dtStr][exchg]

class WtCacheMonSS(WtCacheMon):
    '''
    快照缓存管理器\n
    通过读取wtpy的datakit当日生成的快照文件，缓存当日行情数据\n
    一般目录为"数据存储目录/his/snapshots/xxxxxxx.csv"
    '''

    def __init__(self, snapshot_path:str):
        WtCacheMon.__init__(self)
        self.snapshot_path = snapshot_path

    def cache_snapshot(self, curDT:datetime):
        '''
        缓存指定日期的快照数据\n

        @curDT  指定的日期
        '''
        dtStr = curDT.strftime('%Y%m%d')

        filename = "%s%s.csv" % (self.snapshot_path, dtStr)
        content = readFileContent(filename)
        lines = content.split("\n")

        if dtStr not in self.day_cache:
            self.day_cache[dtStr] = dict()

        cacheItem = self.day_cache[dtStr]
        for idx in range(1, len(lines)):
            line = lines[idx]
            if len(line) == 0:
                break
            items = line.split(",")
            
            exchg = items[1]
            if exchg not in cacheItem:
                cacheItem[exchg] = dict()

            day = DayData()
            day.pid = extractPID(items[2])
            day.code = items[2]
            # 收盘价
            day.close = float(items[6])
            # 成交量
            day.volumn = int(items[8])
            # 持仓量
            day.hold = int(items[10])
            day.month = day.code[len(day.pid):]
            if len(day.month) == 3:
                if day.month[0] >= '0' and day.month[0] <= '5':
                    day.month = "2" + day.month
                else:
                    day.month = "1" + day.month
            cacheItem[exchg][day.code] = day

    def get_cache(self, exchg, curDT:datetime):
        '''
        获取指定日期的某个交易所合约的快照数据\n

        @exchg  交易所代码\n
        @curDT  指定日期
        '''

        dtStr = curDT.strftime('%Y%m%d')
        if dtStr not in self.day_cache:
            self.cacheSnapshot(curDT)

        if dtStr not in self.day_cache:
            return None

        if exchg not in self.day_cache[dtStr]:
            return None
        return self.day_cache[dtStr][exchg]

class WtMailNotifier:
    '''
    邮件通知器
    '''
    def __init__(self, user:str, pwd:str, sender:str=None, host:str="smtp.exmail.qq.com", port=465, isSSL:bool = True):
        self.user = user
        self.pwd = pwd
        self.sender = sender if sender is not None else "WtHotNotifier"
        self.receivers = list()

        self.mail_host = host
        self.mail_port = port
        self.mail_ssl = isSSL

    def add_receiver(self, name:str, addr:str):
        '''
        添加收件人\n

        @name   收件人姓名\n
        @addr   收件人邮箱地址
        '''
        self.receivers.append({
            "name":name,
            "addr":addr
        })

    def notify(self, change_list:dict, nextDT:datetime.datetime, hotFile:str, hotMap:str):
        '''
        通知主力切换事件\n

        @change_list    当日切换的规则列表\n
        @nextDT         生效日期\n
        @hotFile        主力规则文件\n
        @hotMap         主力映射文件\n
        '''
        dtStr = nextDT.strftime('%Y.%m.%d')
    
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.application import MIMEApplication
        from email.header import Header

        sender = self.sender
        receivers = self.receivers

        content = ''
        for exchg in change_list:
            for pid in change_list[exchg]:
                item = change_list[exchg][pid][-1]
                content +=  '品种%s.%s的主力合约已切换,下个交易日(%s)生效, %s -> %s\n' % (exchg, pid, dtStr, item["from"], item["to"])

        msg_mp = MIMEMultipart()
        msg_mp['From'] = sender  # 发送者          
        
        subject = '主力合约换月邮件<%s>' % (dtStr)
        msg_mp['Subject'] = Header(subject, 'utf-8')

        content = MIMEText(subject, 'plain', 'utf-8')
        msg_mp.attach(content)

        xlspart = MIMEApplication(open(hotFile,'rb').read())
        xlspart["Content-Type"] = 'application/octet-stream'
        xlspart.add_header('Content-Disposition','attachment', filename=os.path.basename(hotFile))
        msg_mp.attach(xlspart)

        xlspart = MIMEApplication(open(hotMap,'rb').read())
        xlspart["Content-Type"] = 'application/octet-stream'
        xlspart.add_header('Content-Disposition','attachment', filename=os.path.basename(hotMap))
        msg_mp.attach(xlspart)

        if self.mail_ssl:
            smtpObj = smtplib.SMTP_SSL(self.mail_host, self.mail_port)
        else:
            smtpObj = smtplib.SMTP(self.mail_host, self.mail_port)

        try:
            smtpObj.ehlo()
            smtpObj.login(self.user, self.pwd) 
            logging.info("%s 登录成功 %s:%d", self.user, self.mail_host, self.mail_port)
        except smtplib.SMTPException as ex:
            logging.error("邮箱初始化失败：{}".format(ex))

        for item in receivers:
            to = "%s<%s>" % (item["name"], item["addr"])
            msg_mp['To'] =  Header(to, 'utf-8')    # 接收者
            try:
                smtpObj.sendmail(sender, item["addr"], msg_mp.as_string())
                logging.info("邮件发送失败，收件人: %s", to)
            except smtplib.SMTPException as ex:
                logging.error("邮件发送失败，收件人：{}, {}".format(to, ex))

class WtHotPicker:
    '''
    主力选择器
    '''
    def __init__(self, markerFile:str = "./marker.json", hotFile = "../Common/hots.json"):
        self.marker_file = markerFile
        self.hot_file = hotFile

        self.mail_notifier:WtMailNotifier = None
        self.cache_monitor:WtCacheMon = None

    def set_cacher(self, cacher:WtCacheMon):
        '''
        设置日行情缓存器
        '''
        self.cache_monitor = cacher
        
    def set_mail_notifier(self, notifier:WtMailNotifier):
        '''
        设置邮件通知器
        '''
        self.mail_notifier = notifier

    def pick_exchg_hots(self, current_hots, exchg, beginDT, endDT, alg = 0):
        '''
        确定指定市场的主力合约\n

        @current_hots   当前主力映射表\n
        @exchg          交易所代码\n
        @beginDT        开始日期\n
        @endDT          截止日期\n
        @alg            切换规则算法，0-除中金所外，按成交量确定，1-中金所，按照成交量和总持共同确定
        '''

        cacheMon = self.cache_monitor
        if exchg not in current_hots:
            current_hots[exchg] = dict()
        lastHots = current_hots[exchg]
        switch_list = {}

        curDT = beginDT

        while curDT <= endDT:
            hots = {}
            logging.info("[%s]开始拉取%s数据" % (exchg, curDT.strftime('%Y%m%d')))
            items = cacheMon.get_cache(exchg, curDT)
            if items is not None:
                for code in items:
                    item = items[code]
                    pid = item.pid
                    if pid not in hots:
                        hots[pid] = item.code
                    else:
                        # 这里开始正式的判断
                        oldCode = hots[pid]
                        oldData = items[oldCode]
                        if alg == 1:#中金所算法
                            # 如果新合约的成交量大于旧合约的成交量的三分之一，并且总持大于旧合约的总持，则进行切换
                            if item.month > oldData.month:
                                if item.hold > oldData.hold and item.volumn > oldData.volumn/3:
                                    hots[pid] = code
                            else:
                                if oldData.hold <= item.hold or oldData.volumn <= item.volumn/3:
                                    hots[pid] = code
                        elif alg == 0:
                            # 如果新合约的总持大于旧合约的总持，则进行切换
                            if item.month > oldData.month:
                                if item.hold > oldData.hold:
                                    hots[pid] = code
                            else:
                                if oldData.hold <= item.hold:
                                    hots[pid] = code

                for key in hots.keys():
                    nextDT = curDT + datetime.timedelta(days=1)
                    if key not in lastHots:
                        item = {}
                        item["date"] = int(curDT.strftime('%Y%m%d'))
                        item["from"] = ""
                        item["to"] = hots[key]
                        item["oldclose"] = 0.0
                        item["newclose"] = items[hots[key]].close
                        switch_list[key] = [item]
                        lastHots[key] = hots[key]
                        logging.info("[%s]品种%s主力确认, 确认日期: %s, %s", exchg,key, nextDT.strftime('%Y%m%d'), hots[key])
                    else:
                        oldcode = lastHots[key]
                        newcode = hots[key]
                        oldItem = None
                        if oldcode in items:
                            oldItem = items[oldcode]
                        newItem = items[newcode]
                        if oldItem is None or newItem.month > oldItem.month:
                            item = {}
                            item["date"] = int(nextDT.strftime('%Y%m%d'))
                            item["from"] = oldcode
                            item["to"] = newcode
                            if oldcode in items:
                                item["oldclose"] = items[oldcode].close
                            else:
                                item["oldclose"] = 0.0
                                item["date"] = int(curDT.strftime('%Y%m%d'))
                            item["newclose"] = items[newcode].close
                            if key not in switch_list:
                                switch_list[key] = list()
                            switch_list[key].append(item)
                            logging.info("[%s]品种%s主力切换 切换日期: %s，%s -> %s", exchg, key, nextDT.strftime('%Y%m%d'), lastHots[key], hots[key])
                            lastHots[key] = hots[key]
            # 日期递增
            curDT = curDT + datetime.timedelta(days=1)
        return switch_list, current_hots
    
    def merge_switch_list(self, total, exchg, switch_list):
        '''
        合并主力切换规则\n
        
        @total          已有的全部切换规则\n
        @exchg          交易所代码\n
        @switcg_list    新的切换规则
        '''
        if exchg not in total:
            total[exchg] = switch_list
            logging.info("[%s]全市场主力切换规则重构" % (exchg))
            return True, total
        
        bChanged = False
        for pid in switch_list:
            if pid not in total[exchg]:
                total[exchg][pid] = switch_list[pid]
                logging.info("[%s]品种%s主力切换规则重构" % (exchg, pid))
                bChanged = True
            else:
                total[exchg][pid].extend(switch_list[pid])
                logging.info("[%s]品种%s主力切换规则追加%d条" % (exchg, pid, len(switch_list[pid])))
                bChanged = True
        return bChanged, total

    def execute_rebuild(self, beginDate:datetime.datetime = None, endDate:datetime.datetime = None, exchanges = ["CFFEX", "SHFE", "CZCE", "DCE", "INE"]):
        '''
        重构全部的主力切换规则\n
        不依赖现有数据，全部重新确定主力合约的切换规则\n

        @beginDate  开始日期\n
        @endDate    截止日期\n
        @exchanges  要重构的交易所列表
        '''
        if endDate is None:
            endDate = datetime.datetime.now()

        if beginDate is None:
            beginDate = datetime.datetime.strptime("2016-01-01", '%Y-%m-%d')
        
        total = dict()
        current_hots = dict()

        for exchg in exchanges:
            current_hots[exchg] = dict()
        
        change_list = dict()
        curDate = beginDate
        while curDate <= endDate:
            for exchg in exchanges:
                alg = 1 if exchg=='CFFEX' else 0    # 中金所的换月算法和其他交易所不同
                cfHots,current_hots = self.pick_exchg_hots(current_hots, exchg, curDate, curDate, self.cache_monitor, alg=alg)

                if len(cfHots.keys()) > 0:
                    hasChange,total = self.merge_switch_list(total, exchg, cfHots)

                    if exchg not in change_list:
                        change_list[exchg] = dict()
                    change_list[exchg].update(cfHots)

            curDate = curDate + datetime.timedelta(days=1)

        #日期标记要保存
        marker = dict()
        marker["date"] = int(endDate.strftime('%Y%m%d'))
        output = open(self.marker_file, 'w')
        output.write(json.dumps(marker, sort_keys=True, indent = 4))
        output.close()
        
        logging.info("主力切换规则已更新")

        output = open(self.hot_file, 'w')
        output.write(json.dumps(total, sort_keys=True, indent = 4))
        output.close()

        output = open("hotmap.json", 'w')
        output.write(json.dumps(current_hots, sort_keys=True, indent = 4))
        output.close()

        if self.mail_notifier is not None:
            self.mail_notifier.notify(change_list, endDate, self.hot_file, "hotmap.json")

        return total
  
    def execute_increment(self, endDate:datetime.datetime = None, exchanges = ["CFFEX", "SHFE", "CZCE", "DCE", "INE"]):
        '''
        增量更新主力切换规则\n
        会自动加载marker.json取得上次更新的日期，并读取hots.json确定当前的映射规则\n

        @endDate    截止日期\n
        @exchanges  要重构的交易所列表
        '''

        if endDate is None:
            endDate = datetime.datetime.now()

        markerFile = self.marker_file
        hotFile = self.hot_file

        marker = {"date":"0"}
        c = readFileContent(markerFile)
        if len(c) > 0:
            marker = json.loads(c)

        c = readFileContent(hotFile)
        total = dict()
        if len(c) > 0:
            total = json.loads(c)
        else:
            marker["date"] = "0"

        lastDate = str(marker["date"])
        if lastDate >= endDate.strftime('%Y%m%d'):
            logging.info("上次更新日期%s大于结束日期%s，退出更新" % (lastDate, endDate.strftime('%Y%m%d')))
            exit()
        elif lastDate != "0":
            beginDT = datetime.datetime.strptime(lastDate, "%Y%m%d") + datetime.timedelta(days=1)
        else:
            beginDT = datetime.datetime.strptime("2016-01-01", '%Y-%m-%d')
        
        current_hots = dict()

        for exchg in total:
            if exchg not in current_hots:
                current_hots[exchg] = dict()

            for pid in total[exchg]:
                ay = total[exchg][pid]
                current_hots[exchg][pid] = ay[-1]["to"]
        
        bChanged = False
        change_list = dict()
        for exchg in exchanges:
            logging.info("[%s]开始分析主力换月数据" % exchg)
            alg = 1 if exchg=='CFFEX' else 0    # 中金所的换月算法和其他交易所不同
            cfHots,current_hots = self.pick_exchg_hots(current_hots, exchg, beginDT, endDate, self.cache_monitor, alg=alg)

            if len(cfHots.keys()) > 0:
                hasChange,total = self.merge_switch_list(total, exchg, cfHots)
                bChanged  = bChanged or hasChange
                change_list[exchg] = cfHots


        #日期标记要保存
        marker = dict()
        marker["date"] = int(endDate.strftime('%Y%m%d'))
        output = open(markerFile, 'w')
        output.write(json.dumps(marker, sort_keys=True, indent = 4))
        output.close()
        
        if bChanged:
            logging.info("主力切换规则已更新")

            output = open(hotFile, 'w')
            output.write(json.dumps(total, sort_keys=True, indent = 4))
            output.close()

            output = open("hotmap.json", 'w')
            output.write(json.dumps(current_hots, sort_keys=True, indent = 4))
            output.close()

            if self.mail_notifier is not None:
                self.mail_notifier.notify(change_list, endDate, hotFile, "hotmap.json")
        else:
            logging.info("主力切换规则未更新，不保存数据")
from asyncio.windows_events import NULL
from cmath import log
from datetime import datetime,timedelta
import pandas as pd
from pandas.core.common import flatten
import os
from sqlalchemy import Column, String, create_engine,Integer,VARCHAR,Table,MetaData,inspect
from sqlalchemy.orm import sessionmaker
from WindPy import w
import requests
import json
from rich.console import Console

console = Console(color_system="256")

# 格式化一个表格
def printTable(data,columns):
    d = pd.DataFrame(data,columns=columns)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 180)
    print(d)
    return d


# 数组扁平化
def flat(_list: list):
    return list(flatten(_list))

# 获取当前路径
def getCurPath(path=''):
    return os.path.join(os.getcwd(), path)


session:sessionmaker = None;
engine = None;

def getEngine():
    return engine

def initDB():
    global engine
    engine = create_engine('mysql+mysqlconnector://root:dc123456@192.168.22.35:3306/ams?auth_plugin=mysql_native_password')
    DBSession = sessionmaker(bind=engine)
    console.print(':white_check_mark:  [#37E2D5]数据库连接成功啦[/] ')
    global session
    session = DBSession()

def addData(data):
    print(data)
    session.add(data)
    session.commit()

def removeOldData(name='nav',endDate=''):
    metadata = MetaData()
    table = Table(name,metadata,autoload=True, autoload_with=engine)
    print(datetime.strptime(endDate,'%Y-%m-%d'))
    count = session.query(table).filter(table.c.endDate if name != 'tdays' else table.c.date>=datetime.strptime(endDate,'%Y-%m-%d')).delete()
    console.print(f'{name}表[#37E2D5]删除{count}条数据')
    session.commit()


# 查询当前表的最新数据日期
def getlastItemDate(name='nav'):
    metadata = MetaData()
    table = Table(name,metadata,autoload=True, autoload_with=engine)
    lastItem = session.query(table).order_by(table.c.id.desc()).first()
    endDate = None
    if(lastItem!=None):
        endDate = lastItem.endDate if name != 'tdays' else lastItem.date
        return formateDate(endDate,f='%Y-%m-%d')
    else:
        # endDate = datetime.strptime('2000-01-01','%Y-%m-%d')
        return 'No Data'
    

def formateDate(date:datetime,f='%Y%m%d'):
    return date.strftime(f)

today = datetime.today()

def getLastMonday():
    weekday = today.weekday()
    return formateDate(today - timedelta(days=weekday + 7))
    

def getLastFriday():
    weekday = today.weekday()
    return formateDate(today - timedelta(days=weekday + 3))

def getYearFristDay(date):
    return formateDate(datetime(datetime.strptime(date,'%Y-%m-%d').year, 1, 1))

def getToday(f=None):
    return formateDate(today,f)

def getNextDay(date):
    return formateDate(datetime.strptime(date,'%Y-%m-%d')+timedelta(days=1),f='%Y-%m-%d')

def getTDays(startDate="2022-01-01",endDate=getToday("%Y-%m-%d")):
    dates = w.tdays(startDate, endDate, '').Data[0]
    return list(map(formateDate,dates))

wxkey = '7a6866e7-4900-4376-9a75-a4bb7d5e71d2'

def robot(msg=""):
    timestr = formateDate(datetime.now(),'%Y%m%d %H:%M:%S')
    requests.post(url='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key='+wxkey, headers={'Content-Type': 'application/json'}, data=json.dumps({
        "msgtype": "text",
        "text": {
            "content": f'【{timestr}】\n{msg}'
        }
    }))
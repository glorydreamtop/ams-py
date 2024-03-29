from datetime import datetime,timedelta
import pandas as pd
from pandas.core.common import flatten
import os
from sqlalchemy import Column, String, create_engine,Integer,Date,Table,MetaData
from sqlalchemy.orm import sessionmaker,declarative_base
from WindPy import w
import requests
import json
import time
from rich.console import Console

Base = declarative_base()

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

def getSession():
    return session

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

def removeOldData(name='nav',startDate='',endDate=''):
    metadata = MetaData()
    table = Table(name,metadata,autoload=True, autoload_with=engine)
    s = datetime.strptime(startDate,'%Y-%m-%d')
    e = datetime.strptime(endDate,'%Y-%m-%d')
    if(name!='tdays'):
        count = session.query(table).filter((table.c.endDate>=s) & (table.c.endDate<=e)).delete()

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

class NoData(Base):
    __tablename__ = 'nodata'  
    id = Column(Integer, primary_key=True)  
    pname = Column(String(20))  
    tablename = Column(String(20)) 
    startDate = Column(Date) 
    endDate = Column(Date) 

def noDataLog(tablename,pname,startDate,endDate):
    metadata = MetaData()
    table = Table('nodata',metadata,autoload=True, autoload_with=getEngine())
    items = session.query(table).filter((table.c.endDate==datetime.strptime(endDate,'%Y%m%d')) & (table.c.pname==pname)&(table.c.tablename==tablename)).all()
    if(len(items)>0):
        return
    session.add(NoData(tablename=tablename,pname=pname,startDate=startDate,endDate=endDate))
    session.commit()

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
    return formateDate(datetime(datetime.strptime(date,'%Y%m%d').year, 1, 1))

def getToday(f='%Y-%m-%d'):
    return formateDate(today,f)

def getNextDay(date):
    return formateDate(datetime.strptime(date,'%Y-%m-%d')+timedelta(days=1),f='%Y-%m-%d')

def getTDays(startDate="2021-12-01",endDate=getToday("%Y-%m-%d")):
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
    
# 使用 time 模块来测量代码执行时间
def start_timer():
    return time.time()

def end_timer(start_time):
    end_time = time.time()
    elapsed_time = end_time - start_time
    return elapsed_time
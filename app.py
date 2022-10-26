from cmath import log
from datetime import datetime
import functools
from main import connectWind
from WindPy import w
import pandas as pd
import utils
from utils import formateDate
from flask import Flask,request,jsonify
from rich.console import Console
import jwt

console = Console(color_system="256")

sectorcode = '1'

console.print(f'[#5FD068]这是实时数据提取服务，通常你需要经常开着我')

# utils.initDB()
# connectWind()

app = Flask(__name__)


def jwt_auth(func):
    @functools.wraps(func)
    def wrapper(*args,**kwags):
        # try:
        #     token = request.headers["authorization"][7:]
        #     payload = jwt.decode(token,'whatispastisprologue',algorithms=["HS256"])
        #     if('userid' not in payload):
        #         raise '没有userid'
        # except Exception as e:
        #     print(e)
        #     resjson = {
        #         "msg":'身份验证失败',
        #         "code":500,
        #         "flag":False
        #     }
        #     return jsonify(resjson),401
        return func(*args,**kwags)
    return wrapper
    
m = ('平湖1号','平湖2号','平湖3号')

@app.route("/py/getTotalPL",methods=["GET"])
@jwt_auth
def getTotalPLApi():
    name:str = request.args.get('name')
    startDate = utils.formateDate(datetime.strptime(request.args.get('startDate'),'%Y-%m-%d'))
    endDate = utils.formateDate(datetime.strptime(request.args.get('endDate'),'%Y-%m-%d'))
    Merge = request.args.get('merge')
    connectWind()
    
    Penetration = "M" if name in m else "N"
    query = "TotalPL,ExposureRatio,Trading" if Merge == 'N' else 'TotalPL,AssetAccount,Trading'
    console.print(f'实时查询{name},{query}数据，{startDate}到{endDate}')
    data = w.wpf(name, query,f"view=AMS;startDate={startDate};endDate={endDate};Currency=CNY;sectorcode=1;displaymode=1;AmountUnit=0;Penetration={Penetration};Merge={Merge}").Data
    
    if(data==[['WPF: No Data.']]):
        data = []
    df_ = pd.DataFrame(data=data)
    
    if(df_.empty):
        data = []
    df = df_.T.drop(axis=1,columns=[0])
    if(Merge=='C'):
        df = df_.T.drop(axis=1,columns=[0])
    df.insert(0, 'pname', name)
    df.columns = ['pname','code','name','value','exposure','trading']if Merge == 'N' else['pname','code','name','value','acc','trading']
    df['startDate'] = startDate
    df['endDate'] = endDate
    res = df.loc[df['trading']!='平衡项']
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":res.to_dict(orient="records"),
            "startDate":request.args.get('startDate'),
            "endDate":request.args.get('endDate'),
            'pname':name
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

@app.route("/py/getWPF",methods=["GET"])
@jwt_auth
def getgetWPFApi():
    name = request.args.get('name')
    query = request.args.get('query')
    view = request.args.get('view')
    connectWind()
    data = w.wpf(name, query,view).Data
    if(data == [['WPF: Server no response!.']]):
        resjson = {
        "msg":'请重试',
        "code":500,
        "flag":False
    }
    if(data==[['WPF: No Data.']]):
        df = pd.DataFrame(data=[])
    else:
        df = pd.DataFrame(data=data).T
        df = df.drop(axis=1,columns=[0])
        df.insert(0, 'pname', name)
        df.columns = ['pname','code','name',*query.split(',')]
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":df.to_dict(orient="records"),
            'pname':name
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

@app.route("/py/getWPS",methods=["GET"])
@jwt_auth
def getWPSApi():
    name = request.args.get('name')
    query = request.args.get('query')
    view = request.args.get('view')
    connectWind()
    data = w.wps(name, query,view).Data
    if(data == [['WPS: Server no response!.']]):
        resjson = {
        "msg":'请重试',
        "code":500,
        "flag":False
    }
    if(data==[['WPS: No Data.']]):
        df = pd.DataFrame(data=[])
    else:
        df = pd.DataFrame(data=data).T
        df.insert(0, 'name', name)
        df.columns = ['name',*query.split(',')]
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":df.to_dict(orient="records"),
            'pname':name
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

@app.route("/py/getTdays",methods=["GET"])
@jwt_auth
def getTDays():
    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    dates = w.tdays(startDate, endDate, '').Data[0]
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":dates,
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

@app.route("/py/getPosition",methods=["GET"])
@jwt_auth
def getPositionApi():
    name = request.args.get('name')
    endDate = utils.formateDate(datetime.strptime(request.args.get('endDate'),'%Y-%m-%d'))
    data = w.wpf(name, "Position,Trading",f"view=AMS;startDate={endDate};endDate={endDate};Currency=CNY;sectorcode=1;displaymode=1;AmountUnit=0;Merge=N").Data
    if(data==[['WPF: No Data.']]):
        data = []
    df_ = pd.DataFrame(data=data)
    
    if(df_.empty):
        data = []
    df = df_.T.drop(axis=1,columns=[0])
    df.insert(0, 'pname', name)
    df.columns = ['pname','code','name','value','trading']
    res = df[df['trading'].isin(['股票','期货'])]
    res['date'] = endDate
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":res.to_dict(orient="records"),
            "date":request.args.get('endDate'),
            'pname':name
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

# 实时行情
@app.route("/py/getWSQ",methods=["GET"])
@jwt_auth
def getWSQApi():
    name = request.args.get('name')
    query = request.args.get('query')
    data = w.wsq(name, query)
    if(data==[['WSQ: No Data.']]):
        data = []
    l = data.Data
    l.insert(0,data.Codes)
    l = pd.DataFrame(data=l).T
    l.columns = ['code','value']
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":l.to_dict(orient="records"),
            "date":request.args.get('endDate'),
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

@app.route("/py/getNav",methods=["GET"])
@jwt_auth
def getNavApi():
    name = request.args.get('name')
    startDate = utils.formateDate(datetime.strptime(request.args.get('startDate'),'%Y-%m-%d'))
    endDate = utils.formateDate(datetime.strptime(request.args.get('endDate'),'%Y-%m-%d'))
    connectWind()
    query = "Nav,Nav_Acc,Return_w,Return_m,Return_q,Return_y,Return_std,NetAsset"
    console.print(f'实时查询{name},{query}数据，{startDate}到{endDate}')
    data = w.wps(name, query,f"view=AMS;startDate={startDate};endDate={endDate};Currency=CNY;fee=1").Data
    
    if(data==[['WPF: No Data.']]):
        data = []
    df_ = pd.DataFrame(data=data)
    if(df_.empty):
        data = []
    arr = utils.flat([name,utils.flat(data),startDate,endDate])
    data = pd.DataFrame(data=arr).T
    data.columns=['name','Nav','Nav_Acc','Return_w','Return_m','Return_q','Return_y','Return_std','NetAsset','startDate','endDate']
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":data.to_dict(orient="records"),
            "startDate":request.args.get('startDate'),
            "endDate":request.args.get('endDate'),
            'pname':name
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

# 日期序列
@app.route("/py/getWSD",methods=["GET"])
@jwt_auth
def getWSDApi():
    names = request.args.get('names')
    # startDate = utils.formateDate(datetime.strptime(request.args.get('startDate'),'%Y-%m-%d'))
    # endDate = utils.formateDate(datetime.strptime(request.args.get('endDate'),'%Y-%m-%d'))
    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    console.print(f'实时查询{names}数据，{startDate}到{endDate}')
    connectWind()
    data = w.wsd(names, "close", startDate, endDate, "Currency=CNY")
    l = data.Data
    l.insert(0,list(map(lambda x:formateDate(x,'%Y-%m-%d'),data.Times)))
    l = pd.DataFrame(data=l).T
    l.columns = ['date',*names.split(',')]
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":l.to_dict(orient="records"),
            "startDate":request.args.get('startDate'),
            "endDate":request.args.get('endDate'),
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)

# 运营报表
@app.route("/py/getWPD",methods=["GET"])
@jwt_auth
def getWPDApi():
    name = request.args.get('name')
    query = request.args.get('query')
    view = request.args.get('view')
    startDate = request.args.get('startDate')
    endDate = request.args.get('endDate')
    connectWind()
    data = w.wpd(name, query,startDate,endDate,view)
    if(data.Data == [['WPD: Server no response!.']]):
        resjson = {
        "msg":'请重试',
        "code":500,
        "flag":False
    }
    if(data==[['WPD: No Data.']]):
        df = pd.DataFrame(data=[])
    else:
        l = data.Data
        l.insert(0,list(map(lambda x:formateDate(x,'%Y-%m-%d'),data.Times)))
        df = pd.DataFrame(data=l).T
        df.insert(0, 'productName', name)
        df.columns = ['productName','date',*query.split(',')]
    resjson = {
        "msg":'查询成功',
        "info":{
            "list":df.to_dict(orient="records"),
            'pname':name
        },
        "code":200,
        "flag":True
    }
    return jsonify(resjson)


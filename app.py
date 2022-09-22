from datetime import datetime
from main import connectWind
from WindPy import w
import pandas as pd
import utils
from utils import formateDate, getEngine
from flask import Flask,request,jsonify
from rich.console import Console

console = Console(color_system="256")

sectorcode = '1'

console.print(f'[#5FD068]这是实时数据提取服务，通常你需要经常开着我')

utils.initDB()
connectWind()

app = Flask(__name__)

@app.route("/py/getTotalPL",methods=["GET"])
def getTotalPLApi():
    name = request.args.get('name')
    startDate = utils.formateDate(datetime.strptime(request.args.get('startDate'),'%Y-%m-%d'))
    endDate = utils.formateDate(datetime.strptime(request.args.get('endDate'),'%Y-%m-%d'))
    Merge = request.args.get('merge')
    connectWind()
    m = ('平湖1号','平湖2号','平湖3号')
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

@app.route("/py/getNav",methods=["GET"])
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

@app.route("/py/getWSD",methods=["GET"])
def getWSD():
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
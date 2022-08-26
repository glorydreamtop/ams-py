from datetime import datetime
from main import connectWind
from WindPy import w
import pandas as pd
import utils
from utils import getEngine
from flask import Flask,request,jsonify
from rich.console import Console

console = Console(color_system="256")

sectorcode = '10002'

utils.initDB()
connectWind()

app = Flask(__name__)

@app.route("/py/getTotalPL",methods=["GET"])
def getTotalPLApi():
    name = request.args.get('name')
    startDate = utils.formateDate(datetime.strptime(request.args.get('startDate'),'%Y-%m-%d'))
    endDate = utils.formateDate(datetime.strptime(request.args.get('endDate'),'%Y-%m-%d'))
    Merge = request.args.get('merge')
    console.print(f'{name},{startDate},{endDate},{Merge}')
    connectWind()
    m = ('平湖1号','平湖2号','平湖3号')
    Penetration = "M" if name in m else "N"
    query = "TotalPL,ExposureRatio,Trading" if Merge == 'Y' else 'TotalPL,AssetAccount,Trading'
    # 分类：自定义分类；视图：全部+分类+明细；汇总方式：单产品汇总；持仓穿透：不穿透
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
    df.columns = ['pname','code','name','value','exposure','trading']if Merge == 'Y' else['pname','code','name','value','acc','trading']
    df['startDate'] = startDate
    df['endDate'] = endDate
    res = df.loc[df['trading']!='平衡项']
    # res.to_csv(utils.getCurPath(f'表格{name}.csv'),encoding='utf_8_sig')
    # res.to_sql('totalpl1', getEngine(),if_exists='append',index=False)
    # res.to_csv('./data.csv')
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
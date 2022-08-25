from datetime import datetime, timedelta
from time import sleep
from WindPy import w
import pandas as pd
import utils
from utils import getEngine, getNextDay,getTDays, getToday, getYearFristDay, getlastItemDate, removeOldData, robot
from rich.console import Console
from rich.table import Table,box
from rich.prompt import Prompt

console = Console(color_system="256")

def connectWind():
    wconnected =  w.isconnected() # 判断WindPy是否已经登录成功
    if(wconnected==False):
        with console.status('准备链接Wind服务...') as s:
            w.start() # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒  
            wconnected =  w.isconnected()
        print('Wind服务链接成功' if wconnected==True else 'Wind服务链接失败')

PortfolioNames = ('平湖1号','平湖2号','平湖3号','众诚一号','嘉佑1号','闰诚1号','青诚一号','百川量化一号')


def getNav(name,startDate = '20220722',endDate = '20220722',isRetry=False):
    connectWind()
    # NAVReturnRise_1w,Nav,Nav_Acc  产品净值增长（1周），单位净值，累计净值,区间回报
    if(isRetry == False):
        print(f'下面查询{endDate}的产品{name}净值增长（1周），单位净值，累计净值数据')
    else:
        print(f'重试{endDate}的产品{name}净值增长（1周），单位净值，累计净值数据')
    data = w.wps(name, "Nav,Nav_Acc,Return_w,Return_m,Return_y,Return_std,NetAsset",f"view=AMS;startDate={startDate};endDate={endDate};Currency=CNY;fee=1").Data
    if(data == [['WPS: Server no response!.']]):
        robot(f'{name},{endDate},Nav,no response,retry...')
        return getNav(name,startDate,endDate,isRetry=True)
    if(data==[['WPS: No Data.']]):
        robot(f'{name},{endDate},Nav,no data.')
        return None
    print(data)
    arr = utils.flat([name,utils.flat(data),startDate,endDate])
    data = pd.DataFrame(data=arr).T
    data.columns=['name','Nav','Nav_Acc','Return_w','Return_m','Return_y','Return_std','NetAsset','startDate','endDate']
    if(data.empty):
        return None
    return data

def getTotalPL(name='青诚一号',startDate='20220101',endDate = '20220104',isRetry=False,Merge='N'):
    if(isRetry != False):
        console.print(f'[#990000]重试{endDate}产品{name}的每个交易日到年初区间盈亏数据')
    connectWind()
    _merge = '按账户汇总'if Merge == 'C' else'按单产品汇总'
    # 获得所有的区间盈亏和风险净敞口%数据
    console.print(f'\n区间盈亏数据，截止日期：[#4B8673]{startDate},{endDate},{_merge}')
    m = ('平湖1号','平湖2号','平湖3号')
    Penetration = "M" if name in m else "N"
    query = "TotalPL,ExposureRatio,Trading" if Merge == 'N' else 'TotalPL,AssetAccount,Trading'
    # 分类：自定义分类；视图：分类；汇总方式：单产品汇总；持仓穿透：不穿透
    data = w.wpf(name, query,f"view=AMS;startDate={startDate};endDate={endDate};Currency=CNY;sectorcode=1;displaymode=1;AmountUnit=0;Penetration={Penetration};Merge={Merge}").Data
    if(data==[['WPF_New: Server no response!.']]):
        robot(f'{name},{endDate},totalPL,no response,retry...')
        return getTotalPL(name=name,startDate=startDate,endDate=endDate,isRetry=True,Merge=Merge)
    if(data==[['WPF: No Data.']]):
        robot(f'{name},{endDate},totalPL,no data.')
        return None
    df_ = pd.DataFrame(data=data)
    if(df_.empty):
        robot(f'{name},{endDate},totalPL,is empty.')
        return None
    df = df_.T.drop(axis=1,columns=[0])
    if(Merge=='C'):
        df = df_.T.drop(axis=1,columns=[0])
    df.insert(0, 'pname', name)
    df.columns = ['pname','code','name','value','exposure','trading']if Merge == 'N' else['pname','code','name','value','acc','trading']
    df['startDate'] = startDate
    df['endDate'] = endDate
    # df.to_csv(utils.getCurPath(f'表格{name}.csv'),encoding='utf_8_sig')
    res = df.loc[df['trading']!='平衡项']
    
    return res


def nav(startDate,endDate):
    dates = getTDays(startDate,endDate)
    for date in dates:
        dataframes = []
        for name in PortfolioNames:
            d = getNav(name=name,startDate=date,endDate=date)
            dataframes.append(d)
        if all(i is None for i in dataframes):
            continue
        res = pd.concat(dataframes)
        print(res)
        res.to_sql('nav', getEngine(),if_exists='append',index=False)

def totalPL(startDate,endDate):
    dates = getTDays(startDate,endDate)
    # yearFirstDay = getYearFristDay(date)
    yearFirstDay = '2020-01-01'
    for date in dates:
        dataframes = []
        for name in PortfolioNames:
            d = getTotalPL(name=name,startDate=yearFirstDay,endDate=date)
            dataframes.append(d)
        if all(i is None for i in dataframes):
            continue
        res = pd.concat(dataframes)
        res.to_sql('totalpl', getEngine(),if_exists='append',index=False)

def totalPLAcc(startDate,endDate):
    dates = getTDays(startDate,endDate)
    # yearFirstDay = getYearFristDay(date)
    yearFirstDay = '2020-01-01'
    names = ('分策略_嘉佑一号','众诚一号')
    for date in dates:
        dataframes = []
        for name in names:
            d = getTotalPL(name=name,startDate=yearFirstDay,endDate=date,Merge='C')
            dataframes.append(d)
        if all(i is None for i in dataframes):
            continue
        res = pd.concat(dataframes)
        res.to_sql('totalplAcc', getEngine(),if_exists='append',index=False)
    
def saveTDays():
    connectWind()
    with console.status('[#61afe9]更新交易日列表...') as s:
        dates = getTDays()
        d = pd.DataFrame(dates)
        d.columns = ['date']
        d.to_sql('tdays', getEngine(),if_exists='append',index=False)


def queryLastItemDate():
    tables = {
        "nav": '',
        "totalpl": '',
        "totalplAcc":'',
        "tdays":''
    }
    tableNameCNMap = {
        'nav':"净值表",
        'totalpl':"区间盈亏单产品汇总表",
        'totalplAcc':"区间盈亏账户汇总表",
        'tdays':"交易日表"
    }
    names = list(tables.keys())
    with console.status("[#5FD068]我们先来看看最新的数据是哪天的......") as status:
        for (index,name) in enumerate(names):
            res = getlastItemDate(name)
            tables[name] = f'{res},{getToday()}'
        sleep(1)
    console.print('\n    [#5FD068]当前的最新数据是 :point_down: :point_down: :point_down:')
    console.print('\n    [#5FD068]交易日表总会更新到最新，无需手动处理 :smiley:')
    table = Table(show_header=True,box=box.ROUNDED,show_lines=True,header_style="#5FD068")
    table.add_column('序号')
    table.add_column('表名')
    table.add_column('最新日期')
    table.add_column('计划更新到')
    for (index,name) in enumerate(names):
        table.add_row(f'{index+1}',tableNameCNMap[name],*tables[name].split(','))
    
    console.print(table)
    ask1 = Prompt.ask(f'\n所以我们要从[#37E2D5]下一个交易日[/]开始更新吗？(Y/n):smiley: ')
    if('n' not in ask1.strip()):
        console.print('\n[#5FD068]OKOK![/] :ok_hand:')
        for name in names:
            [s,e] = tables[name].split(',')
            tables[name] = f'{getNextDay(s)},{e}'
        return tables
    else:
        idx = 1
        while idx:
            idx = int(Prompt.ask(f'\n输入你想修改更新日期的表序号，没有就回车 ',default=0))
            if(idx>0):
                tables[names[idx-1]] = Prompt.ask(f'\n你要将[#5FD068]{tableNameCNMap[names[idx-1]]}[/]更新区间修改为YYYY-MM-DD,YYYY-MM-DD')
        table = Table(show_header=True,box=box.ROUNDED,show_lines=True,header_style="#5FD068")
        table.add_column('序号')
        table.add_column('表名')
        table.add_column('更新起点')
        table.add_column('更新终点')
        for (index,name) in enumerate(names):
            table.add_row(f'{index+1}',name,*tables[name].split(','))
        console.print(table)
    ask2 = Prompt.ask(f'\n开始更新数据吗(Y/n):smiley: ')
    console.print('[#5FD068]清除更新起点之后的旧数据...')
    for name in names:
        [startDate,endDate] = tables[name].split(',')
        if(name!='tdays'):
            removeOldData(name,startDate,endDate)
    if('n' not in ask2.strip()):
        return tables
    else:
        return None

if __name__ == '__main__':
    connectWind()
    utils.initDB()
    
    updateDates = queryLastItemDate()
    now = datetime.now()
    today = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    if(updateDates!=None):
        saveTDays()
        if(datetime.strptime(updateDates['nav'].split(',')[0],'%Y-%m-%d') >= today):
            console.print('[#FF5B00]净值表无需更新')
        else:
            [startDate,endDate] = updateDates['nav'].split(',')
            nav(startDate,endDate)
        if(datetime.strptime(updateDates['totalpl'].split(',')[0],'%Y-%m-%d') >= today):
            console.print('[#FF5B00]区间盈亏单产品汇总表无需更新')
        else:
            [startDate,endDate] = updateDates['totalpl'].split(',')
            totalPL(startDate,endDate)
        if(datetime.strptime(updateDates['totalplAcc'].split(',')[0],'%Y-%m-%d') >= today):
            console.print('[#FF5B00]区间盈亏账户汇总表无需更新')
        else:
            [startDate,endDate] = updateDates['totalplAcc'].split(',')
            totalPLAcc(startDate,endDate)
from ast import arg
from datetime import datetime, timedelta
from time import sleep
from WindPy import w
import pandas as pd
import utils
from utils import formateDate, getEngine, getNextDay, getSession,getTDays, getToday, getYearFristDay, getlastItemDate, noDataLog, removeOldData, robot
from rich.console import Console
from rich.table import Table,box
from rich.prompt import Prompt
from threading import Thread
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy import Table as SQL_Table,MetaData

console = Console(color_system="256")

def connectWind():
    wconnected =  w.isconnected() # 判断WindPy是否已经登录成功
    if(wconnected==False):
        with console.status('准备链接Wind服务...') as s:
            w.start() # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒  
            wconnected =  w.isconnected()
        print('Wind服务链接成功' if wconnected==True else 'Wind服务链接失败')

PortfolioNames = ('平湖1号','平湖2号','平湖3号','众诚一号','嘉佑一号','闰诚1号','青诚一号','百川量化一号')


def getNav(name,startDate = '20220722',endDate = '20220722',isRetry=False):
    connectWind()
    # NAVReturnRise_1w,Nav,Nav_Acc  产品净值增长（1周），单位净值，累计净值,区间回报
    if(isRetry != False):
        console.print(f'[#fa2832]重试{endDate}的产品{name}净值增长（1周），单位净值，累计净值数据')
    data = w.wps(name, "Nav,Nav_Acc,NAVReturnRise_1w,NAVReturnRise_1m,NAVReturnRise_1p,NAVReturnRise_1y,Return_w,Return_m,Return_q,Return_y,Return_std,NetAsset",f"view=AMS;startDate={startDate};endDate={endDate};Currency=CNY;fee=1").Data
    if(data == [['WPS: Server no response!.']]):
        robot(f'{name},{endDate},Nav,no response,retry...')
        return getNav(name,startDate,endDate,isRetry=True)
    if(data==[['WPS: No Data.']]):
        robot(f'{name},{endDate},Nav,no data.')
        noDataLog(tablename='nav',pname=name,startDate=startDate,endDate=endDate)
        return None
    if(isRetry == False):
        console.print(f'[#4B8673]已获得{endDate}的产品{name}净值增长（1周），单位净值，累计净值数据')
    else:
        print(f'重试[#4B8673]{endDate}的产品{name}[/]净值增长（1周），单位净值，累计净值数据成功！')
    arr = utils.flat([name,utils.flat(data),startDate,endDate])
    data = pd.DataFrame(data=arr).T
    data.columns=['name','Nav','Nav_Acc','NAVReturnRise_1w','NAVReturnRise_1m','NAVReturnRise_1p','NAVReturnRise_1y','Return_w','Return_m','Return_q','Return_y','Return_std','NetAsset','startDate','endDate']
    if(data.empty):
        return None
    return data

def getTotalPL(name='青诚一号',startDate='20220101',endDate = '20220104',isRetry=False,Merge='N',year=False):
    if(isRetry != False):
        console.print(f'[#fa2832]重试{endDate}产品{name}的每个交易日到年初区间盈亏数据')
    connectWind()
    _merge = '按账户汇总'if Merge == 'C' else'按单产品汇总'
    
    m = ('平湖1号','平湖2号','平湖3号')
    Penetration = "M" if name in m else "N"
    query = "TotalPL,ExposureRatio,Trading" if Merge == 'N' else 'TotalPL,AssetAccount,NetHoldingValue,Trading'
    s_query = f"view=AMS;startDate={startDate};endDate={endDate};Currency=CNY;sectorcode=1;displaymode=1;AmountUnit=0;Penetration={Penetration};Merge={Merge}"
    # 分类：自定义分类；视图：分类；汇总方式：单产品汇总；持仓穿透：不穿透
    data = w.wpf(name, query,s_query).Data
    if(data==[['WPF_New: Server no response!.']]):
        robot(f'{name},{endDate},totalPL,no response,retry...')
        return getTotalPL(name=name,startDate=startDate,endDate=endDate,isRetry=True,Merge=Merge,year=year)
    if(data==[['WPF: No Data.']]):
        print(name,query,s_query)
        if(Merge=='N'):
            noDataLog(tablename='totalpl_year' if year == True else 'totalpl',pname=name,startDate=startDate,endDate=endDate)
            robot(f'{name},{endDate},totalPL,no data.')
        if(Merge=='C'):
            noDataLog(tablename='totalplAcc_year' if year == True else 'totalplAcc',pname=name,startDate=startDate,endDate=endDate)
            robot(f'{name},{endDate},totalPLAcc,no data.')
        return None
        # return getTotalPL(name=name,startDate=startDate,endDate=endDate,isRetry=True,Merge=Merge)
    # 获得所有的区间盈亏和风险净敞口%数据
    if(isRetry != False):
        console.print(f'[#fa2832]重试{endDate}产品{name}数据成功！')
    console.print(f'\n已获得[#4B8673]{name}[/]区间盈亏数据，截止日期：[#4B8673]{startDate},{endDate},{_merge}')
    df_ = pd.DataFrame(data=data)
    if(df_.empty):
        robot(f'{name},{endDate},totalPL,is empty.')
        return None
    df = df_.T.drop(axis=1,columns=[0])
    if(Merge=='C'):
        df = df_.T.drop(axis=1,columns=[0])
    df.insert(0, 'pname', name)
    df.columns = ['pname','code','name','value','exposure','trading']if Merge == 'N' else['pname','code','name','value','acc','net','trading']
    df['startDate'] = startDate
    df['endDate'] = endDate
    # df.to_csv(utils.getCurPath(f'表格{name}.csv'),encoding='utf_8_sig')
    res = df.loc[df['trading']!='平衡项']
    
    return res

def getWSD(names,startDate = '20220722',endDate = '20220722',isRetry=False):
    connectWind()
    data = w.wsd(names, "close", "2022-01-01", "2022-09-06", "Currency=CNY")
    if(data == [['WSD: Server no response!.']]):
        robot(f'{names},WSD,no response,retry...')
        return getWSD(names,startDate,endDate,isRetry=True)
    if(data==[['WSD: No Data.']]):
        robot(f'{names},WSD,no data.')
        return None
    if(isRetry == False):
        console.print(f'[#4B8673]已获得{names}，{startDate}到{endDate}的数据')
    else:
        print(f'重试[#4B8673]{names}，{startDate}到{endDate}的数据成功！')
    arr = utils.flat([name,utils.flat(data),startDate,endDate])
    data = pd.DataFrame(data=arr).T
    data.columns=['name','endDate']
    if(data.empty):
        return None
    return data

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
        # print(res)
        res.to_sql('nav', getEngine(),if_exists='append',index=False)

def totalPL(startDate,endDate,year=False):
    dates = getTDays(startDate,endDate)
    dates1 = []
    dates2 = []
    for index,date in enumerate(dates):
        if(index%2==0):
            dates1.append(date)
        else:
            dates2.append(date)
    pool = ProcessPoolExecutor(max_workers=2)
    pool.map(query_totalPL,[(dates1,year,'N',PortfolioNames),(dates2,year,'N',PortfolioNames)])

def query_totalPL(params):
    utils.initDB()
    dates = params[0]
    year = params[1]
    Merge = params[2]
    names = params[3]
    for date in dates:
        yearFirstDay = getYearFristDay(date) if year == True else '20200101'
        console.print(f'yearFirstDay is {yearFirstDay}')
        dataframes = []
        for name in names:
            d = getTotalPL(name=name,startDate=yearFirstDay,endDate=date,isRetry=False,Merge=Merge,year=year)
            dataframes.append(d)
        if all(i is None for i in dataframes):
            continue
        res = pd.concat(dataframes)
        if(Merge=='N'):
            tablename = 'totalpl_year' if year == True else 'totalpl'
        else:
            tablename = 'totalplacc_year' if year == True else 'totalplacc'
        res.to_sql(tablename, getEngine(),if_exists='append',index=False)
        console.print(f'{date}数据已写入[#4B8673]{tablename}[/]表')

def totalPLAcc(startDate,endDate,year=False):
    names = ('分策略_嘉佑一号','众诚一号','分策略_嘉佑一号2','分策略_平湖2号')
    dates = getTDays(startDate,endDate)
    dates1 = []
    dates2 = []
    for index,date in enumerate(dates):
        if(index%2==0):
            dates1.append(date)
        else:
            dates2.append(date)
    pool = ProcessPoolExecutor(max_workers=2)
    pool.map(query_totalPL,[(dates1,year,'C',names),(dates2,year,'C',names)])
    
def saveTDays():
    connectWind()
    with console.status('[#61afe9]更新交易日列表...') as s:
        dates = getTDays()
        d = pd.DataFrame(dates)
        d.columns = ['date']
        metadata = MetaData()
        table = SQL_Table('tdays',metadata,autoload=True, autoload_with=getEngine())
        getSession().query(table).delete()
        getSession().commit()
        d.to_sql('tdays', getEngine(),if_exists='append',index=False)

def fixData():
    metadata = MetaData()
    table = SQL_Table('nodata',metadata,autoload=True, autoload_with=getEngine())
    items = getSession().query(table).all()
    for item in items:
        console.print(f'{formateDate(item.endDate)}，{item.pname}数据的{item.tablename}正在补入')
        if('totalpl' in item.tablename):
            Merge='C' if'Acc' in item.tablename else 'N'
            d = getTotalPL(name=item.pname,startDate=formateDate(item.startDate),endDate=formateDate(item.endDate),isRetry=False,Merge=Merge,year=True)
        if('nav' in item.tablename):
            d = getNav(name=item.pname,startDate=formateDate(item.startDate),endDate=formateDate(item.endDate),isRetry=False)
        if(d is None):
            continue
        d.to_sql(item.tablename, getEngine(),if_exists='append',index=False)
        console.print(f'{formateDate(item.endDate)}数据已[#fa2832]补写入[/][#4B8673]{item.tablename}[/]表')
        getSession().query(table).filter((table.c.endDate==item.endDate) & (table.c.pname==item.pname)&(table.c.tablename==item.tablename)).delete()
        getSession().commit()

def queryLastItemDate():
    tables = {
        "nav": '',
        # "totalpl": '',
        "totalpl_year": '',
        # 'totalplAcc':'',
        "totalplAcc_year":'',
        "tdays":''
    }
    tableNameCNMap = {
        'nav':"净值表",
        # 'totalpl':"区间盈亏单产品汇总表",
        # 'totalplAcc':"区间盈亏账户汇总表",
        'totalpl_year':"区间盈亏单产品汇总表[年度]",
        'totalplAcc_year':"区间盈亏账户汇总表[年度]",
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
    console.print(f'[#5FD068]这是每周同步一次的数据同步服务，通常你是在你觉得数据要更新的时候（每周一）开着我直到数据爬完')
    connectWind()
    utils.initDB()
    
    # updateDates = queryLastItemDate()
    # now = datetime.now()
    # today = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    # if(updateDates!=None):
    #     saveTDays()
    #     if(datetime.strptime(updateDates['nav'].split(',')[0],'%Y-%m-%d') >= today):
    #         console.print('[#FF5B00]净值表无需更新')
    #     else:
    #         [startDate,endDate] = updateDates['nav'].split(',')
    #         nav(startDate,endDate)
    #     # if(datetime.strptime(updateDates['totalpl'].split(',')[0],'%Y-%m-%d') >= today):
    #     #     console.print('[#FF5B00]区间盈亏单产品汇总表无需更新')
    #     # else:
    #     #     [startDate,endDate] = updateDates['totalpl'].split(',')
    #     #     totalPL(startDate,endDate)
    #     # if(datetime.strptime(updateDates['totalplAcc'].split(',')[0],'%Y-%m-%d') >= today):
    #     #     console.print('[#FF5B00]区间盈亏账户汇总表无需更新')
    #     # else:
    #     #     [startDate,endDate] = updateDates['totalplAcc'].split(',')
    #     #     totalPLAcc(startDate,endDate)
    #     if(datetime.strptime(updateDates['totalpl_year'].split(',')[0],'%Y-%m-%d') >= today):
    #         console.print('[#FF5B00]区间盈亏单产品汇总表[年度]无需更新')
    #     else:
    #         [startDate,endDate] = updateDates['totalpl_year'].split(',')
    #         totalPL(startDate,endDate,year=True)
    #     if(datetime.strptime(updateDates['totalplAcc_year'].split(',')[0],'%Y-%m-%d') >= today):
    #         console.print('[#FF5B00]区间盈亏账户汇总表[年度]无需更新')
    #     else:
    #         [startDate,endDate] = updateDates['totalplAcc_year'].split(',')
    #         totalPLAcc(startDate,endDate,year=True)
    #     # console.print('[#FF5B00]开始补充遗漏数据') 
    #     # fixData()
    saveTDays()
    # totalPL(startDate='20220905',endDate='20220909',year=True)
    # totalPLAcc(startDate='20220819',endDate='20220909',year=True)
    nav(startDate='20220101',endDate='20220909')
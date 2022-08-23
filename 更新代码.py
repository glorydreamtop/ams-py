import requests
import os
import shutil
import zipfile
import tempfile
from rich.console import Console

console = Console(color_system="256")

def get_data():
    with console.status('[#37E2D5]更新代码中...') as s:
        url = "https://github.com/glorydreamtop/ams-py/archive/refs/heads/main.zip"
        response = requests.get(url,proxies={'https': 'http://127.0.0.1:7890'})
    return url, response.content
 
 
if __name__ == '__main__':
    url, data = get_data()  # data为byte字节
 
    _tmp_file = tempfile.TemporaryFile()  # 创建临时文件
 
    _tmp_file.write(data)  # byte字节数据写入临时文件
    # _tmp_file.seek(0)
 
    zf = zipfile.ZipFile(_tmp_file, mode='r')
    for names in zf.namelist():
        f = zf.extract(names,'./')  # 解压到zip目录文件下
    zf.close()
    oldPath = "ams-py-main/"
    newPath = './'
    filelist = os.listdir('ams-py-main')
    for file in filelist:
        shutil.move(os.path.join(oldPath,file),os.path.join(newPath,file))
    shutil.rmtree(oldPath)
    console.print('[#37E2D5]代码已更新')
    console.input('[#98c379]按任意键退出代码更新工具......')
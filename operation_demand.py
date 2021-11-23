import pandas as pd
import urllib.request
import os
import shutil
import zipfile
from datetime import datetime, timedelta
import time

def download_demand():
    # change this date to what date the 1st zip file
    # is in http://nemweb.com.au/Reports/ARCHIVE/Operational_Demand/ACTUAL_HH/
    initial_date = '20201011'

    dt = datetime.strptime(initial_date, '%Y%m%d')

    # loop for a total of 104 times, (2 years)
    # stored data on the AEMO website is a little over last 52 weeks
    for i in range(104):
        try:
            days = 7 * (i + 1)
            var_date = (dt + timedelta(days=days)).strftime('%Y%m%d')
            filename = f'PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_{var_date}.zip'
            url = f'http://nemweb.com.au/Reports/ARCHIVE/Operational_Demand/ACTUAL_HH/{filename}'
            print(f'Downloading: {url}')
            urllib.request.urlretrieve(url, os.path.join('raw/Demand/raw_weekly', filename))
        except urllib.error.HTTPError as e:
            print(e)
            break


def download_solar():
    # change this date to what date the 1st zip file
    # is in http://nemweb.com.au/Reports/ARCHIVE/Operational_Demand/ACTUAL_HH/
    initial_date = '20201008'

    dt = datetime.strptime(initial_date, '%Y%m%d')

    # loop for a total of 104 times, (2 years)
    # stored data on the AEMO website is a little over last 52 weeks
    for i in range(104):
        try:
            days = 7 * (i + 1)
            var_date = (dt + timedelta(days=days)).strftime('%Y%m%d')
            filename = f'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_{var_date}.zip'
            url = f'http://nemweb.com.au/Reports/ARCHIVE/ROOFTOP_PV/ACTUAL/{filename}'
            print(f'Downloading: {url}')
            urllib.request.urlretrieve(url, os.path.join('raw/PVGen/raw_weekly', filename))
        except urllib.error.HTTPError as e:
            print(e)
            break


def extract_outer(i, first, dfA, pathFrom, pathTo, pathExtract, fileList, len_fileList):
    if i == len_fileList:
        return dfA
    else:
        fileName = os.path.join(pathFrom, fileList[i])
        os.mkdir(pathTo)
        os.mkdir(pathExtract)
        with zipfile.ZipFile(fileName, 'r') as zip_ref:
            zip_ref.extractall(pathTo)
        files = os.listdir(pathTo)
        for file in files:
            fileName2 = os.path.join(pathTo, file)
            with zipfile.ZipFile(fileName2, 'r') as zip_ref:
                zip_ref.extractall(pathExtract)

        f = os.listdir(pathExtract)
        g = len(f)
        if first:
            dfA = tabulate(0, first, df, c, f, g)
            first = False
        else:
            dfB = tabulate(0, first, df, c, f, g)
            dfA = pd.concat([dfA, dfB])

        shutil.rmtree(pathTo)
        shutil.rmtree(pathExtract)
        i += 1
        return extract_outer(i, first, dfA, pathFrom, pathTo, pathExtract, fileList, len_fileList)


def tabulate(j, first, dfa, pathExtract, fileList, len_fileList):
    if j == len_fileList:
        return dfa
    else:
        fileName = os.path.join(pathExtract, fileList[j])
        if first:
            dfa = pd.read_csv(fileName, skiprows=1, skipfooter=1, engine='python')
            first = False
            j += 1
            return tabulate(j, first, dfa, pathExtract, fileList, len_fileList)
        else:
            dfb = pd.read_csv(fileName, skiprows=1, skipfooter=1, engine='python')
            dfa = pd.concat([dfa, dfb])
            j += 1
            return tabulate(j, first, dfa, pathExtract, fileList, len_fileList)


if __name__ == '__main__':
    start = time.time()

    download_demand()
    download_solar()
    init = True
    for file in os.listdir('raw'):
        pathMain = os.path.join('raw', file)
        if os.path.isdir(pathMain):
            a = os.path.join(pathMain, 'raw_weekly')
            b = os.path.join(pathMain, 'raw_30minutely')
            c = os.path.join(pathMain, 'extracted')
            d = os.listdir(a)
            e = len(d)
            df = pd.DataFrame()
            df1 = extract_outer(0, True, df, a, b, c, d, e)
        if init:
            df1.to_csv('data/dfDemand.csv', index=False)
            init = False
        else:
            df1.to_csv('data/dfPV.csv', index=False)

    total = time.time() - start
    if total < 60:
        print(f'{"Run Time: "}{total:.2f}{" seconds"}')
    else:
        _min = total // 60
        sec = total - _min*60
        print(f'{"Run Time: "}{_min}{" minutes and "}{sec:.2f}{" seconds."}')





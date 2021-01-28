import datetime
import pandas as pd
import numpy as np


def p2ndate(dt_time):
    if type(dt_time) == list:
        for i in range(len(dt_time)):
            dt_time[i] = int(1e4 * dt_time[i].year + 1e2 * dt_time[i].month + dt_time[i].day)
        return dt_time
    elif type(dt_time) == datetime.datetime:
        return int(1e4 * dt_time.year + 1e2 * dt_time.month + dt_time.day)
    else:
        for i in range(len(dt_time)):
            y = pd.to_datetime(dt_time[i], infer_datetime_format=True).year
            m = pd.to_datetime(dt_time[i], infer_datetime_format=True).month
            d = pd.to_datetime(dt_time[i], infer_datetime_format=True).day
            dt_time[i] = int(1e4 * y + 1e2 * m + d)
        return dt_time


def s2ndate(dt_time):
    if type(dt_time) == pd.Series:
        dt_time = dt_time.fillna(0)
        # dt_time[dt_time.isnull()] = '0'
        temp = pd.Series(dt_time).str.replace('-', '')
        temp = temp.astype(int)
        temp[temp == 0] = None
        return temp
    elif type(dt_time) == str:
        return int(dt_time.replace('-', ''))


def n2pdate(dt_time):
    global conv
    conv = False
    if type(dt_time) == pd.Series:
        dt_time = dt_time.to_list()
    if type(dt_time) == int:
        dt_time = [dt_time]
        conv = True
    y = np.array([np.floor_divide(dt_time, 1e4)]).astype(int)
    m = np.floor_divide((np.array(dt_time) - y * 1e4), 1e2).astype(int)
    d = (dt_time - (y * 1e4 + m * 1e2)).astype(int)
    ndate = np.concatenate((y, m), axis=0)
    ndate = np.concatenate((ndate, d), axis=0)
    pdate = [datetime.datetime(*x) for x in ndate.T]
    if conv is not True:
        return pdate
    elif conv is True:
        return pdate[0]


def s2pdate(dt_time):
    return n2pdate(s2ndate(dt_time))


def p2sdate(dt_time):
    return str(p2ndate(dt_time))
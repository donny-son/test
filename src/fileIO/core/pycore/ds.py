import os
# import numpy as np
# import pandas as pd
from cal import *
from itertools import compress
import pickle

# root = 'Q:\\option\\'
# userpath = "C:\\Users\\user2\\Documents\\" # should change by user
# filepath = userpath + "fileIO\\market\\"

def setpath(ds, reg):
    global fpath
    ext = '.dta'
    if ds is 'mcr':
        fpath = filepath + reg + str(0) + '\\'
        ext = '.pkl'
    if ds is 'stk':
        fpath = filepath + reg + str(1) + '\\'
        ext = '.pkl'
    if ds is 'etf':
        fpath = filepath + reg + str(2) + '\\'
        ext = '.pkl'
    if ds is 'opt':
        # fpath = 'Q:\\option\\'
        fpath = filepath + reg + str(3) +'\\'
        ext = '.pkl'
    if ds is 'opt2':
        fpath = 'Q:\\option2\\'
        ext = '.pkl'
    if ds is 'fun':
        fpath = 'Q:\\fundamental\\' + reg + '\\'
    if ds is 'univ':
        fpath = 'Q:\\univ\\' + reg + '\\'
        ext = '.pkl'
    if ds is 'cache':
        fpath = 'Q:\\cache\\' + reg + '\\'
        ext = '.pkl'
    if ds is 'xref':
        fpath = 'Q:\\xref\\' + reg + '\\'
    return fpath, ext


def fetchdf(ds, reg, dtb, dte=None, cols=None):
    dtb2 = p2ndate(n2pdate(dtb) - datetime.timedelta(days=4))
    if dte is None:
        dte = dtb
    if cols is not None:
        if ds == 'stk':
            cols0 = list(set(cols + ['gvkey', 'iid', 'date']) - {'ret', 'retx'})
            if reg is 'us':
                cols_load = list(set(cols0 + ['ajexdi', 'cheqv', 'trfd', 'prccd', 'cshoc', 'shortint']))
            else:
                cols_load = list(set(cols0 + ['ajexdi', 'cheqv', 'trfd', 'prccd', 'cshoc']))
        else: # elif ds == 'etf':
            cols_load = list(set(cols + ['date']))
    else:
        cols_load = None
    fpath, ext = setpath(ds, reg)
    flist = []
    for r, d, f in os.walk(fpath):
        for file in f:
            if ext in file:
                flist.append(os.path.splitext(file)[0])
    idx = np.asarray(flist, dtype=np.int)
    idx = (idx >= dtb2) & (idx <= dte)
    # idx = (idx >= dtb) & (idx <= dte)
    flist = list(compress(flist, idx))

    # core production
    if len(flist) > 0:
        dfl = {}
        i = 0
        for f in flist:
            if 'dta' in ext:
                dfl[i] = pd.read_stata(fpath + f + '.dta', columns=cols_load)
            elif 'pkl' in ext:
                if cols is None:
                    dfl[i] = pd.read_pickle(fpath + f + '.pkl')
                else:
                    dfl[i] = pd.read_pickle(fpath + f + '.pkl').loc[:,cols_load]
            i = i + 1
        df = pd.concat(dfl, ignore_index=True, sort=False)
        df = df.reset_index(drop='True')
        try:
            df['date'] = df.date.apply(lambda x: x.to_pydatetime().strftime('%Y%m%d'))
        except:
            try:
                df['date'] = df.date.apply(lambda x: x.strftime('%Y%m%d'))
            except:
                pass

        # post production
        if ds == 'stk':
            # adjust shrs and prcs
            df = df.sort_values(by=['gvkey', 'iid', 'date'])
            adj0 = df[{'gvkey', 'iid', 'ajexdi'}].groupby(['gvkey', 'iid']).first()
            adj0 = adj0.rename(columns={'ajexdi': 'cajexdi'})
            df = df.merge(adj0, how='left', on=['gvkey', 'iid'])

            prcs = [c for c in df.columns if 'prc' in c]
            shrs = [c for c in df.columns if 'cshoc' in c or 'shortint' in c]

            for c in prcs:
                df[c + '_adj'] = df[c] / df.cajexdi
            for c in shrs:
                df[c + '_adj'] = df[c] * df.cajexdi

            # calc ret and retx
            df['cheqv'] = df['cheqv'].fillna(0)
            df['trfd'] = df['trfd'].fillna(1)
            grpf = df[{'gvkey', 'iid', 'date'}].groupby(['gvkey', 'iid']).first()
            grpf['grpf'] = 0
            df = df.merge(grpf, how='left', on=['gvkey', 'iid', 'date'])
            df['grpf'] = df['grpf'].fillna(1)
            df['grpf'] = df['grpf'].replace(0, np.nan)
            df['ret'] = (df.prccd_adj * df.trfd * df.grpf) / (df.prccd_adj * df.trfd).shift() - 1
            df['retx'] = ((df.prccd_adj + df.cheqv) * df.grpf) / df.prccd_adj.shift() - 1
            # df['ret'].loc = df['retx']
            df = df.drop(['grpf', 'cajexdi'], axis=1)

        df = df.loc[s2ndate(df.date) >= dtb, :]
        df = df.reset_index(drop='True')
        try:
            df = df.drop(columns=['index'])
        except:
            pass
        if cols is not None:
            df = df[cols]

        return df

    else:
        print('no data is available!')

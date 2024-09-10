#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 16:44:40 2024

@author: htelg
"""

import pathlib as pl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# import plt_tools
import atmPy.tools.plt_tool_kit as plt_tools

def load_logs(path2logs = '/home/grad/htelg/.processlogs/',
              nodays = 9, verbose = False):
    p2fl = pl.Path(path2logs)
    duration = pd.to_timedelta(nodays,'d')
    end = pd.Timestamp.now()
    start = end - duration
    
    products = {}
    out_of_date = []
    for p2f in p2fl.glob('*'):
        if verbose:
            print(f'path2logfile: {p2f}')
        # print(p2f.name)
        if p2f.is_dir():
            continue
        df = pd.read_csv(p2f, index_col=0)
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        df = df.truncate(start, end)
        if df.shape[0] == 0:
            print(f'{p2f.name} has no valid entrys in the last {nodays} days!!')  
            out_of_date.append(p2f)
            continue
        df.loc[end] = np.nan
        df['no_data'] = np.nan
        # idx_last = df.index[-2:]
        try:
            idx_last = df[df.success>0].index[-1]
        except IndexError:
            print(f'{p2f.name} has no valid entry, skip')
            continue
        df.loc[idx_last:df.index[-1],'no_data'] = df.loc[:,['error', 'success', 'warning']].max().max()/2
        # break
        products[p2f.name] = df
    return Status(products, out_of_date)

class Status(object):
    def __init__(self, log, out_of_date):
        self.log = log
        self.out_of_date = out_of_date
        
    def plot(self):
        products = self.log
        f,aa = plt.subplots(len(products), sharex= True, gridspec_kw={'hspace': 0})
        
        prodlist = list(products.keys())
        prodlist.sort(key=lambda x: products[x].success.replace(0, np.nan).dropna().index[-1],
                      reverse=False)
        for e, prod in enumerate(prodlist):
            a = aa[e]
            df = products[prod]
            assert(df.success.sum()>0), 'no data!!!'
            df.success.plot(ax = a, color = 'green', marker = '.')
            df.warning.plot(ax = a, marker = '.')
            df.error.plot(ax = a, marker = '.')
    
            bbox = dict(boxstyle = 'round', fc = [1,1,1,0.7])
            # txt = f'{df.iloc[0].subprocess}\n{df.iloc[0].server}'
            txt = f"{prod.replace('.log','')}\n{df.iloc[0].server.replace('.cmdl.noaa.gov','')}"
            a.text(0.05, 0.9, txt, transform = a.transAxes, va = 'top', bbox = bbox)
            df.no_data.plot(ax = a, color = 'red', lw = 10)
            g = a.get_lines()[-1]
            g.set_solid_capstyle('butt')
            
        # for e,a in enumerate(aa):
            now = pd.Timestamp.now()
            if e == 0:
                text = f'{now.hour:02d}:{now.minute:02d}'
            else:
                text = None
            
            ty = df.loc[:,['error', 'success', 'warning']].max().max() *1.2
            plt_tools.markers.add_position_of_interest2axes(a, x = now, 
                                                            text = text,
                                                            text_pos = (now, ty),
                                                            kwargs=dict(ls = '--', color = 'black'))
            plt_tools.markers.add_position_of_interest2axes(a, x = pd.Timestamp.now().date(), 
                                                            kwargs=dict(ls = '--', color = 'black'))
            for i in [1,2,3,7]: 
                if e == 0:
                    text = f'day {i}'
                else:
                    text = None
                x = pd.Timestamp.now().date() - pd.to_timedelta(i,'d')
                plt_tools.markers.add_position_of_interest2axes(a, x = x, 
                                                                text = text, 
                                                                text_pos=(x,ty),
                                                                kwargs=dict(ls = '--', color = 'black'))
        leg = aa[-1].legend(loc = (1, 0),
                          bbox_to_anchor=[1.01, 0],
                          )
        # leg.set_bbox_to_anchor([0.5,0.5])
        a = aa[-1]
        a.set_xlabel('')
        f.tight_layout()
        f.autofmt_xdate()
        return f,aa,leg#, tp_t
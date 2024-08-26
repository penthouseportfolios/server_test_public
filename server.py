import pandas as pd
import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt
import mplfinance as mpf
import datetime
import time as tt
import matplotlib.pyplot as plt
from tkinter import *
from tkinter import ttk
from matplotlib.backends.backend_tkagg import (
    FigureCanvasTkAgg, NavigationToolbar2Tk)
import threading
import warnings
from tkinter import messagebox
from operator import itemgetter
from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm
from statistics import mean
import matplotlib.dates as mdates
from pywddff.pywddff import modwt
import mysql.connector
import socket
warnings.filterwarnings("ignore")
  
class ServerTrader:

    def __init__(self):
        self.group = input('Which group would you like to run? ')
        self.pairs = self.get_group_pairs()
        self.machine_allocation = input(f'How much would you like to allocate to group {self.group}? ')
        allocation_string = '${:,.2f}'.format(float(round(self.machine_allocation,2)))
        print (f'Starting group {self.group} with ${allocation_string}')
        self.market_open = False
        while True:
            print (datetime.datetime.now())
            if datetime.datetime.now().hour == 9 and  datetime.datetime.now().minute == 29:
                self.market_open = True
            elif datetime.datetime.now().hour == 15 and  datetime.datetime.now().minute == 59:
                self.market_open = False
            if self.market_open == True:
                trades = []
                for i in self.tickers:
                    self.update_price(i)
                    new_trades = self.check_for_trades(i)
                    if len(new_trades) > 0:
                        for i in new_trades:
                            trades.append(i)
                if len(trades) > 0:
                    for j in trades:
                        self.send_trade(j)
            if datetime.datetime.now().hour == 1 and  datetime.datetime.now().minute == 00:
                self.reset_data()
    def reset_data(self):
        pass

    def send_trade(self,trade):
        count = 1
        while count < 4:
            try:
                self.c.send(trade.encode())
                print (f'{dt.now()}Trade Sent.      Details: {trade}')
                count += 5
            except:
                print (f'{dt.now()}Trade Send Failure. Attempting to reconnect({count})...')
                self.connect_websocket()
                tt.sleep(5)
            count += 1
    def disconnect_websocket(self):
        self.c.close()
        print ('Disconencted from',str(self.addr))  
    def connect_websocket(self):
        self.host = 'local host'
        self.port = 9000

        self.websock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.websock.bind(('',self.port))
        self.websock.listen(3)
        print ('Connecting...')
        self.c,self.addr = self.websock.accept()
        print ('Connected to',str(self.addr))
    def check_for_trades(self,pair):
        print (f'{datetime.datetime.now()} Checking {len(self.pairs)} Pairs')
        trades = []
        for pair in self.pairs:
            #state = self.state_dict[pair]
            symbols = pair.split()
            x_symbol = symbols[0]
            y_symbol = symbols[1]
            state = self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'].values[0]

            self.form_spread(x_symbol,y_symbol)
            beta = self.pairs_beta[pair]
            cointegrated = bool(self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Cointegrated'].values[0])

            pair_std = self.pairs_std[pair]
            pair_mean = self.pairs_mean[pair]

            upper = pair_mean+(2*pair_std)
            upper_exit = pair_mean+(0.5*pair_std)
            lower = pair_mean-(2*pair_std)
            lower_exit = pair_mean-(0.5*pair_std)

            allocation = self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Allocation'].values[0]

            x_data = self.data[self.req_ids[x_symbol]]
            x_last = x_data[len(x_data)-1][4]

            y_data = self.data[self.req_ids[y_symbol]]
            y_last = y_data[len(y_data)-1][4]

            spread = self.pairs_spread[pair][len(self.pairs_spread[pair])-1]

            self.trade_log(f'Checking: {pair} State: {state} Spread: {spread}')

            if state == 'CLOSED':
                if spread >= upper or spread <= lower:
                    if self.dont_open_anymore == False:                 
                        x_cash = allocation/((1/beta)+1)
                        y_cash = (1/beta)*x_cash

                        x_shares = round(x_cash/x_last,0)
                        y_shares = round(y_cash/y_last,0)

                        if spread >= upper:
                            trades.append(f'BUY {x_shares} {x_symbol}')
                            trades.append(f'SELL {y_shares} {y_symbol}')

                            self.state_dict.update({pair:'UPPER'})
                            self.state_dict[pair] = 'UPPER'
                            self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'] = 'UPPER'
                        elif spread <= lower:
                            trades.append(f'SELL {x_shares} {x_symbol}')
                            trades.append(f'BUY {y_shares} {y_symbol}')

                            self.state_dict.update({pair:'LOWER'})
                            self.state_dict[pair] = 'LOWER'
                            self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'] = 'LOWER'
            else:
                x_shares = round(abs(self.positions_dict[x_symbol][0]),0)
                y_shares = round(abs(self.positions_dict[y_symbol][0]),0)
                if x_shares == 0 and y_shares == 0:
                    self.state_dict.update({pair:'CLOSED'})
                    self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'] = 'CLOSED'
                else:
                    if state == 'UPPER':
                        if spread <= upper_exit:
                            if x_shares != 0:
                                trades.append(f'SELL {x_shares} {x_symbol}')
                            if y_shares != 0: 
                                trades.append(f'BUY {y_shares} {y_symbol}')

                            self.state_dict.update({pair:'CLOSED'})
                            self.state_dict[pair] = 'CLOSED'
                            self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'] = 'CLOSED'
                    elif state == 'LOWER':
                        if spread >= lower_exit:
                            if x_shares != 0:
                                trades.append(f'BUY {x_shares} {x_symbol}')
                            if y_shares != 0: 
                                trades.append(f'SELL {y_shares} {y_symbol}')


                            self.state_dict.update({pair:'CLOSED'})
                            self.state_dict[pair] = 'CLOSED'
                            self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'] = 'CLOSED'
            self.data_updated.update({pair:0})
            self.pairs_df.loc[(self.pairs_df['X Symbol'] == x_symbol) & (self.pairs_df['Y Symbol'] == y_symbol), 'Position'] = self.state_dict[pair]
            print (f'{len(trades)} Trades Found')
            return trades

    def form_spread(self,x,y):
        x_id = self.req_ids[x]
        y_id = self.req_ids[y]
        x_data = self.data[x_id]
        y_data = self.data[y_id]

        if x_data[len(x_data)-1][0] == y_data[len(y_data)-1][0]:
            if len(x_data) > len(y_data):
                x_data = x_data[-len(y_data):]
            if len(x_data) < len(y_data):
                y_data = y_data[-len(x_data):]

            pair = f'{x} {y}'

            y_prices = [i[4] for i in y_data]
            x_prices = [i[4] for i in x_data]

            if len(self.pairs_spread[pair]) == 0:

                y_train_prices = np.array(self.train_df[y].values)
                x_train_prices = np.array(self.train_df[x].values)

                y_train_prices = y_train_prices[-4096:]
                x_train_prices = x_train_prices[-4096:]

                y_coeffs = [x[1] for x in modwt(y_train_prices,'sym22',1,True)]
                x_coeffs = [x[1] for x in modwt(x_train_prices,'sym22',1,True)]

                reg = sm.OLS(y_coeffs,x_coeffs)
                model = reg.fit()
                beta_1 = model.params[0]

                self.pairs_beta.update({pair:beta_1})  

                for x,y in zip(x_prices,y_prices):
                    ts = y - (beta_1*x)
                    self.pairs_spread[pair].append(ts)

                self.pairs_mean.update({pair:np.mean(self.pairs_spread[pair])})
                self.pairs_std.update({pair:np.std(self.pairs_spread[pair])})

                spread = [x for x in self.pairs_spread[pair]]

                self.pairs_df.loc[(self.pairs_df['X Symbol'] == x) & (self.pairs_df['Y Symbol'] == y), 'Cointegrated'] = self.check_for_stationarity(spread)
                self.pairs_df.loc[(self.pairs_df['X Symbol'] == x) & (self.pairs_df['Y Symbol'] == y), 'Current Beta'] = model.params[0]
         
            else:
                beta_1 = self.pairs_beta[pair]
                tick = y_prices[len(y_prices)-1] + (x_prices[len(x_prices)-1] * -beta_1)
                self.pairs_spread[pair].append(tick)
    def update_price(self,ticker):
        ### req updated price from sql and add to tickers data
        price = 0
        print (f'{ticker} Price Updated to {price}')
        return price
    def get_group_pairs(self):
        ### req tickers from SQL table
        ### sql self.group
        pairs = []
        return pairs
    def get_historical_data(self,ticker):
        ### req data from SQL table
        prices = []
        return prices
if __name__ == '__main__':
    ServerTrader()
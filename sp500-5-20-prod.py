# -*- coding: utf-8 -*-
"""
Created in 2023

@author: Quant Galore
"""

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

import yfinance as yf
import pandas as pd
import numpy as np
import pmdarima as pm
import databento as db
import requests
import time
import matplotlib.pyplot as plt

def call_or_put(prediction):
    
    if prediction > 0:
        return "C"
    elif prediction < 0:
        return "P"

# polygon.io is where we get the historical underlying 1-minute price, so you'll need to sign up for an API Key
# we use the free plan which is limited to 5 requests per minute, so there is a 60 second timeout every 5th request

polygon_api_key = "your polygon.io api key, use QUANTGALORE for 10% off"

# we get the valid dates where the market was open

exchange = 'NYSE'
calendar = get_calendar(exchange)

tomorrow = (datetime.today() + timedelta(days = 1)).strftime("%Y-%m-%d")

VIX_Data = yf.download("^VIX", start="2023-01-01", end=tomorrow)
VIX_Data.index = VIX_Data.index.tz_localize("US/Eastern").tz_convert("US/Eastern")


start_date = (datetime.today() - timedelta(days = 50)).strftime("%Y-%m-%d")
trade_date = datetime.today().strftime("%Y-%m-%d")

# spy #

spy_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

spy_bars = pd.json_normalize(spy_bar_request["results"])
spy_bars["t"] = pd.to_datetime(spy_bars["t"], unit = "ms", utc = True)
spy_bars = spy_bars.set_index("t")
spy_bars.index = spy_bars.index.tz_convert("US/Eastern")

SPY_Open = spy_bars.copy()

# xlc #

xlc_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLC/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlc_bars = pd.json_normalize(xlc_bar_request["results"])
xlc_bars["t"] = pd.to_datetime(xlc_bars["t"], unit = "ms", utc = True)
xlc_bars = xlc_bars.set_index("t")
xlc_bars.index = xlc_bars.index.tz_convert("US/Eastern")

XLC_Open = xlc_bars.copy()

# xly #

xly_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLY/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xly_bars = pd.json_normalize(xly_bar_request["results"])
xly_bars["t"] = pd.to_datetime(xly_bars["t"], unit = "ms", utc = True)
xly_bars = xly_bars.set_index("t")
xly_bars.index = xly_bars.index.tz_convert("US/Eastern")

XLY_Open = xly_bars.copy()

# xlp #

xlp_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLP/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlp_bars = pd.json_normalize(xlp_bar_request["results"])
xlp_bars["t"] = pd.to_datetime(xlp_bars["t"], unit = "ms", utc = True)
xlp_bars = xlp_bars.set_index("t")
xlp_bars.index = xlp_bars.index.tz_convert("US/Eastern")

XLP_Open = xlp_bars.copy()

# xle #

xle_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLE/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xle_bars = pd.json_normalize(xle_bar_request["results"])
xle_bars["t"] = pd.to_datetime(xle_bars["t"], unit = "ms", utc = True)
xle_bars = xle_bars.set_index("t")
xle_bars.index = xle_bars.index.tz_convert("US/Eastern")

XLE_Open = xle_bars.copy()

# xlf #

xlf_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLF/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlf_bars = pd.json_normalize(xlf_bar_request["results"])
xlf_bars["t"] = pd.to_datetime(xlf_bars["t"], unit = "ms", utc = True)
xlf_bars = xlf_bars.set_index("t")
xlf_bars.index = xlf_bars.index.tz_convert("US/Eastern")

XLF_Open = xlf_bars.copy()

# xlv #

xlv_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLV/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlv_bars = pd.json_normalize(xlv_bar_request["results"])
xlv_bars["t"] = pd.to_datetime(xlv_bars["t"], unit = "ms", utc = True)
xlv_bars = xlv_bars.set_index("t")
xlv_bars.index = xlv_bars.index.tz_convert("US/Eastern")

XLV_Open = xlv_bars.copy()

# xli #

xli_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLI/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xli_bars = pd.json_normalize(xli_bar_request["results"])
xli_bars["t"] = pd.to_datetime(xli_bars["t"], unit = "ms", utc = True)
xli_bars = xli_bars.set_index("t")
xli_bars.index = xli_bars.index.tz_convert("US/Eastern")

XLI_Open = xli_bars.copy()

# xlb #

xlb_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLB/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlb_bars = pd.json_normalize(xlb_bar_request["results"])
xlb_bars["t"] = pd.to_datetime(xlb_bars["t"], unit = "ms", utc = True)
xlb_bars = xlb_bars.set_index("t")
xlb_bars.index = xlb_bars.index.tz_convert("US/Eastern")

XLB_Open = xlb_bars.copy()

# xlre #

xlre_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLRE/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlre_bars = pd.json_normalize(xlre_bar_request["results"])
xlre_bars["t"] = pd.to_datetime(xlre_bars["t"], unit = "ms", utc = True)
xlre_bars = xlre_bars.set_index("t")
xlre_bars.index = xlre_bars.index.tz_convert("US/Eastern")

XLRE_Open = xlre_bars.copy()

# xlk #

xlk_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLK/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlk_bars = pd.json_normalize(xlk_bar_request["results"])
xlk_bars["t"] = pd.to_datetime(xlk_bars["t"], unit = "ms", utc = True)
xlk_bars = xlk_bars.set_index("t")
xlk_bars.index = xlk_bars.index.tz_convert("US/Eastern")

XLK_Open = xlk_bars.copy()

# xlu #

xlu_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/XLU/range/1/day/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

xlu_bars = pd.json_normalize(xlu_bar_request["results"])
xlu_bars["t"] = pd.to_datetime(xlu_bars["t"], unit = "ms", utc = True)
xlu_bars = xlu_bars.set_index("t")
xlu_bars.index = xlu_bars.index.tz_convert("US/Eastern")

XLU_Open = xlu_bars.copy()

# - #

VIX_Open = VIX_Data[(VIX_Data.index >= pd.to_datetime(start_date).tz_localize("US/Eastern")) & (VIX_Data.index <= pd.to_datetime(trade_date).tz_localize("US/Eastern"))]

etf_open_list = [SPY_Open, XLC_Open, XLY_Open, XLP_Open, XLE_Open, XLF_Open, XLV_Open, XLI_Open, XLB_Open, XLRE_Open, XLK_Open, XLU_Open]

for etf_open in etf_open_list:
    etf_open["returns"] = ((etf_open["o"] - etf_open["c"].shift(1)) / etf_open["c"].shift(1)).fillna(0)
    
intraday_spy_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/{start_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

intraday_spy_bars = pd.json_normalize(intraday_spy_bar_request["results"])
intraday_spy_bars["t"] = pd.to_datetime(intraday_spy_bars["t"], unit = "ms", utc = True)
intraday_spy_bars = intraday_spy_bars.set_index("t")
intraday_spy_bars.index = intraday_spy_bars.index.tz_convert("US/Eastern")

SPY_Intraday = intraday_spy_bars.copy()
SPY_Intraday["date"] = SPY_Intraday.index.date
SPY_Intraday["hour"] = SPY_Intraday.index.hour
SPY_Intraday["minute"] = SPY_Intraday.index.minute

intraday_return_list = []

for intraday_date in SPY_Intraday["date"].drop_duplicates().values:
    
    if intraday_date == SPY_Intraday["date"].iloc[-1]:
        
        intraday_data = yf.download("SPY", start = datetime.today().strftime("%Y-%m-%d"), end = (datetime.today() + timedelta(days = 1)).strftime("%Y-%m-%d"), interval = "1m")
        trading_session = intraday_data[intraday_data.index.time <= pd.Timestamp("15:55").time()].copy()
        
        trading_session["returns"] = trading_session["Adj Close"].pct_change().fillna(0)
        trading_session["cumulative_returns"] = trading_session["returns"].cumsum()
        
        intraday_return = trading_session["cumulative_returns"].iloc[-1]
        intraday_return_list.append(intraday_return)
        
    else:
    
        intraday_data = SPY_Intraday[SPY_Intraday["date"] == intraday_date].copy()
        trading_session = intraday_data[(intraday_data.index.time >= pd.Timestamp("9:30").time()) & (intraday_data.index.time < pd.Timestamp("15:55").time())].copy()
        trading_session["returns"] = trading_session["c"].pct_change().fillna(0)
        trading_session["cumulative_returns"] = trading_session["returns"].cumsum()
        
        intraday_return = trading_session["cumulative_returns"].iloc[-1]
        intraday_return_list.append(intraday_return)

Merged = pd.concat([XLC_Open.add_prefix("Communications_"),
                    XLY_Open.add_prefix("ConsumerDiscretionary_"),
                    XLP_Open.add_prefix("ConsumerStaples_"), XLE_Open.add_prefix("Energy_"),
                    XLF_Open.add_prefix("Financials_"), XLV_Open.add_prefix("Healthcare_"),
                    XLI_Open.add_prefix("Industrials_"), XLB_Open.add_prefix("Materials_"),
                    XLRE_Open.add_prefix("RealEstate_"),XLK_Open.add_prefix("Technology_"),
                    XLU_Open.add_prefix("Utilities_"), SPY_Open.add_prefix("SP500_"), (VIX_Open/100).add_prefix("VIX_")], axis = 1).dropna()

Merged["intraday_returns"] = intraday_return_list

Merged_Returns = Merged[["Communications_returns","ConsumerDiscretionary_returns", "ConsumerStaples_returns", "Energy_returns",
                         "Financials_returns", "Healthcare_returns", "Industrials_returns",
                         "Materials_returns","RealEstate_returns", "Technology_returns",
                         "Utilities_returns", "VIX_Open","intraday_returns","SP500_returns"]].copy()

Shifted_Merged_Returns = Merged_Returns.copy()
Shifted_Merged_Returns["SP500_returns"] = Shifted_Merged_Returns["SP500_returns"].shift(-1).dropna()
Shifted_Merged_Returns = round(Shifted_Merged_Returns.dropna() * 100, 2)

Five_Day_Merged_Returns = Shifted_Merged_Returns.tail(5).copy()
Five_Day_Training_Dataset = Five_Day_Merged_Returns.copy()

Twenty_Day_Merged_Returns = Shifted_Merged_Returns.tail(20).copy()
Twenty_Day_Training_Dataset = Twenty_Day_Merged_Returns.copy()

if len(Twenty_Day_Training_Dataset) < 20:
    raise Exception

Five_Day_X = Five_Day_Training_Dataset.drop("SP500_returns", axis = 1).values
Five_Day_Y = Five_Day_Training_Dataset["SP500_returns"].values

Twenty_Day_X = Twenty_Day_Training_Dataset.drop("SP500_returns", axis = 1).values
Twenty_Day_Y = Twenty_Day_Training_Dataset["SP500_returns"].values

# train the model

Five_Day_Model = pm.arima.auto_arima(y=Five_Day_Y, X=Five_Day_X)
Twenty_Day_Model = pm.arima.auto_arima(y=Twenty_Day_Y, X=Twenty_Day_X)

#### Production 

Production_Data = round(Merged_Returns.tail(1).copy().drop("SP500_returns", axis = 1) * 100, 2)

SP500_Production_Dataset = Production_Data.copy().values

Five_Day_Prediction = Five_Day_Model.predict(X = SP500_Production_Dataset, n_periods = 1)
Twenty_Day_Prediction = Twenty_Day_Model.predict(X = SP500_Production_Dataset, n_periods = 1)

Average_Prediction = (Five_Day_Prediction[0] + Twenty_Day_Prediction[0]) / 2

print(f"Prediction on {Production_Data.index[0]}: {Average_Prediction}")
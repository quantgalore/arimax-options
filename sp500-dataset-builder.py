# -*- coding: utf-8 -*-
"""
Created in 2023

@author: Quant Galore
"""

from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import numpy as np

import sqlalchemy
import mysql.connector

start_date = "2023-01-01"
end_date = (datetime.today() + timedelta(days = 1)).strftime("%Y-%m-%d")

SPY = yf.download("SPY", start=start_date, end=end_date)

XLC = yf.download("XLC", start=start_date, end=end_date)
XLY = yf.download("XLY", start=start_date, end=end_date)
XLP = yf.download("XLP", start=start_date, end=end_date) 
XLE = yf.download("XLE", start=start_date, end=end_date) 
XLF = yf.download("XLF", start=start_date, end=end_date)
XLV = yf.download("XLV", start=start_date, end=end_date) 
XLI = yf.download("XLI", start=start_date, end=end_date)
XLB = yf.download("XLB", start=start_date, end=end_date)
XLRE = yf.download("XLRE", start=start_date, end=end_date)
XLK = yf.download("XLK", start=start_date, end=end_date) 
XLU = yf.download("XLU", start=start_date, end=end_date)

SPY["Adjustment Multiplier"] = SPY["Adj Close"] / SPY["Close"]

XLC["Adjustment Multiplier"] = XLC["Adj Close"] / XLC["Close"]
XLY["Adjustment Multiplier"] = XLY["Adj Close"] / XLY["Close"]
XLP["Adjustment Multiplier"] = XLP["Adj Close"] / XLP["Close"]
XLE["Adjustment Multiplier"] = XLE["Adj Close"] / XLE["Close"]
XLF["Adjustment Multiplier"] = XLF["Adj Close"] / XLF["Close"]
XLV["Adjustment Multiplier"] = XLV["Adj Close"] / XLV["Close"]
XLI["Adjustment Multiplier"] = XLI["Adj Close"] / XLI["Close"]
XLB["Adjustment Multiplier"] = XLB["Adj Close"] / XLB["Close"]
XLRE["Adjustment Multiplier"] = XLRE["Adj Close"] / XLRE["Close"]
XLK["Adjustment Multiplier"] = XLK["Adj Close"] / XLK["Close"]
XLU["Adjustment Multiplier"] = XLU["Adj Close"] / XLU["Close"]

SPY["Adj Open"] = SPY["Open"] * SPY["Adjustment Multiplier"]

XLC["Adj Open"] = XLC["Open"] * XLC["Adjustment Multiplier"]
XLY["Adj Open"] = XLY["Open"] * XLY["Adjustment Multiplier"]
XLP["Adj Open"] = XLP["Open"] * XLP["Adjustment Multiplier"]
XLE["Adj Open"] = XLE["Open"] * XLE["Adjustment Multiplier"]
XLF["Adj Open"] = XLF["Open"] * XLF["Adjustment Multiplier"]
XLV["Adj Open"] = XLV["Open"] * XLV["Adjustment Multiplier"]
XLI["Adj Open"] = XLI["Open"] * XLI["Adjustment Multiplier"]
XLB["Adj Open"] = XLB["Open"] * XLB["Adjustment Multiplier"]
XLRE["Adj Open"] = XLRE["Open"] * XLRE["Adjustment Multiplier"]
XLK["Adj Open"] = XLK["Open"] * XLK["Adjustment Multiplier"]
XLU["Adj Open"] = XLU["Open"] * XLU["Adjustment Multiplier"]

SPY["returns"] = ((SPY["Adj Open"] - SPY["Adj Close"].shift(1)) / SPY["Adj Close"].shift(1)).fillna(0)

XLC["returns"] = ((XLC["Adj Open"] - XLC["Adj Close"].shift(1)) / XLC["Adj Close"].shift(1)).fillna(0)
XLY["returns"] = ((XLY["Adj Open"] - XLY["Adj Close"].shift(1)) / XLY["Adj Close"].shift(1)).fillna(0)
XLP["returns"] = ((XLP["Adj Open"] - XLP["Adj Close"].shift(1)) / XLP["Adj Close"].shift(1)).fillna(0)
XLE["returns"] = ((XLE["Adj Open"] - XLE["Adj Close"].shift(1)) / XLE["Adj Close"].shift(1)).fillna(0)
XLF["returns"] = ((XLF["Adj Open"] - XLF["Adj Close"].shift(1)) / XLF["Adj Close"].shift(1)).fillna(0)
XLV["returns"] = ((XLV["Adj Open"] - XLV["Adj Close"].shift(1)) / XLV["Adj Close"].shift(1)).fillna(0)
XLI["returns"] = ((XLI["Adj Open"] - XLI["Adj Close"].shift(1)) / XLI["Adj Close"].shift(1)).fillna(0)
XLB["returns"] = ((XLB["Adj Open"] - XLB["Adj Close"].shift(1)) / XLB["Adj Close"].shift(1)).fillna(0)
XLRE["returns"] = ((XLRE["Adj Open"] - XLRE["Adj Close"].shift(1)) / XLRE["Adj Close"].shift(1)).fillna(0)
XLK["returns"] = ((XLK["Adj Open"] - XLK["Adj Close"].shift(1)) / XLK["Adj Close"].shift(1)).fillna(0)
XLU["returns"] = ((XLU["Adj Open"] - XLU["Adj Close"].shift(1)) / XLU["Adj Close"].shift(1)).fillna(0)

Merged = pd.concat([XLC.add_prefix("Communications_"),
                    XLY.add_prefix("ConsumerDiscretionary_"),
                    XLP.add_prefix("ConsumerStaples_"), XLE.add_prefix("Energy_"),
                    XLF.add_prefix("Financials_"), XLV.add_prefix("Healthcare_"),
                    XLI.add_prefix("Industrials_"), XLB.add_prefix("Materials_"),
                    XLRE.add_prefix("RealEstate_"),XLK.add_prefix("Technology_"),
                    XLU.add_prefix("Utilities_"), SPY.add_prefix("SP500_")], axis = 1).dropna()

Merged_Returns = Merged[["Communications_returns","ConsumerDiscretionary_returns", "ConsumerStaples_returns", "Energy_returns",
                         "Financials_returns", "Healthcare_returns", "Industrials_returns",
                         "Materials_returns","RealEstate_returns", "Technology_returns",
                         "Utilities_returns","SP500_returns"]].copy()


Shifted_Merged_Returns = Merged_Returns.copy()
Shifted_Merged_Returns["SP500_returns"] = Shifted_Merged_Returns["SP500_returns"].shift(-1).dropna()
Shifted_Merged_Returns = round(Shifted_Merged_Returns.dropna() * 100, 2)

One_Month_Merged_Returns = Shifted_Merged_Returns.tail(30).copy()

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')

# If this is your first run, there is no database to drop, so skip this deletion line and run the ".to_sql" first.
# On subsequent runs, you run the code as-is, dropping the database each time.

with engine.connect() as conn:
    result = conn.execute(sqlalchemy.text('DROP TABLE sp500_sector_raw_timeseries_30d'))

One_Month_Merged_Returns.to_sql("sp500_sector_raw_timeseries_30d", con = engine, if_exists = "append")


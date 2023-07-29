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

# This file uses today's data, so it does not include the S&P 500 returns, since we want to use today's data to predict tomorrow's return.

start_date = (datetime.today() - timedelta(days = 4)).strftime("%Y-%m-%d")
end_date = (datetime.today() + timedelta(days = 1)).strftime("%Y-%m-%d")

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

XLC["returns"] = ((XLC["Open"] - XLC["Adj Close"].shift(1)) / XLC["Adj Close"].shift(1)).fillna(0)
XLY["returns"] = ((XLY["Open"] - XLY["Adj Close"].shift(1)) / XLY["Adj Close"].shift(1)).fillna(0)
XLP["returns"] = ((XLP["Open"] - XLP["Adj Close"].shift(1)) / XLP["Adj Close"].shift(1)).fillna(0)
XLE["returns"] = ((XLE["Open"] - XLE["Adj Close"].shift(1)) / XLE["Adj Close"].shift(1)).fillna(0)
XLF["returns"] = ((XLF["Open"] - XLF["Adj Close"].shift(1)) / XLF["Adj Close"].shift(1)).fillna(0)
XLV["returns"] = ((XLV["Open"] - XLV["Adj Close"].shift(1)) / XLV["Adj Close"].shift(1)).fillna(0)
XLI["returns"] = ((XLI["Open"] - XLI["Adj Close"].shift(1)) / XLI["Adj Close"].shift(1)).fillna(0)
XLB["returns"] = ((XLB["Open"] - XLB["Adj Close"].shift(1)) / XLB["Adj Close"].shift(1)).fillna(0)
XLRE["returns"] = ((XLRE["Open"] - XLRE["Adj Close"].shift(1)) / XLRE["Adj Close"].shift(1)).fillna(0)
XLK["returns"] = ((XLK["Open"] - XLK["Adj Close"].shift(1)) / XLK["Adj Close"].shift(1)).fillna(0)
XLU["returns"] = ((XLU["Open"] - XLU["Adj Close"].shift(1)) / XLU["Adj Close"].shift(1)).fillna(0)

Merged = pd.concat([XLC.add_prefix("Communications_"),
                    XLY.add_prefix("ConsumerDiscretionary_"),
                    XLP.add_prefix("ConsumerStaples_"), XLE.add_prefix("Energy_"),
                    XLF.add_prefix("Financials_"), XLV.add_prefix("Healthcare_"),
                    XLI.add_prefix("Industrials_"), XLB.add_prefix("Materials_"),
                    XLRE.add_prefix("RealEstate_"),XLK.add_prefix("Technology_"),
                    XLU.add_prefix("Utilities_")], axis = 1).dropna()

Merged_Returns = Merged[["Communications_returns","ConsumerDiscretionary_returns", "ConsumerStaples_returns", "Energy_returns",
                         "Financials_returns", "Healthcare_returns", "Industrials_returns",
                         "Materials_returns","RealEstate_returns", "Technology_returns",
                         "Utilities_returns"]].copy()

Shifted_Merged_Returns = Merged_Returns.copy().tail(1)
Shifted_Merged_Returns = round(Shifted_Merged_Returns.dropna() * 100, 2)

engine = sqlalchemy.create_engine('mysql+mysqlconnector://username:password@database-host-name:3306/database-name')

# If this is your first run, there is no database to drop, so skip this deletion line and run the ".to_sql" first.
# On subsequent runs, you run the code as-is, dropping the database each time.

with engine.connect() as conn:
    result = conn.execute(sqlalchemy.text('DROP TABLE sp500_sector_raw_timeseries_production'))
    
Shifted_Merged_Returns.to_sql("sp500_sector_raw_timeseries_production", con = engine, if_exists = "append")

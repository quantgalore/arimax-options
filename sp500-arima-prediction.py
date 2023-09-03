# -*- coding: utf-8 -*-
"""
Created in 2023

@author: Quant Galore
"""

from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import pmdarima as pm

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

VIX = yf.download("^VIX", start=start_date, end=end_date)

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
                    XLU.add_prefix("Utilities_"), SPY.add_prefix("SP500_"), (VIX/100).add_prefix("VIX_")], axis = 1).dropna()

Merged_Returns = Merged[["Communications_returns","ConsumerDiscretionary_returns", "ConsumerStaples_returns", "Energy_returns",
                         "Financials_returns", "Healthcare_returns", "Industrials_returns",
                         "Materials_returns","RealEstate_returns", "Technology_returns",
                         "Utilities_returns", "VIX_Open","SP500_returns"]].copy()


Shifted_Merged_Returns = Merged_Returns.copy()
Shifted_Merged_Returns["SP500_returns"] = Shifted_Merged_Returns["SP500_returns"].shift(-1).dropna()
Shifted_Merged_Returns = round(Shifted_Merged_Returns.dropna() * 100, 2)

One_Month_Merged_Returns = Shifted_Merged_Returns.tail(30).copy()

X = One_Month_Merged_Returns.drop("SP500_returns", axis = 1)
Y = One_Month_Merged_Returns["SP500_returns"]

# we train the model with the default parameters, refer to the pmdarima docs for further specification

arima_model = pm.arima.auto_arima(y=Y, X=X)    

#### Production 

production_start_date = (datetime.today() - timedelta(days = 4)).strftime("%Y-%m-%d")
production_end_date = (datetime.today() + timedelta(days = 1)).strftime("%Y-%m-%d")

Production_XLC = yf.download("XLC", start=production_start_date, end=production_end_date)
Production_XLY = yf.download("XLY", start=production_start_date, end=production_end_date)
Production_XLP = yf.download("XLP", start=production_start_date, end=production_end_date)
Production_XLE = yf.download("XLE", start=production_start_date, end=production_end_date)
Production_XLF = yf.download("XLF", start=production_start_date, end=production_end_date)
Production_XLV = yf.download("XLV", start=production_start_date, end=production_end_date)
Production_XLI = yf.download("XLI", start=production_start_date, end=production_end_date)
Production_XLB = yf.download("XLB", start=production_start_date, end=production_end_date)
Production_XLRE = yf.download("XLRE", start=production_start_date, end=production_end_date)
Production_XLK = yf.download("XLK", start=production_start_date, end=production_end_date)
Production_XLU = yf.download("XLU", start=production_start_date, end=production_end_date)

Production_VIX = yf.download("^VIX", start=production_start_date, end=production_end_date)

Production_XLC["Adjustment Multiplier"] = Production_XLC["Adj Close"] / Production_XLC["Close"]
Production_XLY["Adjustment Multiplier"] = Production_XLY["Adj Close"] / Production_XLY["Close"]
Production_XLP["Adjustment Multiplier"] = Production_XLP["Adj Close"] / Production_XLP["Close"]
Production_XLE["Adjustment Multiplier"] = Production_XLE["Adj Close"] / Production_XLE["Close"]
Production_XLF["Adjustment Multiplier"] = Production_XLF["Adj Close"] / Production_XLF["Close"]
Production_XLV["Adjustment Multiplier"] = Production_XLV["Adj Close"] / Production_XLV["Close"]
Production_XLI["Adjustment Multiplier"] = Production_XLI["Adj Close"] / Production_XLI["Close"]
Production_XLB["Adjustment Multiplier"] = Production_XLB["Adj Close"] / Production_XLB["Close"]
Production_XLRE["Adjustment Multiplier"] = Production_XLRE["Adj Close"] / Production_XLRE["Close"]
Production_XLK["Adjustment Multiplier"] = Production_XLK["Adj Close"] / Production_XLK["Close"]
Production_XLU["Adjustment Multiplier"] = Production_XLU["Adj Close"] / Production_XLU["Close"]

Production_XLC["Adj Open"] = Production_XLC["Open"] * Production_XLC["Adjustment Multiplier"]
Production_XLY["Adj Open"] = Production_XLY["Open"] * Production_XLY["Adjustment Multiplier"]
Production_XLP["Adj Open"] = Production_XLP["Open"] * Production_XLP["Adjustment Multiplier"]
Production_XLE["Adj Open"] = Production_XLE["Open"] * Production_XLE["Adjustment Multiplier"]
Production_XLF["Adj Open"] = Production_XLF["Open"] * Production_XLF["Adjustment Multiplier"]
Production_XLV["Adj Open"] = Production_XLV["Open"] * Production_XLV["Adjustment Multiplier"]
Production_XLI["Adj Open"] = Production_XLI["Open"] * Production_XLI["Adjustment Multiplier"]
Production_XLB["Adj Open"] = Production_XLB["Open"] * Production_XLB["Adjustment Multiplier"]
Production_XLRE["Adj Open"] = Production_XLRE["Open"] * Production_XLRE["Adjustment Multiplier"]
Production_XLK["Adj Open"] = Production_XLK["Open"] * Production_XLK["Adjustment Multiplier"]
Production_XLU["Adj Open"] = Production_XLU["Open"] * Production_XLU["Adjustment Multiplier"]

Production_XLC["returns"] = ((Production_XLC["Adj Open"] - Production_XLC["Adj Close"].shift(1)) / Production_XLC["Adj Close"].shift(1)).fillna(0)
Production_XLY["returns"] = ((Production_XLY["Adj Open"] - Production_XLY["Adj Close"].shift(1)) / Production_XLY["Adj Close"].shift(1)).fillna(0)
Production_XLP["returns"] = ((Production_XLP["Adj Open"] - Production_XLP["Adj Close"].shift(1)) / Production_XLP["Adj Close"].shift(1)).fillna(0)
Production_XLE["returns"] = ((Production_XLE["Adj Open"] - Production_XLE["Adj Close"].shift(1)) / Production_XLE["Adj Close"].shift(1)).fillna(0)
Production_XLF["returns"] = ((Production_XLF["Adj Open"] - Production_XLF["Adj Close"].shift(1)) / Production_XLF["Adj Close"].shift(1)).fillna(0)
Production_XLV["returns"] = ((Production_XLV["Adj Open"] - Production_XLV["Adj Close"].shift(1)) / Production_XLV["Adj Close"].shift(1)).fillna(0)
Production_XLI["returns"] = ((Production_XLI["Adj Open"] - Production_XLI["Adj Close"].shift(1)) / Production_XLI["Adj Close"].shift(1)).fillna(0)
Production_XLB["returns"] = ((Production_XLB["Adj Open"] - Production_XLB["Adj Close"].shift(1)) / Production_XLB["Adj Close"].shift(1)).fillna(0)
Production_XLRE["returns"] = ((Production_XLRE["Adj Open"] - Production_XLRE["Adj Close"].shift(1)) / Production_XLRE["Adj Close"].shift(1)).fillna(0)
Production_XLK["returns"] = ((Production_XLK["Adj Open"] - Production_XLK["Adj Close"].shift(1)) / Production_XLK["Adj Close"].shift(1)).fillna(0)
Production_XLU["returns"] = ((Production_XLU["Adj Open"] - Production_XLU["Adj Close"].shift(1)) / Production_XLU["Adj Close"].shift(1)).fillna(0)

Production_Merged = pd.concat([Production_XLC.add_prefix("Communications_"),
                    Production_XLY.add_prefix("ConsumerDiscretionary_"),
                    Production_XLP.add_prefix("ConsumerStaples_"), Production_XLE.add_prefix("Energy_"),
                    Production_XLF.add_prefix("Financials_"), Production_XLV.add_prefix("Healthcare_"),
                    Production_XLI.add_prefix("Industrials_"), Production_XLB.add_prefix("Materials_"),
                    Production_XLRE.add_prefix("RealEstate_"), Production_XLK.add_prefix("Technology_"),
                    Production_XLU.add_prefix("Utilities_"), (Production_VIX/100).add_prefix("VIX_")], axis = 1).dropna()

Production_Merged_Returns = Production_Merged[["Communications_returns","ConsumerDiscretionary_returns", "ConsumerStaples_returns", "Energy_returns",
                         "Financials_returns", "Healthcare_returns", "Industrials_returns",
                         "Materials_returns","RealEstate_returns", "Technology_returns",
                         "Utilities_returns", "VIX_Open"]].copy()

Production_Shifted_Merged_Returns = Production_Merged_Returns.copy().tail(1)
Production_Shifted_Merged_Returns = round(Production_Shifted_Merged_Returns.dropna() * 100, 2)

SP500_Production_Dataset = Production_Shifted_Merged_Returns.copy()

TimeSeries_Prediction = arima_model.predict(X = SP500_Production_Dataset, n_periods = 1)

next_day = (SP500_Production_Dataset.index[0] + timedelta(days = 1)).strftime("%Y-%m-%d")

print(f"The model predicts the S&P 500 to return {TimeSeries_Prediction.iloc[0]}% on the open of {next_day}")

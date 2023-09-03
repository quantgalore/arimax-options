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

# databento is the options data source, so you'll need to sign up for an API Key

db_api_key = "databento-api-key"
db_api_client = db.Historical(db_api_key)

# polygon.io is where we get the historical underlying 1-minute price, so you'll need to sign up for an API Key
# we use the free plan which is limited to 5 requests per minute, so there is a 60 second timeout every 5th request

polygon_api_key = "polygon.io-api-key"

# we get the valid dates where the market was open

exchange = 'NYSE'
calendar = get_calendar(exchange)

# the historical data starts at 03/28/23 and is available on a T+1 basis

valid_dates = calendar.schedule(start_date = "2023-04-01", end_date = "2023-08-30").index.strftime("%Y-%m-%d").values

prediction_actual_list = []

start_time = datetime.now()

for trade_date in valid_dates:
    
    day_of_week = pd.to_datetime(trade_date).day_name()
    
    # The system only trades on consecutive weekdays; no Fridays
    
    if day_of_week == "Friday":
        continue

    # always keep the start date to be Jan. 1st, we use .tail(n) to get the last n day's returns

    start_date = "2023-01-01"
    end_date = pd.to_datetime(trade_date) + timedelta(days = 1)
    
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
    
    if len(One_Month_Merged_Returns) < 30:
        break
    
    X = One_Month_Merged_Returns.drop("SP500_returns", axis = 1)
    Y = One_Month_Merged_Returns["SP500_returns"]
    
    # we train the model with the default parameters, refer to the pmdarima docs for further specification
    
    arima_model = pm.arima.auto_arima(y=Y, X=X)    
    
    #### Production 
    
    production_start_date = (pd.to_datetime(trade_date) - timedelta(days = 4)).strftime("%Y-%m-%d")
    production_end_date = (pd.to_datetime(trade_date) + timedelta(days = 1)).strftime("%Y-%m-%d")

    XLC = yf.download("XLC", start=production_start_date, end=production_end_date)
    XLY = yf.download("XLY", start=production_start_date, end=production_end_date)
    XLP = yf.download("XLP", start=production_start_date, end=production_end_date)
    XLE = yf.download("XLE", start=production_start_date, end=production_end_date)
    XLF = yf.download("XLF", start=production_start_date, end=production_end_date)
    XLV = yf.download("XLV", start=production_start_date, end=production_end_date)
    XLI = yf.download("XLI", start=production_start_date, end=production_end_date)
    XLB = yf.download("XLB", start=production_start_date, end=production_end_date)
    XLRE = yf.download("XLRE", start=production_start_date, end=production_end_date)
    XLK = yf.download("XLK", start=production_start_date, end=production_end_date)
    XLU = yf.download("XLU", start=production_start_date, end=production_end_date)
    
    VIX = yf.download("^VIX", start=production_start_date, end=production_end_date)
    
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
                        XLU.add_prefix("Utilities_"), (VIX/100).add_prefix("VIX_")], axis = 1).dropna()
    
    Merged_Returns = Merged[["Communications_returns","ConsumerDiscretionary_returns", "ConsumerStaples_returns", "Energy_returns",
                             "Financials_returns", "Healthcare_returns", "Industrials_returns",
                             "Materials_returns","RealEstate_returns", "Technology_returns",
                             "Utilities_returns", "VIX_Open"]].copy()
    
    Shifted_Merged_Returns = Merged_Returns.copy().tail(1)
    Shifted_Merged_Returns = round(Shifted_Merged_Returns.dropna() * 100, 2)
    
    SP500_Production_Dataset = Shifted_Merged_Returns.copy()
    
    TimeSeries_Prediction = arima_model.predict(X = SP500_Production_Dataset, n_periods = 1)
    
    actual_start_date = (pd.to_datetime(trade_date) - timedelta(days = 4)).strftime("%Y-%m-%d")
    actual_end_date = (pd.to_datetime(trade_date) + timedelta(days = 7)).strftime("%Y-%m-%d")
    
    SPY = yf.download("SPY", start=actual_start_date, end=actual_end_date)
    SPY["Adjustment Multiplier"] = SPY["Adj Close"] / SPY["Close"]
    SPY["Adj Open"] = SPY["Open"] * SPY["Adjustment Multiplier"]
    SPY["returns"] = (((SPY["Adj Open"] - SPY["Adj Close"].shift(1)) / SPY["Adj Close"].shift(1)).fillna(0))*100
    
    if trade_date == valid_dates[-1]:
        continue

    next_day = (pd.to_datetime(trade_date) + timedelta(days = 1)).strftime("%Y-%m-%d")
    
    Actual_Dataset = SPY[SPY.index == next_day]
    
    # if holiday or if the next trading day is not a consecutive week day, we don't trade
    if len(Actual_Dataset) < 1:
        
        continue
    
    # get price at 3:55 (5 min before market close)
    
    spy_bar_request = requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/{trade_date}/{trade_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()

    spy_bars = pd.json_normalize(spy_bar_request["results"])
    spy_bars["t"] = pd.to_datetime(spy_bars["t"], unit = "ms", utc = True)
    spy_bars = spy_bars.set_index("t")
    spy_bars.index = spy_bars.index.tz_convert("US/Eastern")

    spy_bars["hour"] = spy_bars.index.hour
    spy_bars["minute"] = spy_bars.index.minute
    
    five_minutes_before_close = spy_bars[(spy_bars["hour"] == 15) & (spy_bars["minute"] == 55)]
    
    # if the data is unavailable for any reason
    if len(five_minutes_before_close) < 1:
        continue
    
    ### opening trade ###
    
    # option format = "TICKER,3whitespaces,YYMMDD,SIDE,00$$$.000" | Ex. 'SPY   230831P00486000'
    
    five_minutes_before_close_price = int(spy_bars[(spy_bars["hour"] == 15) & (spy_bars["minute"] == 55)]["o"].iloc[0]) 
    
    expiration_date = pd.to_datetime(next_day).strftime("%y%m%d")
    ticker = "SPY   "
    side = call_or_put(TimeSeries_Prediction.iloc[0])
    
    option_symbol = f'{ticker}{expiration_date}{side}00{five_minutes_before_close_price}000'

    entry_data = db_api_client.timeseries.get_range(
       dataset='OPRA.PILLAR',
       schema='mbp-1',
       stype_in='raw_symbol',
       symbols=[option_symbol],
       start=f'{trade_date}T19:55:25',
       end=f'{trade_date}T19:55:30',
    )
    
    entry_dataset = entry_data.to_df()
    entry_dataset.index = entry_dataset.index.tz_convert("US/Eastern")
    
    entry_dataset["hour"] = entry_dataset.index.hour
    entry_dataset["minute"] = entry_dataset.index.minute
    
    entry_dataset = entry_dataset[(entry_dataset["hour"] == 15) & (entry_dataset["minute"] == 55)]
    
    # if we are pulling from a daylight savings adjusted time
    if len(entry_dataset) < 1:
        
        entry_data = db_api_client.timeseries.get_range(
           dataset='OPRA.PILLAR',
           schema='mbp-1',
           stype_in='raw_symbol',
           symbols=[option_symbol],
           start=f'{trade_date}T18:55:25',
           end=f'{trade_date}T18:55:30',
        )
        entry_dataset = entry_data.to_df()
        entry_dataset.index = entry_dataset.index.tz_convert("US/Eastern")
        
        entry_dataset["hour"] = entry_dataset.index.hour
        entry_dataset["minute"] = entry_dataset.index.minute
        
        entry_dataset = entry_dataset[(entry_dataset["hour"] == 15) & (entry_dataset["minute"] == 55)]

    # we use the average ask price of the 5 second interval from 3:55:25 to 3:55:30
    entry_price = entry_dataset["ask_px_00"].mean()
    
    ### closing trade ###
    
    closing_data = db_api_client.timeseries.get_range(
       dataset='OPRA.PILLAR',
       schema='mbp-1',
       stype_in='raw_symbol',
       symbols=[option_symbol],
       start=f'{next_day}T13:30:15',
       end=f'{next_day}T13:30:30',
    )
    
    closing_dataset = closing_data.to_df()
    closing_dataset.index = closing_dataset.index.tz_convert("US/Eastern")
    
    closing_dataset["hour"] = closing_dataset.index.hour
    closing_dataset["minute"] = closing_dataset.index.minute
    
    closing_dataset = closing_dataset[(closing_dataset["hour"] == 9) & (closing_dataset["minute"] == 30)]
    
    # if we are pulling from a daylight savings adjusted time
    if len(closing_dataset) < 1:
        
        closing_data = db_api_client.timeseries.get_range(
           dataset='OPRA.PILLAR',
           schema='mbp-1',
           stype_in='raw_symbol',
           symbols=[option_symbol],
           start=f'{next_day}T12:30:15',
           end=f'{next_day}T12:30:30',
        )
        
        closing_dataset = closing_data.to_df()
        closing_dataset.index = closing_dataset.index.tz_convert("US/Eastern")
        
        closing_dataset["hour"] = closing_dataset.index.hour
        closing_dataset["minute"] = closing_dataset.index.minute
        
        closing_dataset = closing_dataset[(closing_dataset["hour"] == 9) & (closing_dataset["minute"] == 30)]
    

    # we use the average ask price of the 15 second interval from 9:30:15 to 9:30:30
    closing_price = closing_dataset["ask_px_00"].mean()
    
    gross_pnl = closing_price - entry_price
    
    option_fees = 0.0165 # ((0.65 commission per trade) * 2) + $0.35 regulatory fees (extremely conservative)
    
    estimated_slippage = 0.02
    
    net_pnl = gross_pnl - option_fees - estimated_slippage
    
    Pred_Actual = pd.DataFrame([{"prediction_date": SP500_Production_Dataset.index[0].strftime("%Y-%m-%d"), "prediction": TimeSeries_Prediction.iloc[0],
                             "actual_date": Actual_Dataset.index[0].strftime("%Y-%m-%d"), "actual": Actual_Dataset["returns"].iloc[0],
                                "option_entry_price": entry_price, "option_closing_price": closing_price,
                                "gross_pnl":gross_pnl, "option_fees": option_fees, "estimated_slippage": estimated_slippage,
                                "net_pnl_after_fees": net_pnl}])

    prediction_actual_list.append(Pred_Actual)
    
    # Every 5th request time out for a minute to reset API quota
    if (len(prediction_actual_list) % 5) == 0:
        
        time.sleep(59)

end_time = datetime.now()

Elapsed_Time = end_time - start_time
print(f"Elapsed Time: {Elapsed_Time}")

Prediction_Actual_DataFrame = pd.concat(prediction_actual_list).reset_index(drop = True)

Prediction_Actual_DataFrame["prediction_sign"] = np.sign(Prediction_Actual_DataFrame["prediction"]).values
Prediction_Actual_DataFrame["actual_sign"] = np.sign(Prediction_Actual_DataFrame["actual"]).values

Prediction_Actual_DataFrame["dollar_profit"] = round(Prediction_Actual_DataFrame["net_pnl_after_fees"] * 100, 2)
Prediction_Actual_DataFrame["capital"] = 1000 + Prediction_Actual_DataFrame["net_pnl_after_fees"].cumsum() * 100

overall_win_rate = len(Prediction_Actual_DataFrame[Prediction_Actual_DataFrame["prediction_sign"] == Prediction_Actual_DataFrame["actual_sign"]]) /  len(Prediction_Actual_DataFrame)
positive_win_rate = len(Prediction_Actual_DataFrame[(Prediction_Actual_DataFrame["prediction_sign"] == Prediction_Actual_DataFrame["actual_sign"]) & (Prediction_Actual_DataFrame["prediction_sign"] >= 1)]) /  len(Prediction_Actual_DataFrame[Prediction_Actual_DataFrame["prediction_sign"] >= 1])
negative_win_rate = len(Prediction_Actual_DataFrame[(Prediction_Actual_DataFrame["prediction_sign"] == Prediction_Actual_DataFrame["actual_sign"]) & (Prediction_Actual_DataFrame["prediction_sign"] < 0)]) /  len(Prediction_Actual_DataFrame[Prediction_Actual_DataFrame["prediction_sign"] < 0])

print(f"From {valid_dates[0]} to {valid_dates[-1]}, the system had an overall win rate of {overall_win_rate*100}%, with positive predictions winning at {positive_win_rate*100}%, and negative predictions winning at {negative_win_rate*100}%")

# strategy variations

Cheap_Options_Only = Prediction_Actual_DataFrame[Prediction_Actual_DataFrame["option_entry_price"] < 1].copy()
Cheap_Options_Only["capital"] = 1000 + Cheap_Options_Only["net_pnl_after_fees"].cumsum() * 100
Cheap_Options_Only["gross_capital"] = 1000 + Cheap_Options_Only["gross_pnl"].cumsum() * 100

plt.figure(dpi = 800)

plt.plot(pd.to_datetime(Cheap_Options_Only["prediction_date"]), Cheap_Options_Only["capital"].values)
plt.plot(pd.to_datetime(Cheap_Options_Only["prediction_date"]), Cheap_Options_Only["gross_capital"].values)

plt.suptitle(f"Growth of $1,000 from {valid_dates[0]} to {valid_dates[-1]}")
plt.title("Only Options less than $1.00")

plt.xticks(rotation = 45)

plt.legend(["Incl. Fees", "Gross"])
plt.show()

########

Average_Options_Only = Prediction_Actual_DataFrame[(Prediction_Actual_DataFrame["option_entry_price"] >= 1) & (Prediction_Actual_DataFrame["option_entry_price"] < 2)].copy()
Average_Options_Only["capital"] = 1000 + Average_Options_Only["net_pnl_after_fees"].cumsum() * 100
Average_Options_Only["gross_capital"] = 1000 + Average_Options_Only["gross_pnl"].cumsum() * 100

plt.figure(dpi = 800)

plt.plot(pd.to_datetime(Average_Options_Only["prediction_date"]), Average_Options_Only["capital"].values)
plt.plot(pd.to_datetime(Average_Options_Only["prediction_date"]), Average_Options_Only["gross_capital"].values)

plt.suptitle(f"Growth of $1,000 from {valid_dates[0]} to {valid_dates[-1]}")
plt.title("Only Options between $1.00 and 2.00")

plt.xticks(rotation = 45)

plt.legend(["Incl. Fees", "Gross"])
plt.show()

##########

Expensive_Options_Only = Prediction_Actual_DataFrame[(Prediction_Actual_DataFrame["option_entry_price"] > 2)].copy()
Expensive_Options_Only["capital"] = 1000 + Expensive_Options_Only["net_pnl_after_fees"].cumsum() * 100
Expensive_Options_Only["gross_capital"] = 1000 + Expensive_Options_Only["gross_pnl"].cumsum() * 100

plt.figure(dpi = 800)

plt.plot(pd.to_datetime(Expensive_Options_Only["prediction_date"]), Expensive_Options_Only["capital"].values)
plt.plot(pd.to_datetime(Expensive_Options_Only["prediction_date"]), Expensive_Options_Only["gross_capital"].values)

plt.suptitle(f"Growth of $1,000 from {valid_dates[0]} to {valid_dates[-1]}")
plt.title("Only Options greater than $2.00")

plt.xticks(rotation = 45)

plt.legend(["Incl. Fees", "Gross"])
plt.show()

##########

# regular strategy, but buy 5 contracts if the option price is less than $1

Cheap_Option_Strategy = pd.concat(prediction_actual_list).reset_index(drop = True)
Cheap_Option_Strategy["net_pnl_after_fees"] = Cheap_Option_Strategy.apply(lambda row: row['net_pnl_after_fees'] * 5 if row['option_entry_price'] < 1 else row['net_pnl_after_fees'], axis=1)
Cheap_Option_Strategy["capital"] = 1000 + Cheap_Option_Strategy["net_pnl_after_fees"].cumsum() * 100

plt.figure(dpi = 800)

plt.plot(pd.to_datetime(Prediction_Actual_DataFrame["prediction_date"]), Prediction_Actual_DataFrame["capital"].values)
plt.plot(pd.to_datetime(Cheap_Option_Strategy["prediction_date"]), Cheap_Option_Strategy["capital"].values)

plt.suptitle(f"Growth of $1,000 from {valid_dates[0]} to {valid_dates[-1]}")
plt.title("Standard Strategy vs. Buying More When Option is Cheap")

plt.xticks(rotation = 45)

plt.legend(["Original", "Original+Cheap"])
plt.show()

##########
# miscellaneous scatter plots

plt.figure(dpi = 800)

plt.scatter(x = Prediction_Actual_DataFrame["option_entry_price"], y = Prediction_Actual_DataFrame["option_closing_price"])

plt.title("Option Entry Price v. Option Closing Price")

plt.xlabel("Entry Price")
plt.ylabel("Closing Price")

plt.show()

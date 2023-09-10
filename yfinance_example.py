import yfinance as yf
import pandas as pd


ticker = "MSFT"
stock = yf.Ticker(ticker)
stock_info = stock.fast_info
print(stock_info)
print(stock_info["lastPrice"])

# stock_info.keys() for other properties you can explore
market_price = stock_info["regularMarketPrice"]
previous_close_price = stock_info["regularMarketPreviousClose"]
# print("market price ", market_price)
# print("previous close price ", previous_close_price)


# get historical market data
hist = stock.history(period="max")
# print(hist)
# show meta information about the history (requires history() to be called first)
hist_data = stock.history_metadata
# print(hist_data)

# show actions (dividends, splits, capital gains)
# print(stock.actions)

# show dividends/splits/capital_gains
# print(stock.dividends)

# show share count --> VALUABLE
print(stock.shares)
print(stock.shares["BasicShares"][2019])

# show financials

# - show income statement
# print(stock.income_stmt)
df = pd.DataFrame(stock.income_stmt)
df.to_csv(ticker + "_income_statements.csv", sep=";")
# print(stock.quarterly_income_stmt)
# - balance sheet
# print(stock.balance_sheet)
df = pd.DataFrame(stock.balance_sheet)
df.to_csv(ticker + "_balance_sheet.csv", sep=";")

# see Ticker.get_income_stsmt() for more options
# print(yf.Ticker(ticker).get_income_stmt())
print(stock.isin)

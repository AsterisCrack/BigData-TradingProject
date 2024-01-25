import yfinance as yfinance
import pandas as pd
import datetime as dt
import os
import time
import sys

from AllCompanies.getCompaniesData import getCompanyTickers

# Get the current working directory
cwd = os.getcwd()
#Create directory historical_data if it doesn't exist
if not os.path.exists(cwd + '/historical_data'):
    os.makedirs(cwd + '/historical_data')

def get_historical_data_single_ticker(ticker, start_date, end_date):
    """
    Gets historical data for a given ticker and date range
    :param ticker: Stock ticker
    :param start_date: Start date
    :param end_date: End date
    :return: Historical data for the given ticker and date range
    """
    # Get the historical data
    historical_data = yfinance.download(ticker, start=start_date, end=end_date)

    # Add the ticker as a column
    historical_data['ticker'] = ticker

    # Reset the index
    historical_data.reset_index(inplace=True)

    # Reorder the columns
    historical_data = historical_data[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]

    # Return the historical data
    return historical_data

def get_historical_data(start_date, end_date):
    compannies = getCompanyTickers()
    # Create an empty dataframe
    historical_data = pd.DataFrame()
    # For each ticker
    for ticker in compannies:
        # Get the historical data for the ticker
        ticker_historical_data = get_historical_data_single_ticker(ticker, start_date, end_date)
        # Concatenate the dataframes
        historical_data = pd.concat([historical_data, ticker_historical_data])
        print(ticker_historical_data.head())
    return historical_data

if __name__ == '__main__':
    # Get the start date
    start_date = dt.datetime(2020, 1, 1)
    # Get the end date
    end_date = dt.datetime(2023, 12, 31)
    
    # Get the historical data
    historical_data = get_historical_data(start_date, end_date)

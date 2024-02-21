import yfinance as yf
import pandas as pd
import os, sys, time
import datetime as dt
from dotenv import load_dotenv
from AllCompanies.getCompaniesData import getCompanyTickers, getComanyCIK
import Kafka.producer as producer

load_dotenv()
# Get the current working directory
cwd = os.getcwd()

def download_data(tickers, start_date, end_date):
    """
    Getsreal time data for a given ticker and date range
    :param ticker: Stock ticker
    :param start_date: Start date
    :param end_date: End date
    :return: Historical data for the given ticker and date range
    """

    # Download data
    downloaded_data = yf.download(tickers, start=start_date, end=end_date)

    # Extract only the 'Adj Close' prices
    columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    for column in columns:
        col_data = downloaded_data[column]
        col_data = col_data.reset_index()
        col_data = pd.melt(col_data, id_vars='Date', var_name='Ticker', value_name=column)
        if column == columns[0]:
            data = col_data
        else:
            data = pd.merge(data, col_data, on=['Date', 'Ticker'])

    data = data[['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
    # Return the historical data
    return data

def get_data(full_timestamp=False):
    compannies = getCompanyTickers()
    start_date = dt.datetime.strptime(os.getenv("START_DATE_REALTIME"), '%Y-%m-%d')
    start_date = start_date if full_timestamp else dt.datetime.now() - dt.timedelta(days=int(os.getenv("N_DAYS_REALTIME")))
    end_date = dt.datetime.now()
    # Download the data
    data = download_data(compannies, start_date, end_date)
    # Create a new df with the companies and their cik and then merge it with the data
    ciks = pd.DataFrame(columns=["Ticker", "Cik"])
    for company in compannies:
        cik = getComanyCIK(company)
        ciks = pd.concat([ciks, pd.DataFrame({"Ticker": [company], "Cik": [cik]})])
    data = pd.merge(data, ciks, on="Ticker")
    return data

def get_jsons(data):
    jsons = []
    for company in data["Ticker"].unique():
        jsons.append(data[data["Ticker"] == company].to_json(orient="records"))
    return jsons

if __name__ == "__main__":
    full_timestamp = sys.argv[1] if len(sys.argv) > 1 else "False"
    if full_timestamp not in ["True", "False"]:
        print("Invalid argument for 'get_full_timestamp'. Please use True or False")
        sys.exit(1)
    full_timestamp = True if full_timestamp == "True" else False    
    data = get_data(full_timestamp)
    jsons = get_jsons(data)
    for json in jsons:
        producer.send_data_to_kafka(json)
        time.sleep(0.01)

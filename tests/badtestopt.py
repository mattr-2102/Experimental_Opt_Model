import os
import pandas as pd
import requests
from datetime import datetime

# Your Polygon API key
API_KEY = "UvSnRxq0stT5Lb9p2rvGO6JBFzzBuclX"

def get_options_contracts(underlying_ticker, expiration_date_gte, limit=100):
    url = (
        f"https://api.polygon.io/v3/reference/options/contracts?"
        f"underlying_ticker={underlying_ticker}"
        f"&expiration_date.gte={expiration_date_gte}"
        f"&as_of={expiration_date_gte}"
        f"&expired=false"
        f"&limit={limit}"
        f"&apiKey={API_KEY}"
    )
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching contracts: HTTP {response.status_code}")
        return []
    data = response.json()
    return data.get("results", [])

def get_hourly_aggregates(option_ticker, start_date, end_date):
    """
    Retrieves hourly aggregates (OHLCV) for a given option contract ticker over the specified date range.
    """
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{option_ticker}/range/1/hour/"
        f"{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={API_KEY}"
    )
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error for {option_ticker}: HTTP {response.status_code}")
        return []
    data = response.json()
    return data.get("results", [])

def download_hourly_options_chain(underlying_ticker, start_date, end_date, save_path="tests"):
    """
    Downloads hourly historical OHLCV data for all options contracts on a given underlying ticker over
    a specified date range.
    
    This function:
      1. Retrieves the list of active options contracts (with expiration on or after start_date).
      2. For each contract, downloads hourly aggregates between start_date and end_date.
      3. Combines the data into a single CSV.
    
    Parameters:
      underlying_ticker (str): The underlying stock ticker symbol.
      start_date (str): Start date in 'YYYY-MM-DD' format (also used as the lower bound for contract expirations).
      end_date (str): End date in 'YYYY-MM-DD' format.
      save_path (str): Directory where the CSV will be saved.
    """
    os.makedirs(save_path, exist_ok=True)
    
    print(f"Fetching contracts for {underlying_ticker} with expiration >= {start_date}...")
    contracts = get_options_contracts(underlying_ticker, start_date)
    print(f"Found {len(contracts)} contracts.")
    
    all_data = []
    for contract in contracts:
        # Use the "ticker" field from the JSON example
        option_ticker = contract.get("ticker")
        if not option_ticker:
            continue
        print(f"Fetching hourly data for {option_ticker}...")
        aggregates = get_hourly_aggregates(option_ticker, start_date, end_date)
        if aggregates:
            df = pd.DataFrame(aggregates)
            # Convert timestamp (in ms) to a datetime column
            df["Date"] = pd.to_datetime(df["t"], unit="ms")
            df["option_ticker"] = option_ticker
            all_data.append(df)
        else:
            print(f"No hourly data for {option_ticker}")
    
    if not all_data:
        print("No hourly options data collected.")
        return
    
    combined_df = pd.concat(all_data, ignore_index=True)
    # Rearrange and rename columns
    combined_df = combined_df[["option_ticker", "Date", "o", "h", "l", "c", "v"]]
    combined_df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}, inplace=True)
    
    file_path = os.path.join(save_path, f"{underlying_ticker}_options_hourly_{start_date}_to_{end_date}.csv")
    combined_df.to_csv(file_path, index=False)
    print(f"✅ Saved hourly options data to {file_path}")

if __name__ == "__main__":
    # For example, retrieving data between 2025-01-25 and 2025-01-29.
    download_hourly_options_chain("SPY", "2025-01-25", "2025-01-29", save_path="tests")

import os
import pandas as pd
import requests
from datetime import datetime

def download_market_data(ticker, save_path="tests", start_date="2025-01-01", end_date="2025-01-31"):
    """
    Downloads OHLCV market data for a specified date range using the Polygon API and saves it as a CSV.
    """
    os.makedirs(save_path, exist_ok=True)  # Ensure the save directory exists

    # Polygon API key (hardcoded for demonstration)
    api_key = "UvSnRxq0stT5Lb9p2rvGO6JBFzzBuclX"
    
    # Build the URL for daily aggregates (OHLCV data) using Polygon's Aggregates endpoint.
    # Here, multiplier=1 and timespan=day returns daily bars.
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={api_key}")
    
    print(f"Fetching market data for {ticker} from {start_date} to {end_date}...")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Error fetching market data: HTTP {response.status_code}")
        return
    
    json_data = response.json()
    
    # The Polygon API returns the results in a key called "results"
    results = json_data.get("results", [])
    if not results:
        print(f"❌ No data found for {ticker} between {start_date} and {end_date}.")
        return
    
    # Convert the results list to a DataFrame.
    df = pd.DataFrame(results)
    
    df["Date"] = pd.to_datetime(df["t"], unit="ms")
    df["Open"] = df["o"]
    df["High"] = df["h"]
    df["Low"] = df["l"]
    df["Close"] = df["c"]
    df["Volume"] = df["v"]
    
    # Ensure data is sorted by Date in ascending order
    df = df.sort_values("Date")
    
    # Select only the relevant columns
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]
    
    # Save the combined data as CSV
    file_path = os.path.join(save_path, f"{ticker}_market_{start_date}_to_{end_date}.csv")
    df.to_csv(file_path, index=False)
    
    print(f"✅ Saved market data for {ticker} from {start_date} to {end_date} to {file_path}")

# Example usage
if __name__ == "__main__":
    download_market_data("SPY")

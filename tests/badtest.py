import os
import pandas as pd
from datetime import datetime
from alpha_vantage.timeseries import TimeSeries

def download_market_data(ticker, save_path="tests", start_date="2025-01-01", end_date="2025-01-31"):
    """
    Downloads market data for a specified date range using Alpha Vantage and saves it as a CSV.
    
    Parameters:
    - ticker: The stock ticker symbol.
    - save_path: The directory where the CSV will be saved.
    - start_date: Start date in 'YYYY-MM-DD' format.
    - end_date: End date in 'YYYY-MM-DD' format.
    """
    os.makedirs(save_path, exist_ok=True)  # Ensure save directory exists

    # Get the Alpha Vantage API key from your config (hardcoded here for demonstration)
    api_key = "2V6AECCNMRV5OE9J"
    ts = TimeSeries(key=api_key, output_format='pandas')

    # Fetch daily data using 'full' to retrieve all historical data
    data, meta_data = ts.get_daily(symbol=ticker, outputsize='full')
    
    # Convert index to datetime
    data.index = pd.to_datetime(data.index)
    
    # Filter the DataFrame for the specified date range
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    data = data.loc[(data.index >= start_dt) & (data.index <= end_dt)]
    
    # Reset index for saving
    data.reset_index(inplace=True)

    file_path = os.path.join(save_path, f"{ticker}_market_{start_date}_to_{end_date}.csv")
    data.to_csv(file_path, index=False)
    
    print(f"✅ Saved market data for {ticker} from {start_date} to {end_date} to {file_path}")

# Example usage
if __name__ == "__main__":
    # Update the start_date and end_date as needed.
    download_market_data("SPY", start_date="2023-01-01", end_date="2023-01-31")

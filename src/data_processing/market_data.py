import os
import pandas as pd
import requests
import time
import datetime as dt
from datetime import datetime, timedelta
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.technical_indicators import add_technical_indicators

class MarketDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        
        self.end = DATA_CONFIG["data_sources"]["end_date"]
        self.days_back = DATA_CONFIG["data_sources"]["days_back"]
        self.intraday_interval = DATA_CONFIG["data_sources"]["intraday_interval"]
        
        self.key = DATA_CONFIG["data_sources"]["poly_api"]
        self.df = None  # Placeholder for data

    def fetch_market_data(self):
        BASE_URL = "https://api.polygon.io"
        # Calculate start and end date strings (YYYY-MM-DD)
        end_date = self.end
        start_date = end_date - timedelta(days=self.days_back)
        end_str = end_date.strftime("%Y-%m-%d")
        start_str = start_date.strftime("%Y-%m-%d")

        # Construct the initial URL
        url = (f"{BASE_URL}/v2/aggs/ticker/{self.ticker}/range/{self.intraday_interval}/"
            f"{start_str}/{end_str}?adjusted=false&sort=asc&apiKey={self.key}")

        all_results = []
        session = requests.Session()
        while url:
            try:
                resp = session.get(url, timeout=30)
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                break
            if resp.status_code != 200:
                print(f"HTTP error {resp.status_code} for URL: {url}")
                break
            data = resp.json()
            if "results" in data:
                all_results.extend(data["results"])
            else:
                print("No 'results' in response.")
                break

            # Check for a 'next_url' for pagination; if it exists, append the API key.
            next_url = data.get("next_url")
            if next_url:
                url = f"{next_url}&apiKey={self.key}"
            else:
                url = None
            time.sleep(13)  # 13-second pause between calls to meet rate limits

        if not all_results:
            print("No market data fetched.")
            return None

        df = pd.DataFrame(all_results)
        # Convert the Unix millisecond timestamp in 't' to a datetime column called 'Date'
        if "t" in df.columns:
            # Convert as UTC, then convert to Eastern Time and drop the timezone
            df["Date"] = (pd.to_datetime(df["t"], unit="ms", utc=True)
                        .dt.tz_convert("US/Eastern")
                        .dt.tz_localize(None))
            
        
        # Drop the 't' column and rename columns: assume price and volume fields are 'o', 'l', 'h', 'c', 'v'
        df.drop(columns=["t"], inplace=True)
        df.rename(columns={"o": "Open", "l": "Low", "h": "High", "c": "Close", "v": "Volume"}, inplace=True)
        
        # Only keep rows where the time is between 9:30 AM and 4:00 PM.
        df = df[(df["Date"].dt.time >= dt.time(9, 30)) & (df["Date"].dt.time < dt.time(16, 0))]

        # Ensure required columns exist
        required_cols = {"Date", "Open", "Low", "High", "Close", "Volume"}
        if not required_cols.issubset(df.columns):
            print("Missing required columns in market data.")
            return None
        self.df = df
        return df


    def save_to_csv(self):
        if self.df is None:
            print("No data to save.")
            return
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.df.to_csv(filename, index=False)
        print(f"Market data saved to {filename}.")

    def process_market_and_save(self):
        print("Fetching market data...")
        df = self.fetch_market_data()
        if df is None or df.empty:
            print("No market data fetched.")
            return
        print("Adding technical indicators...")
        df_with_indicators = add_technical_indicators(df)
        self.df = df_with_indicators
        self.save_to_csv()
        return self.df

if __name__ == "__main__":
    # Example usage with ticker SPY
    processor = MarketDataProcessor("SPY")
    processor.process_market_and_save()

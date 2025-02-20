import os
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import requests
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.technical_indicators import add_technical_indicators

class MarketDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        self.start = DATA_CONFIG["data_sources"]["start_date"]
        self.end = DATA_CONFIG["data_sources"]["end_date"]
        self.df = None  # Placeholder for data

    def fetch_data(self):
        """Download stock price data using Polygon."""
        print(f"📥 Fetching stock data for {self.ticker} from {self.start} to {self.end}...")
        url = (f"https://api.polygon.io/v2/aggs/ticker/{self.ticker}/range/1/day/"
        f"{self.start}/{self.end}?adjusted=true&sort=asc&apiKey={DATA_CONFIG["data_sources"]["poly_api"]}")
        
        response = requests.get(url)
    
        # check if data can fetch
        if response.status_code != 200:
            raise ValueError(f"❌ No data fetched for {self.ticker}. Check API connection or ticker validity.")        

        json_data = response.json()
        results = json_data.get("results", [])
        
        # check if data found
        if not results:
            print(f"❌ No data found for {self.ticker} between {self.start_date} and {self.end_date}.")
            return
        
        self.df = pd.DataFrame(results)
        
        self.df["Date"] = pd.to_datetime(self.df["t"], unit="ms")
        self.df["Open"] = self.df["o"]
        self.df["High"] = self.df["h"]
        self.df["Low"] = self.df["l"]
        self.df["Close"] = self.df["c"]
        self.df["Volume"] = self.df["v"]
        
        # Ensure data is sorted by Date in ascending order
        self.df = self.df.sort_values("Date")
        
        # Select only the relevant columns
        self.df = self.df[["Date", "Open", "High", "Low", "Close", "Volume"]]

        
        print("📊 Downloaded Data Sample:\n", self.df.head())  
        print("📊 Columns Retrieved:", self.df.columns)  

        # ✅ Ensure all expected columns exist
        expected_columns = {"Open", "High", "Low", "Close", "Volume"}
        missing_columns = expected_columns - set(self.df.columns)
        if missing_columns:
            raise ValueError(f"❌ Missing expected columns: {missing_columns}")

        self.df.dropna(inplace=True)
        self.df.reset_index(inplace=True)
        return self.df

    def process_data(self):
        """Fetch data and apply technical indicators."""
        self.fetch_data()
        self.df = add_technical_indicators(self.df)
        return self.df

    def save_to_csv(self):
        """Save processed market data using dynamic filename."""
        if self.df is None or self.df.empty:
            raise ValueError(f"⚠️ No data available for {self.ticker}. Skipping save.")

        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        os.makedirs(os.path.dirname(filename), exist_ok=True)  # Ensure directory exists
        self.df.to_csv(filename, index=False)
        print(f"✅ Saved market data for {self.ticker} to {filename}")

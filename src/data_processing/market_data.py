import os
import alpha as yf
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.technical_indicators import add_technical_indicators

class MarketDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        self.start = DATA_CONFIG["data_sources"]["start_date"]
        self.end = DATA_CONFIG["data_sources"]["end_date"]
        self.df = None  # Placeholder for data
        self.api_key = DATA_CONFIG["data_sources"]["alpha_api"]

    def fetch_data(self):
        """Download stock price data using Yahoo Finance."""
        print(f"📥 Fetching stock data for {self.ticker} from {self.start} to {self.end}...")
        ts = TimeSeries(key=api_key, output_format='pandas')
    
        if self.df.empty:
            raise ValueError(f"❌ No data fetched for {self.ticker}. Check API connection or ticker validity.")        

        # Convert index to datetime
        data.index = pd.to_datetime(data.index)
        # Filter the DataFrame for the specified date range
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        data = data.loc[(data.index >= start_dt) & (data.index <= end_dt)]
        # Reset index for saving
        data.reset_index(inplace=True)

        print("📊 Downloaded Data Sample:\n", self.df.head())  
        print("📊 Columns Retrieved:", self.df.columns)  

        # ✅ Ensure all expected columns exist
        expected_columns = {"Open", "High", "Low", "Close", "Volume"}
        missing_columns = expected_columns - set(self.df.columns)
        if missing_columns:
            raise ValueError(f"❌ Missing expected columns: {missing_columns}")

        self.df.dropna(inplace=True)
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

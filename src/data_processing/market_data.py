import yfinance as yf
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.technical_indicators import add_technical_indicators

class MarketDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        self.start = DATA_CONFIG["data_sources"]["start_date"]
        self.end = DATA_CONFIG["data_sources"]["end_date"]
        self.df = None  # Placeholder for data

    def fetch_data(self):
        """Download stock price data using Yahoo Finance."""
        print(f"📥 Fetching stock data for {self.ticker} from {self.start} to {self.end}...")
        self.df = yf.download(self.ticker, start=self.start, end=self.end)
        self.df.dropna(inplace=True)
        return self.df

    def process_data(self):
        """Fetch data and apply technical indicators."""
        self.fetch_data()
        self.df = add_technical_indicators(self.df)
        return self.df

    def save_to_csv(self):
        """Save processed market data using dynamic filename."""
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        self.df.to_csv(filename, index=False)
        print(f"✅ Saved market data for {self.ticker} to {filename}")

# Example Usage:
if __name__ == "__main__":
    for ticker in DATA_CONFIG["data_sources"]["tickers"]:
        processor = MarketDataProcessor(ticker)
        processor.process_data()
        processor.save_to_csv()

import os
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.market_data import MarketDataProcessor
from src.data_processing.options_chain import OptionsDataProcessor

class DataLoader:
    def __init__(self, ticker):
        self.ticker = ticker

    def safe_load_csv(self, filename, data_fetch_function=None):
        """Safely load CSV data. If missing, fetch new data."""
        if not os.path.exists(filename):
            print(f"⚠️ File {filename} not found. Fetching new data...")

            if data_fetch_function is not None:
                df = data_fetch_function()
                if df is not None and not df.empty:
                    df.to_csv(filename, index=False)  # Save fetched data
                    print(f"✅ Fetched and saved data for {self.ticker} to {filename}")
                else:
                    print(f"⚠️ No data available for {self.ticker}. Returning empty DataFrame.")
                    return pd.DataFrame()
            else:
                return pd.DataFrame()

        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

    def load_market_data(self):
        """Load processed market data, fetch if missing."""
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        return self.safe_load_csv(filename, lambda: MarketDataProcessor(self.ticker).process_data())

    def load_options_data(self):
        """Load processed options chain data, fetch if missing."""
        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        return self.safe_load_csv(filename, lambda: OptionsDataProcessor(self.ticker).fetch_options_chain())

    def load_macro_indicators(self):
        """Load macro indicators data (global indicators, so no need for per-ticker fetching)."""
        filename = MODEL_PATHS["paths"]["macro_indicators"]
        return self.safe_load_csv(filename)

# Example Usage:
if __name__ == "__main__":
    for ticker in DATA_CONFIG["data_sources"]["tickers"]:
        loader = DataLoader(ticker)
        market_data = loader.load_market_data()
        options_data = loader.load_options_data()
        macro_data = loader.load_macro_indicators()

        print("📊 Market Data Sample:\n", market_data.head())
        print("📊 Options Data Sample:\n", options_data.head())
        print("📊 Macro Indicators Sample:\n", macro_data.head())

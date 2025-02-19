import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

class DataLoader:
    def __init__(self, ticker):
        self.ticker = ticker

    def load_market_data(self):
        """Load processed market data."""
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

    def load_options_data(self):
        """Load processed options chain data."""
        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        return pd.read_csv(filename)

    def load_macro_indicators(self):
        """Load macro indicators data (Global data)."""
        return pd.read_csv(MODEL_PATHS["paths"]["macro_indicators"], parse_dates=["Date"], index_col="Date")

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

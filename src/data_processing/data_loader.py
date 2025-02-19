import os
import pandas as pd
from src.config.config_loader import MODEL_PATHS
from src.data_processing.market_data import MarketDataProcessor
from src.data_processing.options_chain import OptionsDataProcessor
from src.data_processing.macro_factors import MacroIndicators

class DataLoader:
    def __init__(self, ticker):
        self.ticker = ticker

    def _check_and_fetch_data(self, file_path, fetch_function):
        """Checks if the file exists. If not, calls the fetch function to generate it."""
        if not os.path.exists(file_path):
            print(f"📌 {file_path} not found. Fetching and processing data for {self.ticker}...")
            fetch_function()
        else:
            print(f"✅ {file_path} found. Loading existing data.")
    
    def load_market_data(self):
        """Load or fetch processed market data."""
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, lambda: self._fetch_market_data())
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

    def _fetch_market_data(self):
        """Fetch and process market data if it does not exist."""
        processor = MarketDataProcessor(self.ticker)
        processor.process_data()
        processor.save_to_csv()

    def load_options_data(self):
        """Load or fetch processed options data."""
        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, lambda: self._fetch_options_data())
        return pd.read_csv(filename)

    def _fetch_options_data(self):
        """Fetch and process options data if it does not exist."""
        processor = OptionsDataProcessor(self.ticker)
        processor.fetch_options_chain()
        processor.process_and_save()

    def load_macro_indicators(self):
        """Load or fetch macro indicators (global data)."""
        filename = MODEL_PATHS["paths"]["macro_indicators"]
        self._check_and_fetch_data(filename, lambda: self._fetch_macro_indicators())
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

    def _fetch_macro_indicators(self):
        """Fetch and process macro indicators if they do not exist."""
        macro = MacroIndicators()
        macro.fetch_macro_indicators()
        macro.save_to_csv()  # Now saving to file after fetching


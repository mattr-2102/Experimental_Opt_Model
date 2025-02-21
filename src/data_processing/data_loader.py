import os
import pandas as pd
from src.config.config_loader import MODEL_PATHS
from src.data_processing.market_data import MarketDataProcessor
from src.data_processing.options_chain import OptionsDataProcessor
from src.data_processing.macro_factors import MacroIndicators

class DataLoader:
    """
    DataLoader class to manage loading and fetching data for a given ticker.
    Provides methods to load market data, options data, and macro indicators.
    If a CSV file is not present (or is empty), the corresponding processing
    module is called to fetch and save the data.
    """
    
    def __init__(self, ticker):
        self.ticker = ticker

    def _check_and_fetch_data(self, file_path, fetch_function):
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            print(f"📌 {file_path} not found or is empty. Fetching and processing data for {self.ticker}...")
            fetch_function()
        else:
            print(f"✅ {file_path} found. Loading existing data.")

    def _fetch_market_data(self):
        processor = MarketDataProcessor(self.ticker)
        processor.process_market_and_save()

    def _fetch_options_data(self):
        processor = OptionsDataProcessor(self.ticker)
        processor.process_options_and_save()

    def _fetch_macro_indicators(self):
        macro = MacroIndicators()
        macro.process_macro_and_save()

    def load_market_data(self):
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, self._fetch_market_data)
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

    def load_options_data(self):
        # Ensure market data is available first
        _ = self.load_market_data()
        
        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, self._fetch_options_data)
        return pd.read_csv(filename)

    def load_macro_indicators(self):
        filename = MODEL_PATHS["paths"]["macro_indicators"]
        self._check_and_fetch_data(filename, self._fetch_macro_indicators)
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")
import os
import pandas as pd

from src.config.config_loader import MODEL_PATHS
from src.data_processing.market_data import MarketDataProcessor
from src.data_processing.options_chain import OptionsDataProcessor
from src.data_processing.macro_factors import MacroIndicators
from src.data_processing.options_refined import OptionsRefinedDataProcessor

class DataLoader:
    """
    DataLoader class to manage loading and fetching data for a given ticker.
    Provides methods to load market data, options data, refined options data,
    and macro indicators. If a CSV file is not present (or is empty), the 
    corresponding processing module is called to fetch and save the data.
    """

    def __init__(self, ticker):
        self.ticker = ticker

    def _check_and_fetch_data(self, file_path, fetch_function):
        """
        Check if a file exists and is non-empty. If not, call the fetch_function
        to generate and save that data. Otherwise, just print a message that
        it exists.
        """
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            print(f"📌 {file_path} not found or is empty. "
                  f"Fetching and processing data for {self.ticker}...")
            fetch_function()
        else:
            print(f"✅ {file_path} found. Loading existing data.")

    # ----------------------------
    # Private "fetch" methods
    # ----------------------------
    def _fetch_market_data(self):
        processor = MarketDataProcessor(self.ticker)
        processor.process_market_and_save()

    def _fetch_options_data(self):
        processor = OptionsDataProcessor(self.ticker)
        processor.process_options_and_save()

    def _fetch_macro_indicators(self):
        macro = MacroIndicators()
        macro.process_macro_and_save()
        
    def _fetch_options_refined_data(self):
        processor = OptionsRefinedDataProcessor(self.ticker)
        processor.process_options_refined_and_save()

    # ----------------------------
    # Public "load" methods
    # ----------------------------
    def load_market_data(self):
        """
        Checks if market data for self.ticker exists in CSV form.
        If not found, fetches from source. Returns a DataFrame.
        """
        filename = MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, self._fetch_market_data)
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

    def load_options_data(self):
        """
        Ensures market data is available, then checks if raw options data exists.
        If not found, fetches from source. Returns a DataFrame of raw options.
        """
        # Ensure market data is available first
        _ = self.load_market_data()

        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, self._fetch_options_data)
        return pd.read_csv(filename)

    def load_sampled_options_data(self):
        """
        Ensures raw options data is available, then checks if the refined
        (sampled) options file exists. If not found, fetches and processes it.
        Returns a DataFrame of refined options.
        """
        # Ensure raw options data is available first
        _ = self.load_options_data()

        filename = MODEL_PATHS["paths"]["sampled_options_chain"].format(ticker=self.ticker)
        self._check_and_fetch_data(filename, self._fetch_options_refined_data)
        return pd.read_csv(filename)

    def load_macro_indicators(self):
        """
        Checks if macro indicators data is available, fetches if needed.
        Returns a DataFrame of macro data (VIX, MOVE, etc.).
        """
        filename = MODEL_PATHS["paths"]["macro_indicators"]
        self._check_and_fetch_data(filename, self._fetch_macro_indicators)
        return pd.read_csv(filename, parse_dates=["Date"], index_col="Date")

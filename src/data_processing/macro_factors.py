import yfinance as yf
import pandas as pd
from src.config.config_loader import DATA_CONFIG

class MacroIndicators:
    def __init__(self):
        self.data = {}

    def fetch_macro_indicators(self):
        """Fetch macro indicators from Yahoo Finance."""
        for indicator in DATA_CONFIG["data_sources"]["macro_indicators"]:
            try:
                self.data[indicator] = yf.download(
                    indicator, start=DATA_CONFIG["data_sources"]["start_date"], 
                    end=DATA_CONFIG["data_sources"]["end_date"]
                )["Close"]
                print(f"📈 Fetched {indicator}")
            except Exception as e:
                print(f"⚠️ Error fetching {indicator}: {e}")
                self.data[indicator] = None  # Handle missing data

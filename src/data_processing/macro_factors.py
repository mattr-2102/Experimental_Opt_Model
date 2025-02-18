import yfinance as yf
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

class MacroIndicators:
    def __init__(self):
        self.data = {}

    def fetch_macro_indicators(self):
        """Fetch macro indicators from Yahoo Finance."""
        for indicator in DATA_CONFIG["data_sources"]["macro_indicators"]:
            try:
                self.data[indicator] = yf.download(indicator, start=DATA_CONFIG["data_sources"]["start_date"], 
                                                   end=DATA_CONFIG["data_sources"]["end_date"])["Close"]
                print(f"📈 Fetched {indicator}")
            except Exception as e:
                print(f"⚠️ Error fetching {indicator}: {e}")

    def save_to_csv(self):
        """Save macro indicators data."""
        filename = MODEL_PATHS["paths"]["macro_indicators"]
        df = pd.DataFrame(self.data)
        df.to_csv(filename)
        print(f"✅ Saved macro indicators data to {filename}")

# Example Usage:
if __name__ == "__main__":
    macro = MacroIndicators()
    macro.fetch_macro_indicators()
    macro.save_to_csv()

import yfinance as yf
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

class OptionsDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        self.options_data = None

    def fetch_options_chain(self):
        """Fetch options chain from Yahoo Finance."""
        stock = yf.Ticker(self.ticker)
        expirations = stock.options  # Get expiration dates
        data = []

        for exp in expirations:
            try:
                opt_chain = stock.option_chain(exp)
                calls = opt_chain.calls
                puts = opt_chain.puts
                calls["Type"] = "Call"
                puts["Type"] = "Put"
                calls["Expiration"] = exp
                puts["Expiration"] = exp
                data.append(calls)
                data.append(puts)
            except Exception as e:
                print(f"⚠️ Error fetching options for {exp}: {e}")

        self.options_data = pd.concat(data)
        self.options_data.reset_index(drop=True, inplace=True)
        return self.options_data

    def filter_options(self, min_oi=500, atm_range=0.05):
        """Filter options: High OI & ATM."""
        stock_price = yf.Ticker(self.ticker).history(period="1d")["Close"].iloc[-1]
        self.options_data["Moneyness"] = abs(self.options_data["strike"] - stock_price) / stock_price
        self.options_data = self.options_data[(self.options_data["openInterest"] >= min_oi) & (self.options_data["Moneyness"] <= atm_range)]
        return self.options_data

    def save_to_csv(self):
        """Save processed options chain using dynamic filename."""
        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        self.options_data.to_csv(filename, index=False)
        print(f"✅ Saved options chain data to {filename}")

# Example Usage:
if __name__ == "__main__":
    for ticker in DATA_CONFIG["data_sources"]["tickers"]:
        processor = OptionsDataProcessor(ticker)
        processor.fetch_options_chain()
        processor.filter_options()
        processor.save_to_csv()

import yfinance as yf
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.smart_sampling import stratified_sampling, save_sampled_data

class OptionsDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        self.options_data = None

    def fetch_options_chain(self):
        """Fetch options chain from Yahoo Finance."""
        stock = yf.Ticker(self.ticker)
        expirations = stock.options

        if not expirations:
            print(f"⚠️ No options data available for {self.ticker}. Skipping.")
            return None

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
                print(f"⚠️ Error fetching options for {self.ticker} ({exp}): {e}")

        if not data:
            print(f"⚠️ No options data retrieved for {self.ticker}. Skipping.")
            return None

        self.options_data = pd.concat(data, ignore_index=True)

        # Validate required columns before further processing
        required_cols = {"strike", "openInterest", "volume"}
        if not required_cols.issubset(self.options_data.columns):
            print(f"⚠️ Missing required columns in options data for {self.ticker}. Skipping.")
            self.options_data = None
        return self.options_data

    def filter_options(self, stock_price, min_oi=500, atm_range=0.05):
        """Filter options based on open interest and moneyness."""
        if self.options_data is None or self.options_data.empty:
            print(f"⚠️ No valid options data for {self.ticker}. Skipping filtering.")
            return None

        required_cols = {"strike", "openInterest", "volume"}
        if not required_cols.issubset(self.options_data.columns):
            print(f"⚠️ Missing required columns in options data for {self.ticker}. Skipping filtering.")
            return None

        self.options_data["Moneyness"] = abs(self.options_data["strike"] - stock_price) / stock_price
        self.options_data = self.options_data[
            (self.options_data["openInterest"] >= min_oi) & 
            (self.options_data["Moneyness"] <= atm_range)
        ]

        if self.options_data.empty:
            print(f"⚠️ No options data left after filtering for {self.ticker}.")
        return self.options_data


    def process_and_save(self):
        """Save processed and sampled options chain using dynamic filename."""
        stock_price = yf.Ticker(self.ticker).history(period="1d")["Close"].iloc[-1]  
        self.filter_options(stock_price)  

        if self.options_data is None or self.options_data.empty:
            print(f"⚠️ No valid options data for {self.ticker}. Skipping save.")
            return

        processed_filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        self.options_data.to_csv(processed_filename, index=False)
        print(f"✅ Saved processed options chain data to {processed_filename}")
        
        # Apply Smart Sampling
        if "sampled_options_chain" in MODEL_PATHS["paths"]:  # Ensure key exists
            sampled_filename = MODEL_PATHS["paths"]["sampled_options_chain"].format(ticker=self.ticker)
            sampled_data = stratified_sampling(self.options_data, sample_size=500)

            if sampled_data.empty:
                print(f"⚠️ No sampled data available for {self.ticker}. Skipping sampled save.")
                return

            save_sampled_data(sampled_data, self.ticker, sampled_filename)
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
            return None
        
        # 🔹 Save the unfiltered data first
        raw_filename = MODEL_PATHS["paths"]["unfiltered_options_chain"].format(ticker=self.ticker)
        self.options_data.to_csv(raw_filename, index=False)
        print(f"✅ Saved unfiltered options chain data to {raw_filename}")

        # 🔹 Fetch stock price once
        stock_price = stock.history(period="1d")["Close"].iloc[-1]

        # 🔹 Process, save, and return **filtered and sampled** data
        return self.process_and_save(stock_price)

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

    def process_and_save(self, stock_price):
        """Filter and sample the options data, save all versions, and return the final processed dataset."""
        self.filter_options(stock_price)

        if self.options_data is None or self.options_data.empty:
            print(f"⚠️ No valid options data for {self.ticker}. Skipping save.")
            return None

        # 🔹 Save the **filtered options data**
        filtered_filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        self.options_data.to_csv(filtered_filename, index=False)
        print(f"✅ Saved filtered options chain data to {filtered_filename}")
        
        # 🔹 Apply smart sampling
        if "sampled_options_chain" in MODEL_PATHS["paths"]:  # Ensure key exists
            sampled_filename = MODEL_PATHS["paths"]["sampled_options_chain"].format(ticker=self.ticker)
            sampled_data = stratified_sampling(self.options_data, sample_size=500)

            if sampled_data.empty:
                print(f"⚠️ No sampled data available for {self.ticker}. Skipping sampled save.")
                return None

            save_sampled_data(sampled_data, self.ticker, sampled_filename)
            print(f"✅ Saved sampled options chain data to {sampled_filename}")
            
            # 🔹 Return the sampled dataset instead of raw data
            return sampled_data
        
        return self.options_data  # Default return filtered data if sampling fails

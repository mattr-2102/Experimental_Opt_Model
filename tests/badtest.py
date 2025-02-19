import yfinance as yf
import pandas as pd
import os

def download_options_data(ticker, save_path="tests"):
    """Downloads all available options data for a given ticker and saves it as a CSV."""
    os.makedirs(save_path, exist_ok=True)  # Ensure save directory exists

    stock = yf.Ticker(ticker)
    expirations = stock.options  # List of available expiration dates

    if not expirations:
        print(f"❌ No options data available for {ticker}.")
        return

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
        print(f"❌ No options data retrieved for {ticker}.")
        return

    # Combine all options data into a single DataFrame
    options_df = pd.concat(data, ignore_index=True)

    # Save to CSV
    file_path = os.path.join(save_path, f"{ticker}_options.csv")
    options_df.to_csv(file_path, index=False)

    print(f"✅ Saved {ticker} options data to {file_path}")

# Example usage
if __name__ == "__main__":
    download_options_data("SPY")  # Change ticker as needed

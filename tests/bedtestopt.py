import os
import pandas as pd
import requests

def download_options_data_alpha(ticker, save_path="tests"):
    """
    Downloads options data for a given ticker using Alpha Vantage (hypothetical endpoint)
    and saves it as a CSV.
    """
    os.makedirs(save_path, exist_ok=True)  # Ensure the save directory exists

    # Retrieve the Alpha Vantage API key from the config
    api_key = 2V6AECCNMRV5OE9J
    
    # Hypothetical endpoint URL for options data
    url = f"https://www.alphavantage.co/query?function=OPTION_CHAIN&symbol={ticker}&apikey={api_key}"
    
    print(f"Fetching options data for {ticker} from Alpha Vantage...")
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"❌ Error fetching options data for {ticker}: HTTP {response.status_code}")
        return

    data_json = response.json()
    
    # Hypothetical JSON structure:
    # {
    #   "options": [
    #       {
    #           "expiration": "2025-03-15",
    #           "calls": [ {...}, {...}, ... ],
    #           "puts": [ {...}, {...}, ... ]
    #       },
    #       ...
    #   ]
    # }
    options_list = data_json.get("options", [])
    if not options_list:
        print(f"❌ No options data available for {ticker} from Alpha Vantage.")
        return

    data_frames = []
    for option in options_list:
        expiration = option.get("expiration")
        
        # Process calls data
        calls_data = option.get("calls", [])
        if calls_data:
            calls_df = pd.DataFrame(calls_data)
            calls_df["Type"] = "Call"
            calls_df["Expiration"] = expiration
            data_frames.append(calls_df)
        
        # Process puts data
        puts_data = option.get("puts", [])
        if puts_data:
            puts_df = pd.DataFrame(puts_data)
            puts_df["Type"] = "Put"
            puts_df["Expiration"] = expiration
            data_frames.append(puts_df)
    
    if not data_frames:
        print(f"❌ No options data retrieved for {ticker}.")
        return

    # Combine all options data into a single DataFrame
    options_df = pd.concat(data_frames, ignore_index=True)

    # Save to CSV
    file_path = os.path.join(save_path, f"{ticker}_options_alpha.csv")
    options_df.to_csv(file_path, index=False)
    print(f"✅ Saved {ticker} options data to {file_path}")

# Example usage
if __name__ == "__main__":
    download_options_data_alpha("SPY")

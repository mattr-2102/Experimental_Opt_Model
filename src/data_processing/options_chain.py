# "import requests
# import pandas as pd
# from datetime import datetime, timedelta
# import time
# from requests.adapters import HTTPAdapter, Retry

# from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
# from src.data_processing.data_loader import DataLoader
# from src.data_processing.smart_sampling import stratified_sampling

# class OptionsDataProcessor:
#     def __init__(self, ticker):
#         self.ticker = ticker
#         self.itm-otm = DATA_CONFIG["data_source"]["itm-otm"]
#         #add a self.df or something similar to temporarily hold cooresponding stock data for the ticker. Data_config[paths][market_data] maybe? 

#         self.end = DATA_CONFIG["data_sources"]["end_date"]
#         self.days_back = DATA_CONFIG["data_sources"]["days_back"]
#         self.intraday_interval = DATA_CONFIG["data_sources"]["intraday_interval"]
        
#         self.key = DATA_CONFIG["data_sources"]["poly_api"]
#         self.options_data = None  # Will hold the final DataFrame
        
#     def fetch_options_data(self):
#         """
#         go through a loop to grab the full options chain for every day from (todays-date - self.days_back) to (todays-date) using 
#         within this loop, and before the api is used, grab the opening price of the stock from the first period of the current date in the loop (ie "Open" on "Date = 2024-08-26 09:30:00-04:00")
#         now calulate the acceptable window by multiplying this Open value by the self.itm-otm (ie .10) and make a lower range (Open - range) and upper range (Open + range)
#         now call the api using these values for strike_price.lt and strike_price.gt
#         use a small timeout (maybe .02?)
#         an example use of this api: https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=SPY&as_of=2025-01-10&strike_price.gt=600&strike_price.lt=700&limit=1000&sort=strike_price&apiKey=UvSnRxq0stT5Lb9p2rvGO6JBFzzBuclX 
#         note that the api returns calue "t" which is the unix msec timestamp for the open of that specific period which must be translated to a date somehow. it MUST be converted to EST similar to the market_data.py
#         store this data in a temporary holder and then iterate through ever single contract by pulling value "ticker"
        
#         for this loop, you should loop through the entire date range again but this time iterating every self.intraday_interval (ie 15 minutes)
#         this loop should search up every ticker on the temporary container (looping to search the tickers listed on each day from previous loop) and then search each ticker's self.intraday_interval value.
#         here is an example link: https://api.polygon.io/v2/aggs/ticker/O:SPY251219C00650000/range/15/minute/2023-01-09/2023-02-10?adjusted=true&sort=asc&apiKey=UvSnRxq0stT5Lb9p2rvGO6JBFzzBuclX 
#         it must be adjusted=true and sort=asc
#         if you can, pull and print the "resultsCount" to ensure progress is occuring
#         put a very small timeout to avoid flooding (maybe .02?)
#         unlike previous scripts, i am not rate limited and do not need to wait 13 seconds per query, only use the small timeout to avoid flooding
        
#         do please note that polygon limits 99 items to each list, so it will return a value at the bottom of the page labeled "next_url" with a link to the next page.
#         this link REQUIRES that the api is reinserted into the link, here is an example value for next_url: https://api.polygon.io/v3/reference/options/contracts?cursor=YXA9JTdCJTIySUQlMjIlM0ElMjIxMjg3MzM3NDg2MDUxMTI2MTE3NCUyMiUyQyUyMlN0YXJ0RGF0ZVV0YyUyMiUzQSU3QiUyMlRpbWUlMjIlM0ElMjIyMDI0LTExLTI3VDAwJTNBMDAlM0EwMFolMjIlMkMlMjJWYWxpZCUyMiUzQXRydWUlN0QlMkMlMjJFbmREYXRlVXRjJTIyJTNBJTdCJTIyVGltZSUyMiUzQSUyMjIwMjUtMDEtMTBUMDAlM0EwMCUzQTAwWiUyMiUyQyUyMlZhbGlkJTIyJTNBdHJ1ZSU3RCUyQyUyMnVuZGVybHlpbmdfdGlja2VyJTIyJTNBJTIyU1BZJTIyJTJDJTIydGlja2VyJTIyJTNBJTIyTyUzQVNQWTI1MDExMEMwMDYwNzUwMCUyMiUyQyUyMmV4cGlyYXRpb25fZGF0ZSUyMiUzQSUyMjIwMjUtMDEtMTBUMDAlM0EwMCUzQTAwWiUyMiUyQyUyMnN0cmlrZV9wcmljZSUyMiUzQTYwNy41JTJDJTIyY2ZpJTIyJTNBJTIyT0NBU1BTJTIyJTJDJTIyY29udHJhY3RfdHlwZSUyMiUzQSUyMmNhbGwlMjIlMkMlMjJleGVyY2lzZV9zdHlsZSUyMiUzQSUyMmFtZXJpY2FuJTIyJTJDJTIycHJpbWFyeV9leGNoYW5nZSUyMiUzQSU3QiUyMlN0cmluZyUyMiUzQSUyMkJBVE8lMjIlMkMlMjJWYWxpZCUyMiUzQXRydWUlN0QlMkMlMjJzaGFyZXNfcGVyX2NvbnRyYWN0JTIyJTNBMTAwJTJDJTIyYWRkaXRpb25hbF91bmRlcmx5aW5ncyUyMiUzQSUyMiU1QiU1RCUyMiU3RCZhcz0mYXNfb2Y9MjAyNS0wMS0xMCZsaW1pdD0xMDAmc29ydD10aWNrZXImdW5kZXJseWluZ190aWNrZXI9U1BZ 
        
#         lastly, ensure the dataset is not empty, rename "c" to Close, "h" to High, "l" to Low "o" to Open, "v" to Options_Volume
#         """

#     def filter_itm-otm():
#         #this function should first insert
        
#     def save_to_csv(self):
#         #check if data is empty
#         #save csv to model_paths "paths" "options_chain"
        
#     def process_options_and_save(self)
#         #this function is called with the data_loader and handles all other functions for fetching/saving within this class
#         #first call fetch_options_data()
#         #then call save_to_csv
        
# #if name == main example use case to call this script by name using hardcoded ticker SPY to test functionaility. this function should assume a cooresponding market data file for the ticker already exists, need not create a new one."


import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import pytz
from requests.adapters import HTTPAdapter, Retry
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

class OptionsDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        # Threshold for in-the-money / out-of-the-money filtering, e.g. 0.10 means +/-10% from the opening price.
        self.itm_otm = DATA_CONFIG["data_sources"].get("itm-otm", 0.10)
        self.exp_threshold = DATA_CONFIG["data_sources"]["exp_threshold"]
        
        # Ensure self.end is a datetime (if provided as a string in config, parse it)
        self.end = DATA_CONFIG["data_sources"]["end_date"]
        if isinstance(self.end, str):
            self.end = pd.to_datetime(self.end)
            
        self.days_back = DATA_CONFIG["data_sources"]["days_back"]
        self.intraday_interval = DATA_CONFIG["data_sources"]["intraday_interval"]
        self.key = DATA_CONFIG["data_sources"]["poly_api"]
        self.options_data = None  # Will hold the final DataFrame

    def fetch_options_data(self):
        BASE_URL = "https://api.polygon.io"
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        
        # Let pandas automatically parse ISO-formatted dates
        self.stock_df = pd.read_csv(
            MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker),
            parse_dates=["Date"],
            index_col="Date"
        )
        # If your CSV times are in EST but come in as naive, explicitly localize:
        self.stock_df.index = self.stock_df.index.tz_localize("US/Eastern")
        
        all_options = []  # This will collect interval data records for all contracts
        # Define the date range (calendar days)
        end_date = self.end
        start_date = end_date - timedelta(days=self.days_back)
        date_range = pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist()
        
        # Define the EST timezone once for reuse
        est = pytz.timezone("US/Eastern")
        for day in date_range:
            day_ts = pd.Timestamp(day).tz_localize("US/Eastern")
            target_dt = day_ts.replace(hour=9, minute=30)
            timestamp_ms = int(target_dt.timestamp() * 1000)

            # Convert to date string (YYYY-MM-DD)
            date_str = day_ts.strftime("%Y-%m-%d")
            
            # Convert date_str to a datetime, add timedelta, then back to string
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            expiration_date_obj = date_obj + timedelta(days=self.exp_threshold)
            exp_str = expiration_date_obj.strftime("%Y-%m-%d")

            date_obj_plus_1 = date_obj + timedelta(days=1)
            date_str_plus_1 = date_obj_plus_1.strftime("%Y-%m-%d")
            
            try:
                # Filter stock_df for 9:30 on day_ts
                day_data = self.stock_df[self.stock_df.index.normalize() == day_ts.normalize()]
                day_data = day_data[(day_data.index.hour == 9) & (day_data.index.minute == 30)]
                
                if day_data.empty:
                    print(f"No market data for {date_str}")
                    continue

                open_price = day_data.iloc[0]["Open"]
            except Exception as e:
                print(f"Error obtaining opening price for {date_str}: {e}")
                continue

            # Calculate acceptable strike range based on the opening price.
            strike_range = open_price * self.itm_otm
            lower_bound = open_price - strike_range
            upper_bound = open_price + strike_range

            # Build the URL with the expiration_date.lte filter
            contracts_url = (
                f"{BASE_URL}/v3/reference/options/contracts?"
                f"expiration_date.lte={exp_str}&"
                f"underlying_ticker={self.ticker}&as_of={date_str}"
                f"&strike_price.gt={lower_bound}&strike_price.lt={upper_bound}"
                f"&limit=1000&sort=strike_price&apiKey={self.key}"
            )
            
            temp_contracts = []  # Temporary list to hold contracts for the day
            while contracts_url:
                try:
                    resp = session.get(contracts_url, timeout=1.0)
                except requests.exceptions.RequestException as e:
                    print(f"Exception for {date_str}: {e}")
                    break
                if resp.status_code != 200:
                    print(f"HTTP error {resp.status_code} for URL: {contracts_url}")
                    break
                data = resp.json()
                if "results" in data:
                    temp_contracts.extend(data["results"])
                else:
                    print(f"No 'results' for {date_str}")
                    break
                next_url = data.get("next_url")
                if next_url:
                    contracts_url = f"{next_url}&apiKey={self.key}"
                else:
                    contracts_url = None
                time.sleep(0.02)

            # For each contract in temp_contracts, fetch its interval aggregated data.
            for contract in temp_contracts:
                contract_ticker = contract.get("ticker")
                interval_url = (
                    f"{BASE_URL}/v2/aggs/ticker/{contract_ticker}/range/{self.intraday_interval}/"
                    f"{date_str}/{date_str_plus_1}?adjusted=true&sort=asc&apiKey={self.key}"
                )
                
                
                while interval_url:
                    try:
                        hr_resp = session.get(interval_url, timeout=0.5)
                    except requests.exceptions.RequestException as e:
                        print(f"Exception fetching interval data for {contract_ticker} on {date_str}: {e}")
                        break
                    if hr_resp.status_code != 200:
                        print(f"HTTP error {hr_resp.status_code} for interval URL: {interval_url}")
                        break
                    hr_data = hr_resp.json()
                    if "results" in hr_data:
                        for record in hr_data["results"]:
                            # Append contract details and the day's opening price
                            record["contract_ticker"] = contract_ticker
                            record["expiration_date"] = contract.get("expiration_date")
                            record["strike"] = contract.get("strike_price")
                            record["contract_type"] = contract.get("contract_type")
                            record["underlying_price"] = open_price
                            all_options.append(record)
                    next_hr_url = hr_data.get("next_url")
                    if next_hr_url:
                        interval_url = f"{next_hr_url}&apiKey={self.key}"
                    else:
                        interval_url = None
            print(f"Processed {len(temp_contracts)} contracts for {date_str}.")

        if all_options:
            df = pd.DataFrame(all_options)
            # Convert the Unix millisecond timestamp in 't' to a datetime column called 'Date' in EST
            if "t" in df.columns:
                df["Date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("US/Eastern")
            # Rename columns for clarity
            df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}, inplace=True)
            self.options_data = df
        else:
            self.options_data = pd.DataFrame()

    def save_to_csv(self):
        if self.options_data is None or self.options_data.empty:
            print("No options data to save.")
            return
        filename = MODEL_PATHS["paths"]["options_chain"].format(ticker=self.ticker)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        self.options_data.to_csv(filename, index=False)
        print(f"Options data saved to {filename}.")

    def process_options_and_save(self):
        print("Fetching options data...")
        self.fetch_options_data()
        if self.options_data is None or self.options_data.empty:
            print("No options data fetched.")
            return
        self.save_to_csv()

if __name__ == "__main__":
    # Test with hardcoded ticker SPY. This assumes corresponding market data already exists.
    processor = OptionsDataProcessor("SPY")
    processor.process_options_and_save()


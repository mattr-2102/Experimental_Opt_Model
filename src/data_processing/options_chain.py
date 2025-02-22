import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

# ------------------------------------------
# Helper function to fetch one contract's interval data
# ------------------------------------------
def fetch_contract_interval_data(contract, date_str, date_str_plus_1, open_price, base_url, intraday_interval, api_key):
    """
    Fetch intraday interval data for a single contract on a specific date range.
    Returns a list of records.
    """
    contract_ticker = contract.get("ticker")
    records = []

    # We create a separate session here to avoid concurrency issues
    # with a single shared session.
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    interval_url = (
        f"{base_url}/v2/aggs/ticker/{contract_ticker}/range/{intraday_interval}/"
        f"{date_str}/{date_str_plus_1}?adjusted=true&sort=asc&apiKey={api_key}"
    )

    while interval_url:
        try:
            hr_resp = session.get(interval_url, timeout=2)
        except requests.exceptions.RequestException as e:
            print(f"[Thread] Exception fetching interval data for {contract_ticker} on {date_str}: {e}")
            break

        if hr_resp.status_code != 200:
            print(f"[Thread] HTTP error {hr_resp.status_code} for interval URL: {interval_url}")
            break

        hr_data = hr_resp.json()
        if "results" in hr_data:
            for record in hr_data["results"]:
                # Append contract details
                record["contract_ticker"] = contract_ticker
                record["expiration_date"] = contract.get("expiration_date")
                record["strike"] = contract.get("strike_price")
                record["contract_type"] = contract.get("contract_type")
                record["underlying_price"] = open_price
                records.append(record)

        next_hr_url = hr_data.get("next_url")
        if next_hr_url:
            interval_url = f"{next_hr_url}&apiKey={api_key}"
        else:
            interval_url = None

        # A tiny sleep can help avoid hitting rate limits too fast
        time.sleep(0.02)

    return records

# ------------------------------------------
# Main class for Options Data Processing
# ------------------------------------------
class OptionsDataProcessor:
    def __init__(self, ticker):
        self.ticker = ticker
        self.itm_otm = DATA_CONFIG["data_sources"].get("itm-otm", 0.10)
        self.exp_threshold = DATA_CONFIG["data_sources"]["exp_threshold"]
        
        self.end = DATA_CONFIG["data_sources"]["end_date"]
        if isinstance(self.end, str):
            self.end = pd.to_datetime(self.end)
            
        self.days_back = DATA_CONFIG["data_sources"]["days_back"]
        self.intraday_interval = DATA_CONFIG["data_sources"]["intraday_interval"]
        self.key = DATA_CONFIG["data_sources"]["poly_api"]
        self.options_data = None

    def fetch_options_data(self):
        BASE_URL = "https://api.polygon.io"
        
        # Primary session for fetching contract listings (not interval data)
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        
        # Read local stock CSV to get opening prices
        self.stock_df = pd.read_csv(
            MODEL_PATHS["paths"]["market_data"].format(ticker=self.ticker),
            parse_dates=["Date"],
            index_col="Date"
        )
        self.stock_df.index = self.stock_df.index.tz_localize("US/Eastern")
        
        all_options = []
        
        end_date = self.end
        start_date = end_date - timedelta(days=self.days_back)
        date_range = pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist()
        
        est = pytz.timezone("US/Eastern")
        for day in date_range:
            day_ts = pd.Timestamp(day).tz_localize(est)
            date_str = day_ts.strftime("%Y-%m-%d")

            # Next day date string (for interval queries)
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            date_obj_plus_1 = date_obj + timedelta(days=1)
            date_str_plus_1 = date_obj_plus_1.strftime("%Y-%m-%d")

            # Expiration date filter
            expiration_date_obj = date_obj + timedelta(days=self.exp_threshold)
            exp_str = expiration_date_obj.strftime("%Y-%m-%d")

            # Gather opening price
            try:
                day_data = self.stock_df[self.stock_df.index.normalize() == day_ts.normalize()]
                day_data = day_data[(day_data.index.hour == 9) & (day_data.index.minute == 30)]
                if day_data.empty:
                    print(f"No market data for {date_str}")
                    continue
                open_price = day_data.iloc[0]["Open"]
            except Exception as e:
                print(f"Error obtaining opening price for {date_str}: {e}")
                continue

            # Calculate acceptable strike range
            strike_range = open_price * self.itm_otm
            lower_bound = open_price - strike_range
            upper_bound = open_price + strike_range

            # Build the contracts URL
            contracts_url = (
                f"{BASE_URL}/v3/reference/options/contracts?"
                f"expiration_date.lte={exp_str}&"
                f"underlying_ticker={self.ticker}&as_of={date_str}"
                f"&strike_price.gt={lower_bound}&strike_price.lt={upper_bound}"
                f"&limit=1000&sort=strike_price&apiKey={self.key}"
            )

            temp_contracts = []
            while contracts_url:
                try:
                    resp = session.get(contracts_url, timeout=2.0)
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

            # --------------------------------------------
            # CONCURRENT FETCH OF INTERVAL DATA FOR CONTRACTS
            # --------------------------------------------
            contract_records = []  # local list for today's data
            max_workers = 50  # Control concurrency level
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit each contract fetch task
                future_to_contract = {
                    executor.submit(
                        fetch_contract_interval_data,
                        contract,
                        date_str,
                        date_str_plus_1,
                        open_price,
                        BASE_URL,
                        self.intraday_interval,
                        self.key
                    ): contract for contract in temp_contracts
                }
                for future in as_completed(future_to_contract):
                    try:
                        result = future.result()  # this is a list of records
                        contract_records.extend(result)
                    except Exception as e:
                        print(f"[ThreadPool] Error: {e}")

            print(f"Processed {len(temp_contracts)} contracts for {date_str} with concurrency.")

            # Add today's interval records to the global all_options list
            all_options.extend(contract_records)

        # ---------------------------------
        # Convert all collected records to a DataFrame
        # ---------------------------------
        if all_options:
            df = pd.DataFrame(all_options)
            if "t" in df.columns:
                df["Date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert("US/Eastern")
            # Rename columns
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
    processor = OptionsDataProcessor("SPY")
    processor.process_options_and_save()

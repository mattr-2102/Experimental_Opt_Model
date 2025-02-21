import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter, Retry

# Polygon API Key and Base URL
API_KEY = 'UvSnRxq0stT5Lb9p2rvGO6JBFzzBuclX'
BASE_URL = 'https://api.polygon.io'

# Setup a session with retry logic
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount('https://', adapter)

# Define the underlying asset and timeframe
underlying = 'SPY'
end_date = datetime.now()
start_date = end_date - timedelta(days=28)

# Dictionary to store unique contracts and their details
all_contracts = {}

# Step 1: Retrieve Historical Options Contracts for Each Day in the Last 28 Days
for days_ago in range(28):
    date_str = (end_date - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    contracts_url = (
        f'{BASE_URL}/v3/reference/options/contracts?underlying_ticker={underlying}'
        f'&as_of={date_str}&apiKey={API_KEY}'
    )
    
    try:
        response = session.get(contracts_url, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Exception for {date_str}: {e}")
        continue
        
    if response.status_code == 200:
        contracts_data = response.json()
        if 'results' in contracts_data:
            for contract in contracts_data['results']:
                ticker = contract.get('ticker')
                if ticker not in all_contracts:
                    all_contracts[ticker] = {
                        'expiration_date': contract.get('expiration_date'),
                        'strike_price': contract.get('strike_price'),
                        'contract_type': contract.get('contract_type'),
                    }
    else:
        print(f"⚠️ Failed to fetch contracts for {date_str}: HTTP {response.status_code}")
    
    time.sleep(0.2)

print(f"Total unique contracts collected: {len(all_contracts)}")

# Step 2: Fetch Hourly Aggregated Data for Each Contract for Each Day in the 28-Day Range
all_data = []
date_range = pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist()

for ticker, details in all_contracts.items():
    for day in date_range:
        date_str = day.strftime("%Y-%m-%d")
        aggs_url = (
            f'{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/hour/'
            f'{date_str}/{date_str}?apiKey={API_KEY}'
        )
        
        try:
            response = session.get(aggs_url, timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Exception for {ticker} on {date_str}: {e}")
            continue
        
        if response.status_code == 200:
            aggs_data = response.json()
            if 'results' in aggs_data:
                for result in aggs_data['results']:
                    result['ticker'] = ticker
                    result['expiration_date'] = details.get('expiration_date')
                    result['strike_price'] = details.get('strike_price')
                    result['contract_type'] = details.get('contract_type')
                    all_data.append(result)
        else:
            print(f"⚠️ Failed to fetch hourly data for {ticker} on {date_str}: HTTP {response.status_code}")
        
        time.sleep(0.05)
    time.sleep(0.1)

# Convert aggregated data to DataFrame
data_df = pd.DataFrame(all_data)

if 't' in data_df.columns:
    data_df['datetime'] = pd.to_datetime(data_df['t'], unit='ms')
    data_df.sort_values(by='datetime', inplace=True)
else:
    print("⚠️ Timestamp column 't' not found; please verify the API response structure.")

output_file = 'options_data.csv'
data_df.to_csv(output_file, index=False)
print(f"Data saved for {len(all_contracts)} contracts to {output_file}.")

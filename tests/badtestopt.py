import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Polygon API Key and Base URL
API_KEY = 'UvSnRxq0stT5Lb9p2rvGO6JBFzzBuclX'
BASE_URL = 'https://api.polygon.io'

# Define the underlying asset and timeframe
underlying = 'SPY'
end_date = datetime.now()
start_date = end_date - timedelta(days=28)

# Dictionary to store unique contracts and their details
# Key: contract ticker, Value: details dictionary
all_contracts = {}

# Step 1: Retrieve Historical Options Contracts for Each Day in the Last 28 Days
for days_ago in range(28):
    date_str = (end_date - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    contracts_url = (
        f'{BASE_URL}/v3/reference/options/contracts?underlying_ticker={underlying}'
        f'&as_of={date_str}&apiKey={API_KEY}'
    )
    
    response = requests.get(contracts_url)
    if response.status_code == 200:
        contracts_data = response.json()
        if 'results' in contracts_data:
            for contract in contracts_data['results']:
                ticker = contract.get('ticker')
                # Use the ticker as the unique key; if already present, skip or update if needed
                if ticker not in all_contracts:
                    all_contracts[ticker] = {
                        'expiration_date': contract.get('expiration_date'),
                        'strike_price': contract.get('strike_price'),
                        'contract_type': contract.get('contract_type'),  # "call" or "put"
                    }
    else:
        print(f"⚠️ Failed to fetch contracts for {date_str}: HTTP {response.status_code}")

print(f"Total unique contracts collected: {len(all_contracts)}")

# Step 2: Fetch Hourly Aggregated Data for Each Contract for Each Day in the 28-Day Range
all_data = []

# Create a list of all days in the date range (inclusive)
date_range = pd.date_range(start=start_date, end=end_date).to_pydatetime().tolist()

for ticker, details in all_contracts.items():
    for day in date_range:
        date_str = day.strftime("%Y-%m-%d")
        aggs_url = (
            f'{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/hour/'
            f'{date_str}/{date_str}?apiKey={API_KEY}'
        )
        
        response = requests.get(aggs_url)
        if response.status_code == 200:
            aggs_data = response.json()
            if 'results' in aggs_data:
                for result in aggs_data['results']:
                    result['ticker'] = ticker
                    # Append contract details for context
                    result['expiration_date'] = details.get('expiration_date')
                    result['strike_price'] = details.get('strike_price')
                    result['contract_type'] = details.get('contract_type')
                    all_data.append(result)
        else:
            print(f"⚠️ Failed to fetch hourly data for {ticker} on {date_str}: HTTP {response.status_code}")

# Convert aggregated data to DataFrame
data_df = pd.DataFrame(all_data)

# Convert the timestamp field to datetime if needed.
# Polygon's aggs API typically returns a timestamp field 't' (milliseconds since epoch)
if 't' in data_df.columns:
    data_df['datetime'] = pd.to_datetime(data_df['t'], unit='ms')
    # Sort the data by datetime in ascending order
    data_df.sort_values(by='datetime', inplace=True)
else:
    print("⚠️ Timestamp column 't' not found; please verify the API response structure.")

# Save sorted data to CSV
output_file = 'options_data.csv'
data_df.to_csv(output_file, index=False)
print(f"Data saved for {len(all_contracts)} contracts to {output_file}.")

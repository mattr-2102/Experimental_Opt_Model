import pandas as pd

# Path to your existing CSV file
input_file = "data/processed/market_data_SPY.csv"
output_file = "data/processed/market_data_SPY_naive.csv"

# Read the CSV file without automatic date parsing
df = pd.read_csv(input_file)

# Explicitly convert the Date column to datetime, assuming it contains timezone info.
# We force it to UTC (if it's not already) then convert to US/Eastern, then remove tz info.
df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
df["Date"] = df["Date"].dt.tz_convert("US/Eastern").dt.tz_localize(None)

# Save the updated DataFrame to a new CSV file
df.to_csv(output_file, index=False)
print(f"Saved updated CSV without timezone info to {output_file}.")

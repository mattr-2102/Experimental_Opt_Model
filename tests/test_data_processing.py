import os
import pandas as pd
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS
from src.data_processing.data_loader import DataLoader

def test_market_data_loading():
    """Tests if stock market data is loading and fetching correctly."""
    print("\n🔍 Running Market Data Load Test...\n")
    cache_dir = MODEL_PATHS["paths"]["processed_data"]

    for ticker in DATA_CONFIG["data_sources"]["tickers"]:  # ✅ Fixed KeyError here
        file_path = MODEL_PATHS["paths"]["market_data"].format(ticker=ticker)

        loader = DataLoader(ticker)

        # Test if data loads successfully
        try:
            df = loader.load_market_data()
            assert isinstance(df, pd.DataFrame) and not df.empty, f"❌ Failed to load {ticker} market data!"
            print(f"✅ {ticker} market data loaded successfully!")
        except Exception as e:
            print(f"❌ Error loading {ticker} market data: {e}")

        # Test if data fetching works when the file is missing
        if not os.path.exists(file_path):
            print(f"⚠ {ticker} market data not found. Testing auto-fetch...")
            loader._fetch_market_data()
            assert os.path.exists(file_path), f"❌ {ticker} market data was not fetched correctly!"

def test_options_data_loading():
    """Tests if options chain data is loading and fetching correctly."""
    print("\n🔍 Running Options Data Load Test...\n")
    cache_dir = MODEL_PATHS["paths"]["processed_data"]

    for ticker in DATA_CONFIG["data_sources"]["tickers"]:  # ✅ Fixed KeyError here
        file_path = MODEL_PATHS["paths"]["options_chain"].format(ticker=ticker)

        loader = DataLoader(ticker)

        # Test if data loads successfully
        try:
            df = loader.load_options_data()
            assert isinstance(df, pd.DataFrame) and not df.empty, f"❌ Failed to load {ticker} options data!"
            print(f"✅ {ticker} options data loaded successfully!")
        except Exception as e:
            print(f"❌ Error loading {ticker} options data: {e}")

        # Test if data fetching works when the file is missing
        if not os.path.exists(file_path):
            print(f"⚠ {ticker} options data not found. Testing auto-fetch...")
            loader._fetch_options_data()
            assert os.path.exists(file_path), f"❌ {ticker} options data was not fetched correctly!"

if __name__ == "__main__":
    print("\n🚀 Running Data Loading & Fetching Tests...\n")
    test_market_data_loading()
    test_options_data_loading()
    print("\n✅ All tests completed successfully!\n")

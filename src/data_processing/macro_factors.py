import yfinance as yf
import pandas as pd

class MacroIndicators:
    def __init__(self):
        self.data = {}

    def fetch_vix(self):
        """Fetch VIX (Market Volatility) index"""
        self.data["VIX"] = yf.download("^VIX", period="5y")["Close"]

    def fetch_move(self):
        """Fetch MOVE index (Bond Market Volatility)"""
        self.data["MOVE"] = yf.download("^MOVE", period="5y")["Close"]

    def fetch_vvix(self):
        """Fetch VVIX (VIX of VIX)"""
        self.data["VVIX"] = yf.download("^VVIX", period="5y")["Close"]

    def fetch_fed_statements(self):
        """Fetch Fed statements (placeholder for API-based source)"""
        self.data["Fed_Statements"] = "Not Implemented"

    def save_to_csv(self, filename="data/processed/macro_indicators.csv"):
        """Save macro indicators data"""
        df = pd.DataFrame(self.data)
        df.to_csv(filename)
        print(f"Saved macro indicators data to {filename}")

# Example Usage:
if __name__ == "__main__":
    macro = MacroIndicators()
    macro.fetch_vix()
    macro.fetch_move()
    macro.fetch_vvix()
    macro.save_to_csv()

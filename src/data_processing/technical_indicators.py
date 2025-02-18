import ta  # Technical Indicators
import pandas_ta as pta  # More TA options

def add_technical_indicators(df):
    """Compute key technical indicators."""
    df["SMA_50"] = pta.sma(df["Close"], length=50)
    df["SMA_200"] = pta.sma(df["Close"], length=200)
    df["EMA_20"] = pta.ema(df["Close"], length=20)
    df["MACD"] = ta.trend.MACD(df["Close"]).macd()
    df["RSI"] = ta.momentum.RSIIndicator(df["Close"]).rsi()
    df["ADX"] = ta.trend.ADXIndicator(high=df["High"], low=df["Low"], close=df["Close"]).adx()
    df["Bollinger_High"] = ta.volatility.BollingerBands(df["Close"]).bollinger_hband()
    df["Bollinger_Low"] = ta.volatility.BollingerBands(df["Close"]).bollinger_lband()
    df["VWAP"] = ta.volume.VolumeWeightedAveragePrice(df["High"], df["Low"], df["Close"], df["Volume"]).volume_weighted_average_price()
    return df

import ta  # Technical Indicators
import pandas_ta as pta  # More TA options

def add_technical_indicators(df):
    """Compute key technical indicators."""

    # Ensure Close, High, Low, and Volume exist before applying indicators
    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"❌ Missing required columns: {missing_cols}")

    # Moving Averages
    df["SMA_50"] = pta.sma(df["Close"], length=50)
    df["SMA_200"] = pta.sma(df["Close"], length=200)
    df["EMA_20"] = pta.ema(df["Close"], length=20)

    # MACD (Extract as Series)
    macd = ta.trend.MACD(df["Close"])
    df["MACD"] = macd.macd().squeeze()  # Convert from 2D to 1D

    # RSI
    df["RSI"] = ta.momentum.RSIIndicator(df["Close"]).rsi()

    # ADX (Extract only ADX line)
    adx = ta.trend.ADXIndicator(high=df["High"], low=df["Low"], close=df["Close"])
    df["ADX"] = adx.adx().squeeze()  # Convert from 2D to 1D

    # Bollinger Bands (Extract as Series)
    bollinger = ta.volatility.BollingerBands(df["Close"])
    df["Bollinger_High"] = bollinger.bollinger_hband().squeeze()
    df["Bollinger_Low"] = bollinger.bollinger_lband().squeeze()

    # VWAP (Ensure it's a Series)
    vwap = ta.volume.VolumeWeightedAveragePrice(
        high=df["High"], low=df["Low"], close=df["Close"], volume=df["Volume"]
    )
    df["VWAP"] = vwap.volume_weighted_average_price().squeeze()

    return df

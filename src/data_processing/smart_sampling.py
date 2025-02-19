import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

def compute_liquidity_score(df):
    """Compute liquidity score based on open interest and volume."""
    df["Liquidity_Score"] = df["openInterest"] * df["volume"]
    return df

def stratified_sampling(df, sample_size=1000):
    """Perform stratified sampling based on liquidity score."""
    df = compute_liquidity_score(df)
    
    # Categorize contracts based on liquidity deciles
    df["Liquidity_Bins"] = pd.qcut(df["Liquidity_Score"], q=10, labels=False, duplicates='drop')

    # Stratified Sampling: Ensures each liquidity bin is proportionally represented
    sss = StratifiedShuffleSplit(n_splits=1, test_size=sample_size / len(df), random_state=42)
    for train_idx, _ in sss.split(df, df["Liquidity_Bins"]):
        sampled_df = df.iloc[train_idx]

    return sampled_df.drop(columns=["Liquidity_Bins"])  # Remove temporary bin column

def save_sampled_data(df, ticker, path_template):
    """Save the sampled options data to a CSV file."""
    filename = path_template.format(ticker=ticker)
    df.to_csv(filename, index=False)
    print(f"✅ Saved sampled options data to {filename}")

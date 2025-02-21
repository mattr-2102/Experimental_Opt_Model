import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

def compute_liquidity_score(df):
    """Compute liquidity score based on open interest and volume."""
    if df[["openInterest", "volume"]].isnull().any().any():
        raise ValueError("❌ Missing values detected in 'openInterest' or 'volume'. Data must be cleaned before processing.")
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

    return sampled_df.drop(columns=["Liquidity_Bins"])

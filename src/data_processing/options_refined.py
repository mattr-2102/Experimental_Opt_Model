import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.config.config_loader import DATA_CONFIG, MODEL_PATHS

class OptionsRefinedDataProcessor:
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
        
    def 
    def refine_options_sample:
        
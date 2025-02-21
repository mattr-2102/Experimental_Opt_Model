import yaml
import datetime

def now_constructor(loader, node):
    return datetime.datetime.now()

yaml.SafeLoader.add_constructor('!now', now_constructor)

def load_config(file_path):
    """Load a YAML configuration file."""
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

DATA_CONFIG = load_config("src/config/data_sources.yaml")
MODEL_PATHS = load_config("src/config/model_paths.yaml")

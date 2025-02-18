import yaml

def load_config(file_path):
    """Load a YAML configuration file."""
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

# Load specific configurations
DATA_CONFIG = load_config("src/config/data_sources.yaml")
MODEL_PATHS = load_config("src/config/model_paths.yaml")

if __name__ == "__main__":
    print("✅ Data Config:", DATA_CONFIG)
    print("✅ Model Paths:", MODEL_PATHS)

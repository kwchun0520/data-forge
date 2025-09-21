import yaml

def load_yaml(file_path: str) -> dict:
    """Load schema from a YAML file.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        dict: Schema definition.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return config
    
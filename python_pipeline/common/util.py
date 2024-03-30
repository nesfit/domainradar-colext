import os


def ensure_data_dir():
    """Creates the directory specified by the environment variable APP_DATADIR if it does not exist."""
    data_dir = os.getenv("APP_DATADIR")
    if data_dir is not None and not os.path.exists(data_dir):
        os.makedirs(data_dir)

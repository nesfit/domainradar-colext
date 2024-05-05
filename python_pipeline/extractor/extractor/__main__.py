from common import ensure_data_dir
from .app import extractor_app

if __name__ == '__main__':
    ensure_data_dir()
    extractor_app.main()

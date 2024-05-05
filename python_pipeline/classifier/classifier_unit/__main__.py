from common import ensure_data_dir
from .app import classifier_app

if __name__ == '__main__':
    ensure_data_dir()
    classifier_app.main()

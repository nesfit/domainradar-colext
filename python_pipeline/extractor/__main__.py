from common.util import ensure_data_dir
from .app import app

if __name__ == '__main__':
    ensure_data_dir()
    app.main()

from common import ensure_data_dir
from .rtt import rtt_app

if __name__ == '__main__':
    ensure_data_dir()
    rtt_app.main()

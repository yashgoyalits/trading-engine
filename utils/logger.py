import logging
import os
from datetime import datetime

LOG_DIR = os.path.join(os.getcwd(), "logger_files", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Log file with today's date (YYYY-MM-DD.log)
today_str = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"main_script_{today_str}.log")

# Reset root handlers (avoids duplicate SDK handlers)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Base logging setup
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    filename=LOG_FILE,
    filemode='a'
)

# Console handler (optional if you still want logs in terminal)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s'))
logging.getLogger("").addHandler(console)

# Your project logger
logger = logging.getLogger(__name__)

import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('scraping_logger')
logger.setLevel(logging.INFO)

# Rotate when file reaches 5 MB, keep 3 backups
handler = RotatingFileHandler('scraping.log', maxBytes=5*1024*1024, backupCount=3)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

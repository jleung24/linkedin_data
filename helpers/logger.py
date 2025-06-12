import logging
import sys

logger = logging.getLogger('scraping_logger')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

console_handler = logging.StreamHandler(sys.stdout)  # <-- use sys.stdout
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

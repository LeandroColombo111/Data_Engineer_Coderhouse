import logging
import os
from datetime import datetime

def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(filename=log_filename,
                        level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    logging.info(f"Logging setup complete. Logs will be stored in {log_filename}")

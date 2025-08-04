import argparse
import os
import sys
import logging
import datetime



class Logger:
    def __init__(self,notebook_name):
        self.notebook_name = notebook_name
        self.PROJECT_PATH = '//'

    def setup_logger(self):
        logger = logging.getLogger('UberProcessor')
        logger.setLevel(logging.INFO)

        # Create console handler that prints to stdout
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)

        # Create file handler for persistent logs
        log_dir = os.path.join(self.PROJECT_PATH,'logs')
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(
            os.path.join(log_dir, f"{self.notebook_name}_{datetime.datetime.now().strftime('%Y%m%d')}.log")
        )
        file_handler.setLevel(logging.DEBUG)

        # Create formatter and add to handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stdout_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(stdout_handler)
        logger.addHandler(file_handler)

        return logger
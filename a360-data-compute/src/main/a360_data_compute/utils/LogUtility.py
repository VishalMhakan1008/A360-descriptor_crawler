import os
import logging


class LogUtility:
    def __init__(self, log_file_path='/Users/Platform3/Documents/AHM_DOC/pythonProject/logs/app.log'):
        log_directory = os.path.dirname(log_file_path)

        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        #file handler
        fh = logging.FileHandler(log_file_path)
        fh.setLevel(logging.DEBUG)

        #Stream Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def log_info(self, message):
        self.logger.info(message)

    def log_warning(self, message):
        self.logger.warning(message)

    def log_error(self, message, exc_info=False):
        self.logger.error(message, exc_info=exc_info)

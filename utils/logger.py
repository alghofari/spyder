import logging


class Logger:
    def __init__(self, name=__name__, level=logging.INFO, log_format='%(asctime)s - %(levelname)s - %(message)s'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Check if the logger already has handlers to avoid duplicate logs
        if not self.logger.hasHandlers():
            formatter = logging.Formatter(log_format)

            # Console handler
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def get_logger(self):
        return self.logger

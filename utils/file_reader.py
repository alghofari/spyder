import os

from utils.logger import Logger


class FileReader:
    def __init__(self):
        # Configure logging
        self.logger = Logger().get_logger()

    def read_file(self, filepath):
        self.logger.debug("Attempting to read file: %s", filepath)
        try:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File not found: {filepath}")

            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()

            self.logger.info("Successfully read file: %s", filepath)
            return content

        except FileNotFoundError as e:
            self.logger.error("FileNotFoundError: %s", e)
            raise

        except Exception as e:
            self.logger.error("An error occurred: %s", e)
            raise

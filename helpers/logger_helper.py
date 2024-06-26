import logging
import sys
import traceback
import linecache
from google.cloud import storage
from datetime import date

class GCSLogger:
    def __init__(self, bucket_name, log_prefix, official_name):
        self.bucket_name = bucket_name
        self.log_prefix = log_prefix
        self.official_name = official_name
        self.client = storage.Client()

        # Configure the logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        # Create a GCS logger handler
        gcs_handler = logging.StreamHandler()
        gcs_handler.setLevel(logging.INFO)
        gcs_logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        gcs_handler.setFormatter(gcs_logger_formatter)

        # Create an error handler
        error_handler = logging.StreamHandler()
        error_handler.setLevel(logging.ERROR)

        # Add handlers to the logger
        self.logger.addHandler(gcs_handler)
        self.logger.addHandler(error_handler)

        # Set up sys.excepthook to capture unhandled exceptions
        sys.excepthook = self.log_unhandled_exception

    def _upload_to_gcs(self, log_filename, log_content):
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(log_filename)
        current_content = blob.download_as_text() if blob.exists() else ""
        new_content = current_content + log_content
        blob.upload_from_string(new_content)

    def get_current_timestamp(self):
        today_date = date.today()
        return today_date.strftime("%Y-%m-%d")

    def log(self, message):
        log_content = f"{self.get_current_timestamp()}: {message}\n"
        log_filename = f"{self.log_prefix}/{self.get_current_timestamp()}/logs_{self.official_name}.txt"
        self._upload_to_gcs(log_filename, log_content)
        self.logger.info(message)

    def error(self, error_message):
        log_content = f"{self.get_current_timestamp()}: Error: {error_message}\n"
        log_filename = f"{self.log_prefix}/{self.get_current_timestamp()}/logs_{self.official_name}.txt"
        self._upload_to_gcs(log_filename, log_content)
        self.logger.error(error_message)

    def log_unhandled_exception(self, exc_type, exc_value, exc_traceback):
        exception_message = f"Unhandled Exception: {exc_type.__name__}: {exc_value}"
        
        # Extract the line number where the exception occurred
        line_number = exc_traceback.tb_lineno
        line_content = linecache.getline(exc_traceback.tb_frame.f_code.co_filename, line_number)
        
        formatted_exception = traceback.format_exception(exc_type, exc_value, exc_traceback)
        exception_with_traceback = ''.join(formatted_exception)
        
        self.error(f"{exception_message}\n{exception_with_traceback}\nLine {line_number}: {line_content}")
        self.logger.exception(exception_message)

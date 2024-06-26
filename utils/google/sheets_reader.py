import os

import gspread

from utils.logger import Logger


class GoogleSheetsReader:
    """A class to read configuration data from a Google Sheets spreadsheet."""

    def __init__(self, credentials_file=None, spreadsheet_key=None, worksheet_name=None):
        """
        Initialize the config reader.

        Args:
            credentials_file (str): Path to the service account credentials file.
            spreadsheet_key (str): The key of the Google Sheets spreadsheet.
            worksheet_name (str): The name of the worksheet to read.
        """
        self.logger = Logger().get_logger()
        self.credentials_file = credentials_file or os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        self.spreadsheet_key = spreadsheet_key
        self.worksheet_name = worksheet_name

    def read_worksheet(self):
        """Read and return configuration data from the spreadsheet."""
        try:
            gc = gspread.service_account(filename=self.credentials_file)
            sh = gc.open_by_key(self.spreadsheet_key)
            worksheet = sh.worksheet(self.worksheet_name)
            self.logger.info("Successfully read worksheet from Google Sheets.")
            return worksheet
        except gspread.exceptions.GSpreadException as e:
            self.logger.error(f"Error reading from Google Sheets: {e}")
            raise  # Re-raise the exception so it can be handled further if needed
        except FileNotFoundError:
            self.logger.error(f"Credentials file not found: {self.credentials_file}")
            raise  # Re-raise the exception so it can be handled further if needed

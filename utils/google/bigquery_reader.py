from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

from utils.logger import Logger


class BigQueryReader:
    def __init__(self, project=None):
        self.client = bigquery.Client(project=project)

        # Configure logging
        self.logger = Logger().get_logger()

    def execute_query(self, query):
        try:
            self.logger.debug(f"Executing query: {query}")
            query_job = self.client.query(query)
            result = query_job.result()  # Waits for job to complete.
            self.logger.debug("Query executed successfully")
            return result
        except GoogleAPIError as e:
            self.logger.error(f"Google API Error: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            raise

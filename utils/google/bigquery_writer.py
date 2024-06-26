import json

from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import bigquery

from utils.logger import Logger


class BigQueryWriter:
    TIME_PARTITIONING_MAPPING = {
        "DAY": bigquery.TimePartitioningType.DAY,
        "MONTH": bigquery.TimePartitioningType.MONTH,
        "YEAR": bigquery.TimePartitioningType.YEAR
    }

    def __init__(self, schema_path, table_id, create_disposition, write_disposition, time_partitioning_type=None,
                 partition_field=None):
        self.schema_path = schema_path
        self.table_id = table_id
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.time_partitioning_type = self._get_time_partitioning_type(time_partitioning_type)
        self.partition_field = partition_field
        self.client = bigquery.Client()
        self.schema_json = self._load_schema()
        self.job_config = self._create_job_config()

        # Configure logging
        self.logger = Logger().get_logger()

    def _get_time_partitioning_type(self, time_partitioning_type_str):
        if time_partitioning_type_str:
            partitioning_type = self.TIME_PARTITIONING_MAPPING.get(time_partitioning_type_str.upper())
            if not partitioning_type:
                raise ValueError(f"Invalid time partitioning type: {time_partitioning_type_str}")
            return partitioning_type
        return None

    def _load_schema(self):
        try:
            with open(self.schema_path, 'r') as schema_file:
                schema_json = json.load(schema_file)
            return schema_json
        except FileNotFoundError:
            self.logger.error(f"Schema file {self.schema_path} not found.")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Error decoding JSON from schema file {self.schema_path}.")
            raise

    def _create_job_config(self):
        try:
            job_config = bigquery.LoadJobConfig(
                schema=self.schema_json,
                create_disposition=self.create_disposition,
                write_disposition=self.write_disposition
            )
            if self.time_partitioning_type and self.partition_field:
                job_config.time_partitioning = bigquery.TimePartitioning(
                    type_=self.time_partitioning_type,
                    field=self.partition_field
                )
            return job_config
        except Exception as e:
            self.logger.error(f"Error creating job configuration: {e}")
            raise

    def write_to_table(self, dataframe):
        try:
            job = self.client.load_table_from_dataframe(dataframe, self.table_id, job_config=self.job_config)
            job.result()  # Waits for the job to complete
            self.logger.info(f"Data successfully written to {self.table_id}.")
        except GoogleAPICallError as e:
            self.logger.error(f"Google API error: {e}")
            raise
        except NotFound as e:
            self.logger.error(f"Table not found: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise

# Import all the dependencies package
import json
import re
import time

from helpers.date_helper import get_today
from helpers.logger_helper import GCSLogger
from helpers.cloud_storage_helper import detect_file, list_blob_gcs

from extract.tokopedia.seller_center.config import generate_tkpd_config
from transform.tokopedia.seller_center.sales.dataframe_processing import extract_data_into_dataframe

def main(config, gcs_logger=None):
    # Define os name config
    if isinstance(config, str):
        config = generate_tkpd_config(config)

    official_store = config[3]

    if gcs_logger == None:
        # Initialize logging
        gcs_logger = GCSLogger(bucket_name   = "sirclo-data-marketplace",
                            log_prefix    = f"assets/logs/Tokopedia/seller_center_page",
                            official_name = re.sub(r"[^A-Za-z]", "_", official_store.lower()))
    
    gcs_logger.log(f"------------------- ORDER PAGE -------------------")
    gcs_logger.log(f"Start transform order data for {official_store} with new approach")

    # Define base path config
    base_config_path = "./transform/tokopedia/seller_center/sales/resources/"
    
    # Detect filename for each config
    config_json    = base_config_path + "config_sales.json"
    validator_json = base_config_path + "validator.json"
    schema_json    = base_config_path + "schema.json"

    # Define the configuration
    config_json = open(config_json)
    config      = json.load(config_json)

    # Get parameter for google cloud environment
    bucket           = config["gcp_parameter"]["bucket"]
    dataset          = config["gcp_parameter"]["dataset"]
    dataset_append   = config["gcp_parameter"]["dataset_append"]
    bq_project       = config["gcp_parameter"]["bq_project"]
    number_of_bucket = config["gcp_parameter"]["number_of_bucket"]

    table           = config["tokopedia_order"]
    sheet_name      = table["sheet_name"]
    key_column      = table["key_column"]
    order_by_column = table["order_by_column"]

    # Define Prefix Name for Scrapping Bucket
    prefix_scrape_directory = f"assets/excel/tokopedia/sales/{official_store}/{get_today()}/"

    # Detect Scrapping Bucket
    detection_scrape_bucket = detect_file(bucket, prefix_scrape_directory, number_of_bucket)

    if detection_scrape_bucket:
        # Get filename that will be extracted
        detect_filename = list_blob_gcs(bucket, prefix_scrape_directory)[0].split('/')[-1]
        gcs_logger.log(
            f"File sales {detect_filename} is detected, process clean and upload file will be continue")
        
        # Define official_store to string only
        table_os = re.sub(r"[^A-Za-z]", "_", official_store.lower())

        # Define table id for temp table
        table_id        = f"{dataset}_tokopedia_order_{table_os}_temp"

        # Processing data to clean, validate, and upload
        extract_data_into_dataframe(
            prefix_name           = prefix_scrape_directory,
            validator_schema_path = validator_json,
            fix_schema_path       = schema_json,
            tokopedia_sheet_name  = sheet_name,
            key_column            = key_column,
            order_by_column       = order_by_column,
            bq_project            = bq_project,
            dataset               = dataset,
            dataset_append        = dataset_append,
            table_id              = table_id,
            gcs_logger            = gcs_logger
        )
        time.sleep(5)

        gcs_logger.log("Finish transform sales data")
    else:
        gcs_logger.log(
            f"File sales is not detected, please check the extract process")
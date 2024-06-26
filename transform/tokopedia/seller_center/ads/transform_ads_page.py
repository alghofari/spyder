# --- Import all the packages ---
import json
import re

from helpers.date_helper import get_today
from helpers.logger_helper import GCSLogger
from helpers.cloud_storage_helper import detect_file, list_blob_gcs
from extract.tokopedia.seller_center.config import generate_tkpd_config

# --- Function to transform ads data ---
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
    
    gcs_logger.log(f"------------------- ADS PAGE -------------------")
    gcs_logger.log(f"Start transform ads data for {official_store} with new approach")

    # Define base path config
    base_config_path = "./transform/tokopedia/seller_center/ads/resources/"

    # Detect filename for each config
    config_json = base_config_path + "config_ads.json"

    # Define the configuration for each table
    config_json = open(config_json)
    config = json.load(config_json)

    # Get parameter for google cloud environment
    dataset        = config["gcp_parameter"]["dataset"]
    bq_project     = config["gcp_parameter"]["bq_project"]
    list_of_sheets = config["list_of_sheets"]

    # Define Prefix Name for Scrapping Bucket
    prefix_scrape_directory = f"assets/excel/tokopedia/ads/{official_store}/{get_today()}/"

    # Detect Scrapping Bucket
    detection_scrape_bucket = detect_file(
        bucket_name      = "sirclo-data-marketplace",
        prefix_path_name = prefix_scrape_directory,
        number_of_bucket = 0
    )

    if detection_scrape_bucket:
        # Get filename that will be extracted
        detect_filename = list_blob_gcs("sirclo-data-marketplace",
                                        prefix_scrape_directory)[0].split('/')[-1]
        gcs_logger.log(
            f"File ads {detect_filename} is detected, process clean and upload file will be continue")

        for sheet in list_of_sheets:
            # Define table id for temp table
            sheet_name = list_of_sheets[sheet]["sheet_name"]
            main_table = list_of_sheets[sheet]["main_table"]

            from transform.tokopedia.seller_center.ads.dataframe_processing import extract_data_ads_into_dataframe
            extract_data_ads_into_dataframe(
                prefix_name         = prefix_scrape_directory,
                sheet_name          = sheet_name,
                sheet_indicator     = sheet,
                official_store_name = official_store,
                bq_project          = bq_project,
                dataset             = dataset,
                main_table          = main_table,
                gcs_logger          = gcs_logger
            )
    else:
        gcs_logger.log(
            f"File ads is not detected, please check the extract process")
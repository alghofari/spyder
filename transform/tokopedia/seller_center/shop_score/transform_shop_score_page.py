# --- Import all the packages ---
import re
import json

from helpers.date_helper import get_today
from helpers.logger_helper import GCSLogger
from helpers.cloud_storage_helper import detect_file, list_blob_gcs
from extract.tokopedia.seller_center.config import generate_tkpd_config

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

    gcs_logger.log(f"------------------- SHOP SCORE PAGE -------------------")
    gcs_logger.log(f"Start transform operationals data for {official_store} with new approach")

    # Define base path config
    base_config_path = f"./transform/tokopedia/seller_center/shop_score/resources/"

    config_json = base_config_path + f"config_shop_score.json"
    config_json = open(config_json)
    config = json.load(config_json)

    # Get parameter for google cloud environment
    dataset         = config["gcp_parameter"]["dataset"]
    dataset_append  = config["gcp_parameter"]["dataset_append"]
    bq_project      = config["gcp_parameter"]["bq_project"]
    main_table      = config["gcp_parameter"]["main_table"]
    key_column      = config["gcp_parameter"]["key_column"]
    order_by_column = config["gcp_parameter"]["order_by_column"]

    # Define official_store to string only for temp table
    table_os = re.sub(r"[^A-Za-z]", "_", official_store.lower())
    temp_table = f"{dataset}_{main_table}_{table_os}_temp"

    # Define Prefix Name for Scrapping Bucket
    prefix_scrape_directory = f"assets/json/tokopedia/shop_score/{official_store}/{get_today()}/"

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
            f"File shop_score {detect_filename} is detected, process clean and upload file will be continue")

        # Get the dataframe
        from transform.tokopedia.seller_center.shop_score.dataframe_processing import extract_data_shop_score_into_dataframe
        extract_data_shop_score_into_dataframe(
            prefix_name     = prefix_scrape_directory,
            official_store  = official_store,
            main_table      = main_table,
            table_id        = temp_table,
            key_column      = key_column,
            order_by_column = order_by_column,
            bq_project      = bq_project,
            dataset         = dataset,
            dataset_append  = dataset_append,
            gcs_logger      = gcs_logger
        )
    else:
        gcs_logger.log(
            f"File shop_score is not detected, please check the extract process")
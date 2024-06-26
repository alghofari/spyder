# --- Import all the packages ---
import json
import os
import re
import time
import traceback

from extract.tokopedia.seller_center.config import generate_tkpd_config
from helpers.cloud_storage_helper import detect_file, list_blob_gcs
from helpers.date_helper import get_today
from helpers.logger_helper import GCSLogger


# --- Set the function for main transform downloaded data into bq main table ---
def main(config, page_part, gcs_logger=None):
    # Define os name config
    if isinstance(config, str):
        config = generate_tkpd_config(config)

    official_store = config[3]

    if gcs_logger is None:
        # Initialize logging
        gcs_logger = GCSLogger(bucket_name="sirclo-data-marketplace",
                               log_prefix=f"assets/logs/Tokopedia/seller_center_page",
                               official_name=re.sub(r"[^A-Za-z]", "_", official_store.lower()))

    gcs_logger.log(f"------------------- STATISTICS PAGE -------------------")
    gcs_logger.log(f"Start transform statistics data for {official_store} with new approach")

    # Define official_store to string only
    table_os = re.sub(r"[^A-Za-z]", "_", official_store.lower())

    current_directory = os.getcwd()
    json_identifier_path = f"{current_directory}/transform/tokopedia/seller_center/statistic/resources/page.json"

    with open(json_identifier_path, "r") as file:
        json_identifier = json.load(file)

    if page_part != "all":
        identificator = [page_part]
    else:
        identificator = list(json_identifier['statistic'].keys())

    for page in identificator:
        identification = json_identifier['statistic'][page]
        name_homepage = identification['homepage']
        header = identification['header']
        assets_file = name_homepage.lower().replace(" ", "_")

        # Define base path config
        base_config_path = f"./transform/tokopedia/seller_center/statistic/{header}/resources/"

        # Detect filename for each config
        config_json = base_config_path + f"config_{header}.json"

        # Define the configuration for each table
        config_json = open(config_json)
        config = json.load(config_json)

        # Get parameter for google cloud environment
        dataset = config["gcp_parameter"]["dataset"]
        dataset_append = config["gcp_parameter"]["dataset_append"]
        bq_project = config["gcp_parameter"]["bq_project"]

        list_of_sheets = config["list_of_sheets"]

        # Define Prefix Name for Scrapping Bucket
        prefix_scrape_directory = f"assets/excel/tokopedia/{assets_file}/{official_store}/{get_today()}/"

        # Detect Scrapping Bucket
        detection_scrape_bucket = detect_file(
            bucket_name="sirclo-data-marketplace",
            prefix_path_name=prefix_scrape_directory,
            number_of_bucket=0
        )

        if detection_scrape_bucket:
            # Get filename that will be extracted
            detect_filename = list_blob_gcs("sirclo-data-marketplace",
                                            prefix_scrape_directory)[0].split('/')[-1]
            gcs_logger.log(
                f"File {assets_file} {detect_filename} is detected, process clean and upload file will be continue")

            for sheet in list_of_sheets:
                gcs_logger.log(f"Start to define sheet {sheet}")
                main_table = list_of_sheets[sheet]["main_table"]

                # Define table id for temp table
                table_id = f"{dataset}_{main_table}_{table_os}_temp"

                key_column = list_of_sheets[sheet]["key_column"]
                order_by_column = list_of_sheets[sheet]["order_by_column"]

                # Define sheet excel
                sheet_excel_name = sheet.title()

                # Get the dataframe
                if header == "overview":
                    try:
                        from transform.tokopedia.seller_center.statistic.overview.dataframe_processing import \
                            extract_data_overview_into_dataframe
                        extract_data_overview_into_dataframe(prefix_scrape_directory, official_store, sheet_excel_name,
                                                             main_table, table_id, key_column, order_by_column,
                                                             bq_project, dataset, dataset_append, gcs_logger)
                        time.sleep(10)
                    except Exception as e:
                        traceback_str = traceback.format_exc()
                        gcs_logger.error(traceback_str)
                        gcs_logger.error(e)

                elif header == "buyer":
                    try:
                        from transform.tokopedia.seller_center.statistic.buyer.dataframe_processing import \
                            extract_data_buyer_into_dataframe
                        extract_data_buyer_into_dataframe(prefix_scrape_directory, official_store, sheet_excel_name,
                                                          main_table, table_id, key_column, order_by_column, bq_project,
                                                          dataset, dataset_append, gcs_logger)
                        time.sleep(10)
                    except Exception as e:
                        traceback_str = traceback.format_exc()
                        gcs_logger.error(traceback_str)
                        gcs_logger.error(e)

                elif header == "products":
                    try:
                        from transform.tokopedia.seller_center.statistic.products.dataframe_processing import \
                            extract_data_products_into_dataframe
                        extract_data_products_into_dataframe(prefix_scrape_directory, official_store, sheet_excel_name,
                                                             main_table, table_id, key_column, order_by_column,
                                                             bq_project, dataset, dataset_append, gcs_logger)
                        time.sleep(10)
                    except Exception as e:
                        traceback_str = traceback.format_exc()
                        gcs_logger.error(traceback_str)
                        gcs_logger.error(e)

                elif header == "traffic":
                    try:
                        from transform.tokopedia.seller_center.statistic.traffic.dataframe_processing import \
                            extract_data_traffic_into_dataframe
                        extract_data_traffic_into_dataframe(prefix_scrape_directory, official_store, sheet_excel_name,
                                                            main_table, table_id, key_column, order_by_column,
                                                            bq_project, dataset, dataset_append, gcs_logger)
                        time.sleep(10)
                    except Exception as e:
                        traceback_str = traceback.format_exc()
                        gcs_logger.error(traceback_str)
                        gcs_logger.error(e)

                elif header == "video":
                    try:
                        from transform.tokopedia.seller_center.statistic.video.dataframe_processing import \
                            extract_data_video_into_dataframe
                        extract_data_video_into_dataframe(prefix_scrape_directory, official_store, sheet_excel_name,
                                                          main_table, table_id, key_column, order_by_column, bq_project,
                                                          dataset, dataset_append, gcs_logger)
                        time.sleep(10)
                    except Exception as e:
                        traceback_str = traceback.format_exc()
                        gcs_logger.error(traceback_str)
                        gcs_logger.error(e)
        else:
            gcs_logger.log(
                f"File {assets_file} is not detected, please check the extract process")

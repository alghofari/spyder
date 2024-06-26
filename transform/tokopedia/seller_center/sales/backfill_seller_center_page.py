# Import all the dependencies package
import json, re, time

from datetime import datetime, timedelta

from helpers.date_helper import get_today
from helpers.cloud_storage_helper import detect_file, list_blob_gcs
from helpers.bigquery_helper import delete_table

from transform.tokopedia.seller_center.dataframe_processing import processing
from transform.tokopedia.seller_center.load_query_processing import merge_query
from helpers.creds_helper import get_config
from helpers.logger_helper import GCSLogger

# Get parameter for tokopedia scrapping
def main(os_key, date_first, range_date):
    print(f"Start to backfill data for {os_key} from date {date_first} with interval {range_date}")
    # Define os name config
    creds_config = get_config(os_key, "tokopedia")

    official_store_list = []
    if os_key != 'all':
        os_name = creds_config['os_name']
        official_store_list.append(os_name)
    else:
        official_store_list = creds_config

    # Define base path config
    base_config_path = "./transform/tokopedia/seller_center/"
    
    # Detect filename for each config
    config_json = base_config_path + "seller_center.json"
    validator_json = base_config_path + "validator.json"
    schema_json = base_config_path + "schema.json"

    # Define the configuration
    config_json = open(config_json)
    config = json.load(config_json)

    # Get parameter for google cloud environment
    bucket         = config["gcp_parameter"]["bucket"]
    dataset        = config["gcp_parameter"]["dataset"]
    dataset_append = config["gcp_parameter"]["dataset_append"]
    bq_project     = config["gcp_parameter"]["bq_project"]
    number_of_bucket = config["gcp_parameter"]["number_of_bucket"]

    table = config[f"tokopedia_order"]
    sheet_name = table["sheet_name"]
    key_column = table["key_column"]
    order_by_column = table["order_by_column"]

    # Generate backfill date
    date_first = datetime.strptime(date_first, "%Y-%m-%d")

    date_list  = []
    for x in range(range_date):
        date = date_first + timedelta(x)
        date_string = datetime.strftime(date, "%Y-%m-%d")
        date_list.append(date_string)

    # Define Prefix Name for Scrapping Bucket
    official_store_list = ["Garnier Men Official Store"]
    
    for official_store in official_store_list:
        # Define official_store to string only
        table_os = re.sub(r"[^A-Za-z]", "_", official_store.lower())

        # Define table id for temp table
        table_id = f"{dataset}_tokopedia_order_{table_os}_temp"
        temp_table_path = f"{bq_project}.{dataset_append}.{table_id}"
        print(temp_table_path)

        date_list = ["2023-08-04"]

        for date in date_list:
            print(f"Start to generate and backfill data for date {date}")
            # Initialize logging
            gcs_logger = GCSLogger(bucket_name="sirclo-data-marketplace",
                                log_prefix=f"assets/logs/Tokopedia/seller_center",
                                official_name=re.sub(r"[^A-Za-z]", "_", official_store.lower()))
            
            print(f"Start transform data for {official_store}")

            prefix_scrape_directory = f"assets/excel/tokopedia/sales/{official_store}/{date}/"

            # Detect Scrapping Bucket
            print(f"Detect file in {prefix_scrape_directory}")
            detection_scrape_bucket = detect_file(bucket, prefix_scrape_directory, number_of_bucket)

            if detection_scrape_bucket:
                # Get filename that will be extracted
                detect_filename = list_blob_gcs(bucket, prefix_scrape_directory)[0].split('/')[-1]
                print(f"File {detect_filename} is detected, process clean and upload file will be continue")

                # Processing data to clean, validate, and upload
                processing(bucket, prefix_scrape_directory, validator_json, schema_json, sheet_name, temp_table_path, gcs_logger)
            
            else:
                print(f"Files in {prefix_scrape_directory} is none")

        # Merge or Create into main table
        time.sleep(10)
        merge_query(schema_json, key_column, order_by_column, bq_project, dataset, dataset_append, table_id, gcs_logger)
        
        # Delete temp table
        print("Process to delete temp table")
        delete_table(temp_table_path)

        print("Finish transform data")

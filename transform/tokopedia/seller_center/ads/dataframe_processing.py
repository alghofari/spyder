# --- Import all the packages --- 
import time
import json
import traceback
import pandas as pd
from datetime import datetime

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs

# --- Define client and month dictionary ---
dict_month = {
    "Januari"  : "january",
    "Februari" : "february",
    "Maret"    : "march",
    "April"    : "april",
    "Mei"      : "may",
    "Juni"     : "june",
    "Juli"     : "july",
    "Agustus"  : "august",
    "September": "september",
    "Oktober"  : "october",
    "November" : "november",
    "Desember" : "december"
}

def extract_data_ads_into_dataframe(prefix_name, sheet_name, sheet_indicator, official_store_name, bq_project, dataset, main_table, gcs_logger):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        excel_filename  = metadata['excel_filename']
        upload_tstamp   = metadata['upload_tstamp']
        target_filepath = metadata['target_filepath']
        gcs_logger.log(f"Start to process dataframe {excel_filename}")

        if excel_filename != "":
            try:
                # Read metadata data
                df_metadata_ads = pd.read_excel(
                    target_filepath,
                    sheet_name = sheet_name,
                    nrows      = 4,
                    header     = None,
                    index_col  = False
                )
                gcs_logger.log(f"Start to generate data {sheet_indicator}")
                date_string = df_metadata_ads[1][0]
                date_list   = date_string.split(" - ")

                # Define period date
                start_period = "-".join([dict_month.get(i,i) for i in date_list[0].split(" ")])
                start_period = datetime.strptime(start_period, '%d-%B-%Y').date()

                end_period   = "-".join([dict_month.get(i,i) for i in date_list[-1].split(" ")])
                end_period   = datetime.strptime(end_period, '%d-%B-%Y').date()

                # Read data of Semua Iklan Produk
                df_ads = pd.read_excel(
                    target_filepath, 
                    skiprows   = 4,
                    sheet_name = sheet_name
                )
                if not df_ads.empty:
                    print(f"Generate total of {len(df_ads)} rows into dataframe")
                    df_column_ads  = list(df_ads.columns)
                    df_column_ads  = [column.lower().replace(" ", "_").replace("%", "persentase_").replace("-", "_") for column in df_column_ads]
                    df_ads.columns = df_column_ads

                    # Add column
                    df_ads.insert(loc = 0, column = "start_date", value = start_period)
                    df_ads.insert(loc = 1, column = "end_date", value = end_period)
                    df_ads["official_store"] = official_store_name
                    df_ads['upload_tstamp']  = upload_tstamp

                    time.sleep(5)

                    # Transform data of Semua Iklan Produk
                    schema_dataframe_path = f"./transform/tokopedia/seller_center/ads/resources/schema_{sheet_indicator}.json"
                    schema_dataframe_json = open(schema_dataframe_path)
                    schema_dataframe      = json.load(schema_dataframe_json)

                    for key in schema_dataframe:
                        name = key['name']
                        dtype = key['type']
                        if "TIME" in dtype or "DATE" in dtype:
                            df_ads[name] = df_ads[name].astype("str")
                            df_ads[name] = df_ads[name].apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True))

                            if dtype == "DATE":
                                df_ads[name] = df_ads[name].dt.date
                                df_ads[name] = df_ads[name].replace({pd.NaT: None})
                        
                        elif dtype == "INTEGER":
                            df_ads[name] = df_ads[name].astype("Int64")

                        elif dtype == "FLOAT":
                            df_ads[name] = df_ads[name].astype("float")

                    time.sleep(5)
                    print(f"Dataframe {sheet_indicator} has been created")

                    # Load dataframe into bigquery temp table
                    table_path_ads = f"{bq_project}.{dataset}.{main_table}"
                    df_to_bq(
                        df                = df_ads,
                        project_table_bq  = table_path_ads,
                        file_path         = schema_dataframe_path,
                        job_configuration = "ads"
                    )
                    gcs_logger.log(f"End of the pipeline for transform data {sheet_indicator}")
            except:
                gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                traceback_str = traceback.format_exc()
                gcs_logger.error(traceback_str)
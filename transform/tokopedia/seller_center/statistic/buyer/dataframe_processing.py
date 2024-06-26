# --- Import all the packages --- 
import time
import traceback
import pandas as pd
from datetime import datetime

from helpers.bigquery_helper import df_to_bq, delete_table
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs
from transform.tokopedia.seller_center.load_query_processing import merge_query

# Define base schema path
base_config_path = "./transform/tokopedia/seller_center/statistic/buyer/resources/"

# --- Function for extract data from downloaded file ---
def extract_data_buyer_into_dataframe(prefix_name, official_store, sheet, main_table, table_id, key_column, order_by_column, bq_project, dataset, dataset_append, gcs_logger):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        excel_filename  = metadata['excel_filename']
        upload_tstamp   = metadata['upload_tstamp']
        target_filepath = metadata['target_filepath']
        print(f"The Upload tstamp that will be process is : {upload_tstamp}")

        if excel_filename != "":
            # Get summary data
            if sheet == "Summary":
                try:
                    df_weekly_region_buyer = pd.read_excel(target_filepath, sheet_name=sheet)

                    date_range_data = df_weekly_region_buyer.loc[1][0]

                    start_date = datetime.strptime(date_range_data.split(" - ")[0], "%d/%m/%Y")
                    start_date = datetime.strftime(start_date, "%Y-%m-%d")

                    end_date = datetime.strptime(date_range_data.split(" - ")[-1], "%d/%m/%Y")
                    end_date = datetime.strftime(end_date, "%Y-%m-%d")

                    header_sum_region = df_weekly_region_buyer.loc[4][0:6].values.flatten().tolist()

                    df_weekly_region_buyer = df_weekly_region_buyer.iloc[5:, 0:6]
                    if not df_weekly_region_buyer.empty:
                        df_weekly_region_buyer.columns = header_sum_region

                        # Clean dataframe
                        df_weekly_region_buyer[['Pembeli', 'Pesanan', 'Barang']]     = df_weekly_region_buyer[['Pembeli', 'Pesanan', 'Barang']].astype("Int64")
                        df_weekly_region_buyer[['Nilai Pesanan', '% Nilai Pesanan']] = df_weekly_region_buyer[['Nilai Pesanan', '% Nilai Pesanan']].astype("float")

                        # Add column identifier
                        df_weekly_region_buyer['official_store'] = official_store
                        df_weekly_region_buyer['upload_tstamp']  = upload_tstamp

                        # Add column date for merging condition
                        df_weekly_region_buyer.insert(0, "start_date", start_date)
                        df_weekly_region_buyer.insert(1, "end_date", end_date)

                        # Clean new column type of data
                        df_weekly_region_buyer['start_date'] = df_weekly_region_buyer['start_date'].astype("str").apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%Y-%m-%d")).dt.date.replace({pd.NaT: None})
                        df_weekly_region_buyer['end_date']   = df_weekly_region_buyer['end_date'].astype("str").apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%Y-%m-%d")).dt.date.replace({pd.NaT: None})

                        # Get the schema path
                        schema_path_weekly_region_buyer = base_config_path + f"schema_weekly_region_buyer.json"

                        # Change column name
                        schema_df_weekly_region_buyer   = pd.read_json(schema_path_weekly_region_buyer)
                        column_name_weekly_region_buyer = schema_df_weekly_region_buyer['name'].to_list()
                        df_weekly_region_buyer.columns  = column_name_weekly_region_buyer
                        print("Dataframe weekly_region_buyer has been created")
                        time.sleep(5)

                        # Load dataframe into bigquery temp table
                        temp_table_path_weekly = f"{bq_project}.{dataset_append}.{table_id}"
                        df_to_bq(
                            df                = df_weekly_region_buyer,
                            project_table_bq  = temp_table_path_weekly,
                            file_path         = schema_path_weekly_region_buyer,
                            job_configuration = "statistic"
                        )
                        time.sleep(15)
                        print(f"Dataframe weekly_region_buyer successfully created on temp table")

                        # Merge temp table into main table
                        merge_query(
                            schema_path     = schema_path_weekly_region_buyer,
                            key_column      = key_column,
                            order_by_column = order_by_column,
                            bq_project      = bq_project,
                            dataset         = dataset,
                            dataset_append  = dataset_append,
                            main_table      = main_table,
                            table_temp      = table_id,
                            gcs_logger      = gcs_logger)
                        time.sleep(5)

                        # Delete temp table
                        print("Process to delete temp table")
                        delete_table(temp_table_path_weekly)
                        time.sleep(5)
                        gcs_logger.log(f"End of the pipeline for transform weekly_region_buyer data")
                    
                    else:
                        gcs_logger.log(f"There are no data on weekly_region_buyer_data")
                except:
                    gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                    traceback_str = traceback.format_exc()
                    gcs_logger.error(traceback_str)

            elif sheet == "Harian":
                try:
                    df_daily_buyer = pd.read_excel(target_filepath, sheet, skiprows=1)

                    # Define column Pembeli
                    df_daily_buyer['Pembeli'] = df_daily_buyer['Laki-Laki'] + df_daily_buyer['Perempuan'] + df_daily_buyer['Tidak disebutkan']

                    # Clean dataframe
                    df_daily_buyer['Waktu'] = df_daily_buyer['Waktu'].astype("str").apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%d/%m/%Y")).dt.date.replace({pd.NaT: None})
                    df_daily_buyer.loc[:, df_daily_buyer.columns != 'Waktu'] = df_daily_buyer.loc[:, df_daily_buyer.columns != 'Waktu'].astype("Int64")

                    # Add column identifier
                    df_daily_buyer['official_store'] = official_store
                    df_daily_buyer['upload_tstamp']  = upload_tstamp

                    # Get the schema path
                    schema_path_daily_buyer = base_config_path + f"schema_daily_buyer.json"

                    # Change column name
                    schema_df_daily_buyer   = pd.read_json(schema_path_daily_buyer)
                    column_name_daily_buyer = schema_df_daily_buyer['name'].to_list()
                    df_daily_buyer.columns  = column_name_daily_buyer

                    print("Dataframe daily_buyer has been created")

                    # Load dataframe into bigquery temp table
                    temp_table_path_daily = f"{bq_project}.{dataset_append}.{table_id}"
                    df_to_bq(
                        df                = df_daily_buyer,
                        project_table_bq  = temp_table_path_daily,
                        file_path         = schema_path_daily_buyer,
                        job_configuration = "statistic"
                    )
                    time.sleep(15)
                    print(f"Dataframe daily_buyer successfully created on temp table")

                    # Merge temp table into main table
                    merge_query(
                        schema_path     = schema_path_daily_buyer,
                        key_column      = key_column,
                        order_by_column = order_by_column,
                        bq_project      = bq_project,
                        dataset         = dataset,
                        dataset_append  = dataset_append,
                        main_table      = main_table,
                        table_temp      = table_id,
                        gcs_logger      = gcs_logger)
                    time.sleep(5)

                    # Delete temp table
                    print("Process to delete temp table")
                    delete_table(temp_table_path_daily)
                    time.sleep(5)
                    gcs_logger.log(f"End of the pipeline for transform daily_buyer data")
                
                except:
                    gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                    traceback_str = traceback.format_exc()
                    gcs_logger.error(traceback_str)
            
            else:
                gcs_logger.log(f"Sheet {sheet} is not on the mapping for this table, please check the file")

        else:
            gcs_logger.log("There might be some error, check the pipeline")
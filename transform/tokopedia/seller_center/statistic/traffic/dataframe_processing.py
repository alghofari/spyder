import time
import traceback
import pandas as pd
from datetime import datetime

from helpers.bigquery_helper import df_to_bq, delete_table
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs

from transform.tokopedia.seller_center.load_query_processing import merge_query
from transform.tokopedia.seller_center.transform_dtype import (
    check_add_missing_column, transform_dtype)

# base config path
base_config_path = "./transform/tokopedia/seller_center/statistic/traffic/resources/"

def clean_cols_identifier(dataframe, sheet, start_period_date, end_period_date):
    if sheet == "Produk Dilihat - Rincian Waktu" or sheet == "Konversi - Rincian Waktu":
        # Clean the column type for each col
        dataframe['waktu'] = dataframe['waktu'].astype("str").apply(
            lambda x: pd.to_datetime(x, errors="coerce", 
                                    utc=True, format="%d/%m/%Y")).dt.date.replace({pd.NaT: None})
        
    elif sheet == "Produk Dilihat - Rincian Produk" or sheet == "Konversi - Rincian Produk":
        dataframe.insert(
            0, "start_date", start_period_date)
        dataframe.insert(
            1, "end_date", end_period_date)

        # Clean new column type of data
        dataframe['start_date'] = dataframe['start_date'].astype("str").apply(
            lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%Y-%m-%d")).dt.date.replace({pd.NaT: None})
        dataframe['end_date'] = dataframe['end_date'].astype("str").apply(
            lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%Y-%m-%d")).dt.date.replace({pd.NaT: None})
    
    return dataframe

def dataframe_cleansing(prefix_name, name_of_sheet, official_store_name, schema_filepath, list_ambigious_cols, gcs_logger, additional_column=None):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        excel_filename  = metadata['excel_filename']
        upload_tstamp   = metadata['upload_tstamp']
        target_filepath = metadata['target_filepath']

        gcs_logger.log(f"This is the excel filename {excel_filename}")
        gcs_logger.log(f"The Upload tstamp that will be process is : {upload_tstamp}")

        if excel_filename != "":
            try:
                # Define start date and end date
                df_metadata = pd.read_excel(target_filepath, sheet_name="Ringkasan", header=None)

                # Get the start and date based on data in file
                date_range_data = df_metadata.loc[1][0]

                start_date_period = datetime.strptime(
                    date_range_data.split(" - ")[0], "%d/%m/%Y")
                start_date_period = datetime.strftime(start_date_period, "%Y-%m-%d")

                end_date_period = datetime.strptime(
                    date_range_data.split(" - ")[-1], "%d/%m/%Y")
                end_date_period = datetime.strftime(end_date_period, "%Y-%m-%d")

                # Read metadata data
                df = pd.read_excel(target_filepath, sheet_name=name_of_sheet, header=None)

                # Get the data from data products
                header_df = df.loc[2][:].values.flatten().tolist()
                if additional_column is not None:
                    header_df[0] = additional_column
                header_df = [column.replace(" (SEO)", "").replace(" -", "").replace(" ", "_").lower() for column in header_df]

                df = df.iloc[3:, :]
                df.columns = header_df

                # Clean the column type for identifier columns
                df = clean_cols_identifier(
                    dataframe         = df,
                    sheet             = name_of_sheet,
                    start_period_date = start_date_period,
                    end_period_date   = end_date_period)

                # Add column identifier
                df['official_store'] = official_store_name
                df['upload_tstamp']  = upload_tstamp

                # Get the schema path
                schema_path_df = schema_filepath

                # Get column name from schema
                schema_df = pd.read_json(
                    schema_path_df)

                list_of_cols = schema_df['name'].to_list()

                df = check_add_missing_column(
                    dataframe          = df,
                    columns_to_check   = list_ambigious_cols,
                    list_original_cols = header_df,
                    gcs_logger         = gcs_logger
                )

                # Arrange column based on schema order
                df = df[list_of_cols]

                # Transform dataframe with specific type from schema
                df = transform_dtype(
                    schema_path = schema_path_df,
                    dataframe   = df,
                    gcs_logger  = gcs_logger
                )

                return df
            
            except:
                gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                traceback_str = traceback.format_exc()
                gcs_logger.error(traceback_str)

def extract_data_traffic_into_dataframe(prefix_path_gcs, official_store, sheet_name, main_table_id, temp_table_id, key_column, order_by_column, bq_project, dataset, dataset_append, gcs_logger):
    dataframe_traffic = pd.DataFrame()

    # Define main table identifier
    table_identifier = main_table_id.replace("tokopedia_", "")

    # Define schema path
    schema_path = base_config_path + f"schema_{table_identifier}.json"

    # Define column to check
    columns_to_check = ['kejar_diskon_spesial','kejar_diskon','deal_of_the_day','flash_sale']

    # Create dataframe based on each sheet
    if sheet_name == "Produk Dilihat - Rincian Waktu" or sheet_name == "Konversi - Rincian Waktu":
        dataframe_traffic = dataframe_cleansing(
            prefix_name         = prefix_path_gcs,
            name_of_sheet       = sheet_name,
            official_store_name = official_store,
            schema_filepath     = schema_path,
            list_ambigious_cols = columns_to_check,
            gcs_logger          = gcs_logger,
            additional_column   = "Waktu")
        
    elif sheet_name == "Produk Dilihat - Rincian Produk" or sheet_name == "Konversi - Rincian Produk":
        dataframe_traffic = dataframe_cleansing(
            prefix_name         = prefix_path_gcs,
            name_of_sheet       = sheet_name,
            official_store_name = official_store,
            schema_filepath     = schema_path,
            list_ambigious_cols = columns_to_check,
            gcs_logger          = gcs_logger,
            additional_column   = "Nama Produk")
        
    else:
        gcs_logger.log(
            f"The sheet {sheet_name} is not in the mapping for traffic data, please check the file")
        
    time.sleep(15)

    if not dataframe_traffic.empty:
        # Load dataframe into bigquery temp table
        temp_table_path_data = f"{bq_project}.{dataset_append}.{temp_table_id}"
        print(f"Start to upload data into temp table : {temp_table_path_data}")

        df_to_bq(
            df                = dataframe_traffic,
            project_table_bq  = temp_table_path_data,
            file_path         = schema_path,
            job_configuration = "statistic")
        time.sleep(15)
        print(f"Dataframe {temp_table_id.replace('tokopedia_', '')} successfully created on temp table")

        # Merge temp table into main table
        merge_query(
            schema_path     = schema_path,
            key_column      = key_column,
            order_by_column = order_by_column,
            bq_project      = bq_project,
            dataset         = dataset,
            dataset_append  = dataset_append,
            main_table      = main_table_id,
            table_temp      = temp_table_id,
            gcs_logger      = gcs_logger)
        time.sleep(15)

        # Delete temp table
        print("Process to delete temp table")
        delete_table(temp_table_path_data)
        time.sleep(15)
        gcs_logger.log(f"End of the pipeline for transform data {temp_table_id.replace('tokopedia_', '')}")

    else:
        gcs_logger(f'There is some problem when identify the file, please check the bucket')
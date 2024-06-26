# --- Import all the packages ---
import time
import traceback
import pandas as pd
from datetime import datetime

from helpers.bigquery_helper import df_to_bq, delete_table
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs
from transform.tokopedia.seller_center.load_query_processing import merge_query

# Define base schema path
base_config_path = "./transform/tokopedia/seller_center/statistic/products/resources/"

# --- Function for extract data from downloaded file ---
def extract_data_products_into_dataframe(prefix_name, official_store, sheet, main_table, table_id, key_column, order_by_column, bq_project, dataset, dataset_append, gcs_logger):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        excel_filename  = metadata['excel_filename']
        upload_tstamp   = metadata['upload_tstamp']
        target_filepath = metadata['target_filepath']
        print(f"The Upload tstamp that will be process is : {upload_tstamp}")

        if excel_filename != "" and sheet == "Data":
            try:
                # Get Data Product
                df_data_products = pd.read_excel(target_filepath, sheet_name=sheet, header=None)

                # Get the start and date based on data in file
                date_range_data = df_data_products.loc[0][1]

                start_date = datetime.strptime(
                    date_range_data.split(" - ")[0], "%d/%m/%Y")
                start_date = datetime.strftime(start_date, "%Y-%m-%d")

                end_date = datetime.strptime(
                    date_range_data.split(" - ")[-1], "%d/%m/%Y")
                end_date = datetime.strftime(end_date, "%Y-%m-%d")

                # Get the data from data products
                header_data_products = df_data_products.loc[9][:].values.flatten().tolist()

                df_data_products = df_data_products.iloc[10:, :]
                df_data_products.columns = header_data_products

                # Add column identifier
                df_data_products['official_store'] = official_store
                df_data_products['upload_tstamp'] = upload_tstamp

                df_data_products.insert(0, "start_date", start_date)
                df_data_products.insert(1, "end_date", end_date)

                # Clean new column type of data
                df_data_products['start_date'] = df_data_products['start_date'].astype("str").apply(
                    lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%Y-%m-%d")).dt.date.replace({pd.NaT: None})
                df_data_products['end_date'] = df_data_products['end_date'].astype("str").apply(
                    lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%Y-%m-%d")).dt.date.replace({pd.NaT: None})

                # Get the schema path
                schema_path_data_products = base_config_path + \
                    f"schema_data_products.json"

                # Change column name
                schema_df_data_products = pd.read_json(
                    schema_path_data_products)
                column_name_data_products = schema_df_data_products['name'].to_list(
                )
                df_data_products.columns = column_name_data_products
                print("Dataframe data_products has been created")
                time.sleep(5)

                # Load dataframe into bigquery temp table
                temp_table_path_data = f"{bq_project}.{dataset_append}.{table_id}"
                df_to_bq(df=df_data_products, project_table_bq=temp_table_path_data,
                        file_path=schema_path_data_products, job_configuration="statistic")
                time.sleep(15)
                print(f"Dataframe data_products successfully created on temp table")

                # Merge temp table into main table
                merge_query(
                    schema_path     = schema_path_data_products,
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
                delete_table(temp_table_path_data)
                time.sleep(5)

                gcs_logger.log(f"End of the pipeline for transform product statistic data")
            
            except:
                gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                traceback_str = traceback.format_exc()
                gcs_logger.error(traceback_str)

        else:
            gcs_logger.log(
                f"File {sheet} is not trackable on the mapping, or there might be some error. Check the pipeline"
            )

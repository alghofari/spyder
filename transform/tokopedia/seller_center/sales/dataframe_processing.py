# Import all the package
import json
import re
import time
import traceback
import pandas as pd

from collections import Counter

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs
from helpers.bigquery_helper import delete_table

from transform.tokopedia.seller_center.load_query_processing import merge_query

def get_column(dataframe):
    # Rename the column name
    main_df_column    = list(dataframe.columns)
    main_column_lower = [col.lower() for col in main_df_column]
    main_column_space = [col.replace(" ", "_") for col in main_column_lower]
    main_column_regex = [re.sub("[^_a-zA-Z0-9]", "", col) for col in main_column_space]
    
    return main_column_regex

def get_validator_column(schema_path):
    # Get and count the fix column
    json_col_validator = open(schema_path)
    fix_column         = json.load(json_col_validator)
    fix_column         = fix_column[f'tokopedia_order']
    len_fix_column     = len(fix_column)

    return fix_column, len_fix_column

def clean_column(dataframe, schema_path, folder_name, last_update):
    # Set the column name
    column            = pd.read_json(schema_path)
    column_name       = column['name'].to_list()
    column_name_main  = column_name[0:-2]
    dataframe         = dataframe.drop(['nomor'], axis=1)
    dataframe.columns = column_name_main

    # Add additional column
    dataframe['folder']        = folder_name
    dataframe['upload_tstamp'] = last_update

    # Re-arrage column name based on schema
    dataframe = dataframe[column_name]

    return dataframe

def transform_dtype(dataframe: pd.DataFrame, schema: list) -> pd.DataFrame:
    # Stored the field's and their type of data that contained
    if not dataframe.empty:
        for key in schema:
            name  = key["name"]
            dtype = key["type"]

            # --- For time and date purposes
            if "TIME" in dtype or "DATE" in dtype:
                dataframe[name] = dataframe[name].astype("str")
                dataframe[name] = dataframe[name].apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True))

                if dtype == "DATE":
                    dataframe[name] = dataframe[name].dt.date
                    dataframe[name] = dataframe[name].replace({pd.NaT: None})

                elif dtype == "TIME":
                    dataframe[name] = dataframe[name].dt.time

            elif dtype == "INTEGER":
                dataframe[name] = dataframe[name].astype("Int64") # --- For integer purposes
            elif dtype == "FLOAT":
                dataframe[name] = dataframe[name].astype("float") # --- For float purposes
            elif dtype == "STRING":
                dataframe[name] = dataframe[name].apply(lambda x: str(x) if x is not None and x == x else x) # --- For string purposes
            elif dtype == "BOOLEAN":
                dataframe[name] = dataframe[name].astype("boolean") # --- For boolean purposes

    return dataframe

def transform_data(dataframe, schema_path):
    # Transform type of data into string ---
    fix_schema = open(schema_path)
    fix_schema = json.load(fix_schema)
    final_df   = transform_dtype(dataframe, fix_schema)
    return final_df

def val_df_mp(dataframe, schema_path, folder_gcs, upload_time_gcs):
    
    # --- Set, Add, and Arrage Column Name
    val_df = clean_column(dataframe, schema_path, folder_gcs, upload_time_gcs)

    # --- Give some validation ---
    val_df['buyer_name']    = val_df['buyer_name'].replace(r'(\n)[A-Za-z]*\s[A-Za-z]*', ' ', regex=True)
    val_df['buyer_address'] = val_df['buyer_address'].replace(r'(\n)', ' ', regex=True)

    # --- Transform type of data into string ---
    final_df = transform_data(val_df, schema_path)
    return final_df

def extract_data_into_dataframe(prefix_name, validator_schema_path, fix_schema_path, tokopedia_sheet_name, key_column, order_by_column, bq_project, dataset, dataset_append, table_id, gcs_logger):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        store_foldername = metadata['store_foldername']
        excel_filename   = metadata['excel_filename']
        upload_tstamp    = metadata['upload_tstamp']
        target_filepath  = metadata['target_filepath']
        print(f"The Upload tstamp that will be process is : {upload_tstamp}")

        if excel_filename != "":
            try:
                # Read excel file
                main_df = pd.read_excel(
                    target_filepath, 
                    sheet_name = tokopedia_sheet_name,
                    skiprows   = 4,
                    dtype      = str
                )

                # Column checking
                original_column  = get_column(main_df) # Get column name
                fix_column, len_fix_column  = get_validator_column(validator_schema_path) # Identify fix column based on JSON File
                columns_to_check = ['harga_satuan_bundling_idr',
                                    'gudang_pengiriman',
                                    'nama_bundling',
                                    'jumlah_produk_yang_dikurangkan',
                                    'total_pengurangan_idr'
                                    ]

                # Identify the difference column
                checking_exist         = all(x in original_column for x in columns_to_check)
                len_column_original_df = len(original_column)

                # Change column name
                main_df.columns = original_column

                if len_column_original_df == len_fix_column and Counter(fix_column) == Counter(original_column):
                    # Fix column name
                    main_df = main_df[fix_column]

                    # Finalization dataframe
                    final_df = val_df_mp(main_df, fix_schema_path, store_foldername, upload_tstamp)
                
                elif (len_fix_column - len_column_original_df) > 0 or checking_exist == False:
                    for col in columns_to_check:
                        if col not in main_df.columns:
                            gcs_logger.log(f"This {col}, it's not existed, will be add the column into None Value")
                            main_df[col] = None
                    
                    # Adjust the order of the column
                    main_df = main_df[fix_column]

                    # Finalization dataframe
                    final_df = val_df_mp(main_df, fix_schema_path, store_foldername, upload_tstamp)

                else:
                    raise ValueError(f"Check the newest file. There might be some changes")
            
                if not final_df.empty:
                    print("Start upload to temp table")
                    time.sleep(5)
                    temp_table_path = f"{bq_project}.{dataset_append}.{table_id}"
                    
                    df_to_bq(
                        df                = final_df,
                        project_table_bq  = temp_table_path,
                        file_path         = fix_schema_path,
                        job_configuration = "seller_center")
                    print(f"Finish upload {len(final_df)} rows into temp table")

                    # Merge or Create into main table
                    merge_query(
                        schema_path     = fix_schema_path,
                        key_column      = key_column,
                        order_by_column = order_by_column,
                        bq_project      = bq_project,
                        dataset         = dataset,
                        dataset_append  = dataset_append,
                        main_table      = "tokopedia_order",
                        table_temp      = table_id,
                        gcs_logger      = gcs_logger
                    )
                    time.sleep(10)
                    
                    # Delete temp table
                    print("Process to delete temp table")
                    delete_table(temp_table_path)
                    time.sleep(5)

                    gcs_logger.log("Finish transform sales data")

            except:
                gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                traceback_str = traceback.format_exc()
                gcs_logger.error(traceback_str)
            
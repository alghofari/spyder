import traceback

import pandas as pd

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs
from transform.tokopedia.seller_center.load_query_processing import merge_query
from transform.tokopedia.seller_center.transform_dtype import transform_dtype


def extract_data_video_into_dataframe(prefix_name, official_store, sheet, main_table, table_id, key_column,
                                      order_by_column, bq_project, dataset,
                                      dataset_append, gcs_logger):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name="sirclo-data-marketplace",
        prefix_name=prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        excel_filename = metadata['excel_filename']
        upload_tstamp = metadata['upload_tstamp']
        target_filepath = metadata['target_filepath']
        gcs_logger.log(f"The Upload tstamp that will be process is : {upload_tstamp}")

        if excel_filename != "":
            try:
                # Read dataframe
                data_df = pd.read_excel(target_filepath, sheet_name=sheet, skiprows=10)

                # Casting data type
                data_df['Waktu Dibuat'] = pd.to_datetime(data_df['Waktu Dibuat'], format='%Y/%m/%d', errors='coerce')

                # Add column identifier
                data_df['store_name'] = official_store
                data_df['load_timestamp'] = upload_tstamp

                base_config_path = "./transform/tokopedia/seller_center/statistic/video/resources/"
                schema_path = base_config_path + f"schema_video_statistic.json"

                # Change column name
                schema_df = pd.read_json(schema_path)
                column_name = schema_df['name'].to_list()
                data_df.columns = column_name

                # Transform dataframe with specific type from schema
                data_df = transform_dtype(
                    schema_path=schema_path,
                    dataframe=data_df,
                    gcs_logger=gcs_logger
                )

                gcs_logger.log("Dataframe video statistic has been created")

                # Load dataframe into bigquery temp table
                temp_table = f"{bq_project}.{dataset_append}.{table_id}"
                df_to_bq(
                    df=data_df,
                    project_table_bq=temp_table,
                    file_path=schema_path,
                    job_configuration="statistic"
                )
                gcs_logger.log("Dataframe video statistic successfully created on temp table")

                # Merge temp table into main table
                merge_query(
                    schema_path=schema_path,
                    key_column=key_column,
                    order_by_column=order_by_column,
                    bq_project=bq_project,
                    dataset=dataset,
                    dataset_append=dataset_append,
                    main_table=main_table,
                    table_temp=table_id,
                    gcs_logger=gcs_logger
                )

                gcs_logger.log(f"End of the pipeline for transform video statistic data")
            except Exception as e:
                gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                traceback_str = traceback.format_exc()
                gcs_logger.error(traceback_str)
                gcs_logger.error(e)

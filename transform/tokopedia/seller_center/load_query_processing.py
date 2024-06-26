# import package

from google.cloud import bigquery
from helpers.bigquery_helper import check_table_exist

import pandas as pd

# Construct client
bq_client    = bigquery.Client()

def merge_query(schema_path, key_column, order_by_column, bq_project, dataset, dataset_append, main_table, table_temp, gcs_logger):
    # --- Column name ---
    column       = pd.read_json(schema_path)
    column_names = column['name'].to_list()

    # --- String for fields merge ----
    fields_merge = [f"T.{column} = S.{column}" for column in column_names]
    fields_merge = ", ".join(fields_merge)

    if len(key_column) > 1:
        key_column_string = ', '.join(key_column)
    elif len(key_column) == 1:
        key_column_string = ''.join(key_column)

    if len(order_by_column) > 1:
        order_by_string = ', '.join(order_by_column)
    elif len(order_by_column) == 1:
        order_by_string = ''.join(order_by_column)

    # --- String for inserting values ---
    fix_col = []
    for col in column_names:
        if col == 'start_date':
            col = f"DATE({col}) AS start_date"
        elif col == 'end_date':
            col = f"DATE({col}) AS end_date"
        fix_col.append(col)

    string_col = ", ".join(fix_col)

    primary_keys_merge = [
        f"COALESCE(CAST(T.{key} AS STRING), 'NULL') = COALESCE(CAST(S.{key} AS STRING), 'NULL')" for key in key_column]
    primary_keys_merge = " AND ".join(primary_keys_merge)

    insert_merge = [f"S.{column}" for column in column_names]
    insert_merge = ", ".join(insert_merge)

    insert_columns = ["`" + column + "`" for column in column_names]
    insert_query   = f"INSERT ({','.join(insert_columns)}) VALUES ({insert_merge})"

    # Detect main table
    main_table_id = f"{bq_project}.{dataset}.{main_table}"
    type_ingestion = check_table_exist(main_table_id)

    if type_ingestion:
        query_merge = f'''
                    MERGE `{bq_project}.{dataset}.{main_table}` T
                    USING (SELECT DISTINCT {string_col}
                            FROM (
                                SELECT *, ROW_NUMBER () OVER (PARTITION BY {key_column_string} ORDER BY {order_by_string}) AS row_num
                                FROM `{bq_project}.{dataset_append}.{table_temp}`
                            )
                            WHERE row_num = 1
                        ) S
                    ON {primary_keys_merge}
                    WHEN MATCHED THEN
                        UPDATE SET {fields_merge}
                    WHEN NOT MATCHED THEN
                        {insert_query}
                '''
    else:
        query_merge = f"""CREATE TABLE `{bq_project}.{dataset}.{main_table}` PARTITION BY DATE_TRUNC({order_by_string}, day) AS (
            SELECT DISTINCT {string_col}
                            FROM (
                                SELECT *, ROW_NUMBER () OVER (PARTITION BY {key_column_string} ORDER BY {order_by_string}) AS row_num
                                FROM `{bq_project}.{dataset_append}.{table_temp}`
                            )
                            WHERE row_num = 1
        )"""

    bq_client.query(query_merge).result()
    gcs_logger.log(f"merging is done for table {main_table}")

def append_query(schema_path, key_column, order_by_column, bq_project, dataset, dataset_append, main_table, table_temp, gcs_logger):
    # --- Column name ---
    column = pd.read_json(schema_path)
    column_names = column['name'].to_list()

    # --- String for fields merge ----
    fields_merge = [f"T.{column} = S.{column}" for column in column_names]
    fields_merge = ", ".join(fields_merge)

    if len(key_column) > 1:
        key_column_string = ', '.join(key_column)
    elif len(key_column) == 1:
        key_column_string = ''.join(key_column)

    if len(order_by_column) > 1:
        order_by_string = ', '.join(order_by_column)
    elif len(order_by_column) == 1:
        order_by_string = ''.join(order_by_column)

    # --- String for inserting values ---
    fix_col = []
    for col in column_names:
        fix_col.append(col)

    string_col = ", ".join(fix_col)

    # Detect main table
    main_table_id = f"{bq_project}.{dataset}.{main_table}"
    type_ingestion = check_table_exist(main_table_id)

    if type_ingestion:
        query_merge = f'''
                    INSERT `{bq_project}.{dataset}.{main_table}` ({string_col}) 
                    SELECT {string_col}
                    FROM 
                    `{bq_project}.{dataset_append}.{table_temp}`   
                '''
    else:
        query_merge = f"""CREATE TABLE `{bq_project}.{dataset}.{main_table}` PARTITION BY DATE_TRUNC({order_by_string}, day) AS (
            SELECT DISTINCT {string_col}
                            FROM (
                                SELECT *, ROW_NUMBER () OVER (PARTITION BY {key_column_string} ORDER BY {order_by_string}) AS row_num
                                FROM `{bq_project}.{dataset_append}.{table_temp}`
                            )
                            WHERE row_num = 1
        )"""

    bq_client.query(query_merge).result()
    gcs_logger.log(f"Append is done for table {main_table}")
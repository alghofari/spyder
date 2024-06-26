import json
import os
import re
from datetime import datetime
from glob import glob

import pandas as pd
from google.cloud import bigquery

from helpers.bigquery_helper import delete_table, check_table_exist
from transform.lazada.seller_center.orders import merge


def read_data(report_file, store_name, schema):
    dataframe = pd.read_excel(report_file, sheet_name=0)
    dataframe.columns = dataframe.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.lower()
    dataframe["store_name"] = store_name
    dataframe["upload_tstamp"] = datetime.now()

    for column in schema:
        if column['type'] == 'STRING':
            dataframe[column['name']] = dataframe[column['name']].astype('str')
            dataframe[column['name']] = dataframe[column['name']].replace('nan', None)
        elif column['type'] == 'TIMESTAMP':
            dataframe[column['name']] = pd.to_datetime(dataframe[column['name']], format='%d %b %Y %H:%M')
        elif column['type'] == 'BOOLEAN':
            dataframe[column['name']] = dataframe[column['name']].astype("bool")
        elif column['type'] == 'INTEGER':
            dataframe[column['name']] = dataframe[column['name']].astype("int")
        elif column['type'] == 'FLOAT':
            dataframe[column['name']] = dataframe[column['name']].astype("float")

    return dataframe


def write_to_bigquery(client, table_schema, dataframe, table_id, load_method):
    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        write_disposition=load_method,
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def execute_query(client, query):
    client.query(query).result()  # Waits for job to complete.


def main(download_path, store_name):
    client = bigquery.Client()

    project = "sirclo-prod"
    dataset = "bronze_seller_center_commerce"
    dataset_temp = "temp_append"

    store_name_lower = re.sub(r"[^A-Za-z]", "_", store_name.lower())
    temp_table_name = f"lazada_order_{store_name_lower}_temp"
    temp_table_id = f"{project}.{dataset_temp}.{temp_table_name}"
    main_table_name = f"lazada_order"

    base_path = os.getcwd() + "/transform/lazada/seller_center/orders"
    schema_path = f"{base_path}/schema.json"

    key_column = ["order_item_id", "store_name"]
    order_by_column = ["upload_tstamp"]

    with open(schema_path, "r") as f:
        schema = json.loads(f.read())

    delete_table(temp_table_id)

    try:
        report_files = glob(download_path + "/*.xlsx")
        for report_filename in report_files:
            df = read_data(report_filename, store_name, schema)
            write_to_bigquery(client, schema, df, temp_table_id, "WRITE_APPEND")
    except Exception as e:
        print(e)

    if check_table_exist(temp_table_id):
        merge.generate_query(schema_path,
                             key_column,
                             order_by_column,
                             project,
                             dataset,
                             dataset_temp,
                             temp_table_name,
                             main_table_name)

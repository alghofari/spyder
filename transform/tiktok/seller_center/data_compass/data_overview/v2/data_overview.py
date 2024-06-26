import json
import os
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from google.cloud import bigquery

from helpers.bigquery_helper import delete_table, check_table_exist
from helpers.dataframe_helper import datatype_casting


def read_data(report_file, store_name, table_schema):
    # read csv to dataframe
    dataframe = pd.read_excel(report_file, skiprows=4)

    # replace nan value to none
    dataframe = dataframe.replace({np.nan: None})
    dataframe = dataframe.replace({"--": None})

    # rename columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())

    # rename english column
    columns = {
        "Date": "date",
        "GMV (Rp)": "gmv",
        "Refunds (Rp)": "refunds",
        "Items sold": "items_sold",
        "Buyers": "buyers",
        "Page views": "page_views",
        "Visitors": "visitors",
        "Conversion rate": "conversion_rate",
        "SKU orders": "sku_orders",
        "Orders": "orders"
    }
    dataframe = dataframe.rename(columns=columns)

    # rename bahasa indonesia column
    columns = {
        "Tanggal": "date",
        "GMV (Rp)": "gmv",
        "Pengembalian dana (Rp)": "refunds",
        "Produk terjual": "items_sold",
        "Pembeli": "buyers",
        "Tayangan halaman": "page_views",
        "Pengunjung": "visitors",
        "Persentase konversi": "conversion_rate",
        "Pesanan SKU": "sku_orders",
        "Pesanan": "orders"
    }
    dataframe = dataframe.rename(columns=columns)

    # replace percentage symbol to empty string
    dataframe['conversion_rate'] = dataframe['conversion_rate'].replace("[%]", "", regex=True)

    # add new column
    dataframe['store_name'] = store_name
    dataframe['load_timestamp'] = datetime.now()

    # casting data type
    datatype_casting(table_schema, dataframe)
    dataframe = dataframe.replace({"": None, np.NAN: None, np.NaN: None, "None": None, "nan": None})

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
    temp_table_name = f"{project}.{dataset}.tiktok_data_overview_{store_name}_temp"
    base_path = os.getcwd() + "/transform/tiktok/seller_center/data_compass/data_overview/v2"

    delete_table(temp_table_name)

    try:
        report_files = glob(download_path + "/Overview_My Business Performance*.xlsx")
        for report_filename in report_files:
            with open(f"{base_path}/schema.json", "r") as f:
                schema = json.loads(f.read())
            df = read_data(report_filename, store_name, schema)
            write_to_bigquery(client, schema, df, temp_table_name, "WRITE_APPEND")
    except Exception as e:
        print(e)

    if check_table_exist(temp_table_name):
        with open(f"{base_path}/query.sql", "r") as f:
            query_file = f.read().format(temp_table_name=temp_table_name)

        execute_query(client, query_file)
        delete_table(temp_table_name)

import json
import os
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from google.cloud import bigquery

from helpers.bigquery_helper import delete_table, check_table_exist
from helpers.dataframe_helper import datatype_casting


def read_data(report_file, table_schema, store_name):
    # read csv to dataframe
    dataframe = pd.read_excel(report_file, skiprows=[0, 1])

    # replace nan value to none
    dataframe = dataframe.replace({np.nan: None})
    dataframe = dataframe.replace({"--": None})

    # rename columns
    columns = {
        "Nama Kreator": "Creator name",
        "Creator ID": "Creator ID",
        "Informasi Video": "Video Info",
        "Video ID": "Video ID",
        "Waktu": "Time",
        "Produk": "Products",
        "Penghasilan Video (Rp)": "Video Revenue (Rp)",
        "Penjualan Video": "Unit Sales",
        "Pesanan Video": "Orders",
        "Pembeli": "Buyers",
        "Pengembalian Uang video (Rp)": "Refunds (Rp)",
        "Refund produk": "Product refunds",
        "CO Rate": "CO rate",
        "VV": "VV",
        "Likes": "Likes",
        "Komentar": "Comments",
        "Dibagikan": "Shares",
        "Produk Dilihat": "Product Impressions",
        "Klik Produk": "Product Clicks",
        "Pengikut baru": "New followers",
        "CTR": "CTR",
        "Klik Video ke LIVE": "V-to-L clicks",
        "GPM (Rp)": "GPM (Rp)",
        "Rasio Video ke LIVE": "V-to-L rate",
        "Persentase Video yang Ditonton Hingga Selesai": "Video Finish Rate",
        "CTOR": "CO rate"
    }
    dataframe = dataframe.rename(columns=columns)

    # rename columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())
    dataframe = dataframe.rename(columns=lambda x: x.replace(' ', '_').lower())
    dataframe = dataframe.rename(columns=lambda x: x.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\\|`~-=+\n"}))
    dataframe = dataframe.rename(columns=lambda x: x.replace("__", "_"))
    columns = {
        "video_revenue_rp": "video_revenue",
        "refunds_rp": "refunds",
        "gpm_rp": "gpm"
    }
    dataframe = dataframe.rename(columns=columns)

    # casting data type
    dataframe['time'] = pd.to_datetime(dataframe['time'], format='%Y/%m/%d %H:%M:%S', errors='coerce')

    # add new column
    dataframe['store_name'] = store_name
    dataframe['load_timestamp'] = datetime.now()

    datatype_casting(table_schema, dataframe)

    dataframe = dataframe.replace({"None": None})

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
    temp_table_name = f"{project}.{dataset}.tiktok_video_analysis_{store_name}_temp"
    base_path = os.getcwd() + "/transform/tiktok/seller_center/data_compass/video_analysis"

    delete_table(temp_table_name)

    with open(f"{base_path}/schema.json", "r") as f:
        schema = json.loads(f.read())

    try:
        report_files = glob(download_path + "/Video*.xlsx")
        for report_filename in report_files:
            df = read_data(report_filename, schema, store_name)
            write_to_bigquery(client, schema, df, temp_table_name, "WRITE_APPEND")
    except Exception as e:
        print(e)

    if check_table_exist(temp_table_name):
        with open(f"{base_path}/query.sql", "r") as f:
            query_file = f.read().format(temp_table_name=temp_table_name)

        execute_query(client, query_file)
        delete_table(temp_table_name)

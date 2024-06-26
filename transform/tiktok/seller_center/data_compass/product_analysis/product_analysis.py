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
    dataframe = pd.read_excel(report_file, skiprows=[0,1])

    # replace nan value to none
    dataframe = dataframe.replace({np.nan: None})
    dataframe = dataframe.replace({"--": None})

    # rename bahasa indonesia columns to english
    columns = {
        "ID": "ID",
        "Nama": "Product Name",
        "Penghasilan (Rp)": "Revenue (Rp)",
        "Pembeli": "Buyers",
        "Penjualan Produk": "Product sales",
        "Pengembalian Uang (Rp)": "Refunds (Rp)",
        "Produk Dikembalikan": "Product refunds",
        "Pesanan": "Orders",
        "Komisi (Rp)": "Commission (Rp)",
        "Penghasilan live streaming (Rp)": "Livestream revenue (Rp)",
        "Pesanan live streaming": "Livestream orders",
        "Pembeli live streaming": "Livestream buyers",
        "Penjualan produk live streaming": "Livestream product sales",
        "Penonton produk live streaming": "Livestream product viewers",
        "Pengguna yang mengeklik produk live streaming": "Users clicking livestream products",
        "Refund live streaming (Rp)": "Livestream refunds (Rp)",
        "Refund Produk live streaming": "Livestream refunded units",
        "Impresi produk live streaming": "Livestream product impressions",
        "Klik produk live streaming": "Livestream product clicks",
        "CTR produk live streaming": "Livestream product CTR",
        "Tingkat CO produk live streaming": "Livestream product CO rate",
        "Penghasilan Video (Rp)": "Video revenue (Rp)",
        "Pesanan Video": "Video orders",
        "Pembeli Video": "Video buyers",
        "Penjualan Produk Video": "Video product sales",
        "Penonton Video Produk": "Video product viewers",
        "Pengguna (Klik Video Produk)": "Users clicking video products",
        "Refund Video (Rp)": "Video refunds (Rp)",
        "Refund Video Produk": "Video refunded units",
        "Impresi Video Produk": "Video product impressions",
        "Klik video produk": "Video product clicks",
        "CTR video produk": "Video product CTR",
        "CO Rate video produk": "Video product CO rate"
    }
    dataframe = dataframe.rename(columns=columns)

    # rename columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())
    dataframe = dataframe.rename(columns=lambda x: x.replace(' ', '_').lower())
    dataframe = dataframe.rename(columns=lambda x: x.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=+\n"}))
    dataframe = dataframe.rename(columns=lambda x: x.replace("__", "_"))
    columns = {
        "revenue_rp": "revenue",
        "refunds_rp": "refunds",
        "commission_rp": "commission",
        "livestream_revenue_rp": "livestream_revenue",
        "livestream_refunds_rp": "livestream_refunds",
        "video_revenue_rp": "video_revenue",
        "video_refunds_rp": "video_refunds"
    }
    dataframe = dataframe.rename(columns=columns)

    # extract date range
    date_range = pd.read_excel(report_file, usecols="A", nrows=1, header=None, names=["Value"]).iloc[0]["Value"]
    date_range_list = date_range.replace("[Date Range]: ", "").replace(" ", "").split("~")
    start_date = date_range_list[0]
    end_date = date_range_list[1]

    # add new column
    dataframe['start_date'] = start_date
    dataframe['end_date'] = end_date
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
    temp_table_name = f"{project}.{dataset}.tiktok_product_analysis_{store_name}_temp"
    base_path = os.getcwd() + "/transform/tiktok/seller_center/data_compass/product_analysis"

    delete_table(temp_table_name)

    with open(f"{base_path}/schema.json", "r") as f:
        schema = json.loads(f.read())

    try:
        report_files = glob(download_path + "/Product*.xlsx")
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
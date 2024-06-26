import json
import os
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from google.cloud import bigquery

from helpers.bigquery_helper import delete_table, check_table_exist


def read_data(report_file, store_name, schema):
    # read csv to dataframe
    dataframe = pd.read_excel(report_file, skiprows=[0, 1])

    # replace nan value to none
    dataframe = dataframe.replace({np.nan: None})

    # rename english columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())
    columns = {
        "Creator ID": "creator_id",
        "Creator": "creator",
        "Nickname": "nickname",
        "Launched Time": "launched_time",
        "Duration": "duration",
        "Revenue (Rp)": "revenue",
        "Products": "products",
        "Different Products Sold": "different_products_sold",
        "Orders Created": "orders_created",
        "Orders Paid": "orders_paid",
        "Unit Sales": "unit_sales",
        "Buyers": "buyers",
        "Average Price (Rp)": "average_price",
        "CO rate": "co_rate",
        "Viewers": "viewers",
        "Views": "views",
        "ACU": "acu",
        "PCU": "pcu",
        "Avg. Viewing Duration": "avg_viewing_duration",
        "Comments": "comments",
        "Shares": "shares",
        "Likes": "likes",
        "New Followers": "new_followers",
        "Product Impressions": "product_impressions",
        "Product Clicks": "product_clicks",
        "CTR": "ctr"
    }
    dataframe = dataframe.rename(columns=columns)

    # rename bahasa indonesia columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())
    columns = {
        "Creator ID": "creator_id",
        "Kreator": "creator",
        "Nama panggilan": "nickname",
        "Waktu Live": "launched_time",
        "Durasi": "duration",
        "Penghasilan (Rp)": "revenue",
        "Produk": "products",
        "Produk Terjual": "different_products_sold",
        "Pesanan Dibuat": "orders_created",
        "Pesanan Dibayar": "orders_paid",
        "Penjualan": "unit_sales",
        "Pembeli": "buyers",
        "Harga Rata-Rata (Rp)": "average_price",
        "CO Rate": "co_rate",
        "Penonton": "viewers",
        "Live Stream Dilihat": "views",
        "Rata-rata Penonton": "acu",
        "Penonton Tertinggi": "pcu",
        "Durasi Rata-rata": "avg_viewing_duration",
        "Komentar": "comments",
        "Live Dibagikan": "shares",
        "Likes": "likes",
        "Pengikut Baru": "new_followers",
        "Produk Dilihat": "product_impressions",
        "Klik Produk": "product_clicks",
        "CTR": "ctr"
    }
    dataframe = dataframe.rename(columns=columns)

    # replace percentage symbol to empty string
    dataframe['co_rate'] = dataframe['co_rate'].replace("[%]", "", regex=True)
    dataframe['ctr'] = dataframe['ctr'].replace("[%]", "", regex=True)

    # casting data type
    dataframe['creator_id'] = dataframe['creator_id'].astype(str)
    dataframe['revenue'] = dataframe['revenue'].astype(float)
    dataframe['average_price'] = dataframe['average_price'].astype(float)
    dataframe['co_rate'] = dataframe['co_rate'].astype(float)
    dataframe['ctr'] = dataframe['ctr'].astype(float)
    dataframe['launched_time'] = pd.to_datetime(dataframe['launched_time'], format='%Y/%m/%d/ %H:%M', errors='coerce')

    # add new column
    dataframe['acu'] = None
    dataframe['pcu'] = None
    dataframe['store_name'] = store_name
    dataframe['load_timestamp'] = datetime.now()

    # filter DataFrame columns based on schema
    selected_columns = [field['name'] for field in schema]
    dataframe = dataframe[selected_columns]

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
    temp_table_name = f"{project}.{dataset}.tiktok_live_analysis_{store_name}_temp"
    base_path = os.getcwd() + "/transform/tiktok/seller_center/data_compass/live_analysis"

    delete_table(temp_table_name)

    with open(f"{base_path}/schema.json", "r") as f:
        schema = json.loads(f.read())

    try:
        report_files = glob(download_path + "/Live*.xlsx")
        for report_filename in report_files:
            df = read_data(report_filename, store_name, schema)
            write_to_bigquery(client, schema, df, temp_table_name, "WRITE_APPEND")
    except Exception as e:
        print(e)

    if check_table_exist(temp_table_name):
        with open(f"{base_path}/query.sql", "r") as f:
            query_file = f.read().format(temp_table_name=temp_table_name)

        execute_query(client, query_file)
        delete_table(temp_table_name)

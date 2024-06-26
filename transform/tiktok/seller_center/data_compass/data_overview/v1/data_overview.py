import json
import os
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from google.cloud import bigquery

from helpers.bigquery_helper import delete_table, check_table_exist


def read_data(report_file, store_name):
    # read csv to dataframe
    dataframe = pd.read_excel(report_file, skiprows=[0,1])

    # replace nan value to none
    dataframe = dataframe.replace({np.nan: None})
    dataframe = dataframe.replace({"--": None})

    # rename columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())

    # rename english column
    columns = {
        "Time": "time",
        "Revenue (Rp)": "revenue",
        "Shopping Center Revenue": "shopping_center_revenue",
        "Product Views": "product_views",
        "Product Reach": "product_reach",
        "Buyers": "buyers",
        "Unit Sales": "unit_sales",
        "Orders": "orders",
        "Refunds (Rp)": "refunds",
        "Conversion Rate": "conversion_rate",
        "Visitors": "visitors",
        "Negative Review Rate": "negative_review_rate",
        "Rate of Returns for Quality Reasons": "rate_of_returns_for_quality_reasons",
        "Complaint Rate": "complaint_rate",
        "Affiliate revenue (Rp)": "affiliate_revenue",
        "Owned media revenue (Rp)": "owned_media_revenue",
    }
    dataframe = dataframe.rename(columns=columns)

    # rename bahasa indonesia column
    columns = {
        "Waktu": "time",
        "Penghasilan (Rp)": "revenue",
        "Penghasilan dari Pusat Belanja": "shopping_center_revenue",
        "Penghasilan dari Shop Tab": "shopping_center_revenue",
        "Penayangan Produk": "product_views",
        "Jangkauan Produk": "product_reach",
        "Pembeli": "buyers",
        "Penjualan": "unit_sales",
        "Pesanan": "orders",
        "Pengembalian Uang (Rp)": "refunds",
        "Tingkat Konversi": "conversion_rate",
        "Pengunjung": "visitors",
        "Tingkat Ulasan Negatif": "negative_review_rate",
        "Tingkat Pengembalian karena Alasan Kualitas": "rate_of_returns_for_quality_reasons",
        "Tingkat Pengaduan": "complaint_rate",
        "Afiliasi (Rp)": "affiliate_revenue",
        "Toko (Rp)": "owned_media_revenue",
    }
    dataframe = dataframe.rename(columns=columns)

    # replace percentage symbol to empty string
    dataframe['negative_review_rate'] = dataframe['negative_review_rate'].replace("[%]", "", regex=True)
    dataframe['rate_of_returns_for_quality_reasons'] = dataframe['rate_of_returns_for_quality_reasons'].replace("[%]", "", regex=True)
    dataframe['complaint_rate'] = dataframe['complaint_rate'].replace("[%]", "", regex=True)

    # casting data type
    dataframe['time'] = pd.to_datetime(dataframe['time'], format='%m-%d-%Y', errors='coerce').dt.date
    dataframe['revenue'] = dataframe['revenue'].astype(float)
    dataframe['shopping_center_revenue'] = dataframe['shopping_center_revenue'].astype(float)
    dataframe['refunds'] = dataframe['refunds'].astype(float)
    dataframe['negative_review_rate'] = dataframe['negative_review_rate'].astype(float)
    dataframe['rate_of_returns_for_quality_reasons'] = dataframe['rate_of_returns_for_quality_reasons'].astype(float)
    dataframe['complaint_rate'] = dataframe['complaint_rate'].astype(float)
    dataframe['affiliate_revenue'] = dataframe['affiliate_revenue'].astype(float)
    dataframe['owned_media_revenue'] = dataframe['owned_media_revenue'].astype(float)

    # add new column
    dataframe['store_name'] = store_name
    dataframe['load_timestamp'] = datetime.now()

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
    base_path = os.getcwd() + "/transform/tiktok/seller_center/data_compass/data_overview/v1"

    delete_table(temp_table_name)

    try:
        report_files = glob(download_path + "/Core*.xlsx")
        for report_filename in report_files:
            df = read_data(report_filename, store_name)
            with open(f"{base_path}/schema.json", "r") as f:
                schema = json.loads(f.read())
            write_to_bigquery(client, schema, df, temp_table_name, "WRITE_APPEND")
    except Exception as e:
        print(e)

    if check_table_exist(temp_table_name):
        with open(f"{base_path}/query.sql", "r") as f:
            query_file = f.read().format(temp_table_name=temp_table_name)

        execute_query(client, query_file)
        delete_table(temp_table_name)
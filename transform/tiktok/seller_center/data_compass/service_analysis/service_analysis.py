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
        "Agent ID": "Agent ID",
        "Nama panggilan": "Nickname",
        "Tanggal": "Date",
        "Alokasi obrolan": "Allocated chats",
        "Obrolan yang ditanggapi agen CS": "Human response required chats",
        "Obrolan yang ditanggapi agen CS dalam 24 jam": "24h human agent responded chats",
        "Tingkat respons agen CS dalam waktu 24 jam": "24h human agent response rate",
        "Pesan yang terlambat ditanggapi": "Late-responded chats",
        "Pesan yang tidak ditanggapi": "Non-Responded Chats",
        "Tingkat kepuasan": "Satisfaction rate",
        "Rata-rata waktu respons pertama": "Avg. first response time"
    }
    dataframe = dataframe.rename(columns=columns)

    # rename columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())
    dataframe = dataframe.rename(columns=lambda x: x.replace(' ', '_').lower())
    dataframe = dataframe.rename(columns=lambda x: x.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=+\n"}))
    dataframe = dataframe.rename(columns=lambda x: x.replace("__", "_"))

    # casting data type
    dataframe['date'] = pd.to_datetime(dataframe['date'], format='%Y-%m-%d', errors='coerce').dt.date

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
    temp_table_name = f"{project}.{dataset}.tiktok_service_analysis_{store_name}_temp"
    base_path = os.getcwd() + "/transform/tiktok/seller_center/data_compass/service_analysis"

    delete_table(temp_table_name)

    with open(f"{base_path}/schema.json", "r") as f:
        schema = json.loads(f.read())

    try:
        report_files = glob(download_path + "/Service*.xlsx")
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
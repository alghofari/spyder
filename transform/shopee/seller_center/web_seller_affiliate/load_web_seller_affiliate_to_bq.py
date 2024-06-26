import json
import os
from datetime import datetime
from glob import glob

import gspread
import pandas as pd
from google.cloud import bigquery

from helpers.dataframe_helper import datatype_casting


def read_config(os_key):
    gc = gspread.service_account(filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    sh = gc.open_by_key('1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg')
    worksheet = sh.worksheet("shopee")
    records = worksheet.get_all_records()
    config = [i for i in records if i['os_key'] == os_key][0]
    return config


def read_data(report_file, store_name, table_schema):
    df = pd.read_csv(report_file)
    df = df.rename(columns=lambda x: x.strip())
    df = df.rename(columns=lambda x: x.replace(' ', '_').lower())
    df = df.rename(columns=lambda x: x.translate({ord(c): "" for c in "!@#$%^&*()[]{};:,./<>?\\|`~-=+\n"}))
    df = df.rename(columns=lambda x: x.replace("__", "_"))
    columns = {
        "hargarp": "harga_rp",
        "nilai_pembelianrp": "nilai_pembelian_rp",
        "jumlah_pengembalianrp": "jumlah_pengembalian_rp",
        "partner_promo": "partner_kampanye"
    }
    df = df.rename(columns=columns)
    df['store_name'] = store_name
    df['load_timestamp'] = datetime.now()
    df = datatype_casting(table_schema, df)
    df = df.replace({"": None, "None": None, "nan": None})

    # Filter DataFrame columns based on schema
    columns = [field['name'] for field in table_schema]
    df = df[columns]
    return df


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


def main(os_key):
    client = bigquery.Client()
    project = "sirclo-prod"
    dataset = "bronze_seller_center_commerce"
    table_name = f"{project}.{dataset}.shopee_web_seller_affiliate_conversion_report"

    config = read_config(os_key)
    store_name = config['official_store_name']

    schema = open(os.getcwd() + '/transform/shopee/seller_center/web_seller_affiliate/schema.json')
    schema = json.load(schema)

    download_path = "/app/download_folder"
    report_files = glob(download_path + "/SellerConversionReport*.csv")
    for report_filename in report_files:
        df = read_data(report_file=report_filename, store_name=store_name, table_schema=schema)
        write_to_bigquery(client, schema, df, table_name, "WRITE_APPEND")

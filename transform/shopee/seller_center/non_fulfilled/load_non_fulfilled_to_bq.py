import os
import json
import gspread
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from datetime import date
from datetime import datetime
from datetime import timedelta

from helpers.creds_helper import get_config


def load_from_gcs(fs, official_store_name, start_date, end_date):
    blob = fs.blob(f'assets/excel/shopee/non_fulfilled/{official_store_name}/{date.today()}/Shopee_Non_Fulfilled_{end_date}_{start_date}.xlsx')
    data_bytes = blob.download_as_bytes()

    df = pd.read_excel(data_bytes)
    return df

def transform(df, official_store_name, schema):

    df.columns = [i.lower().replace(' ','_').replace('(','').replace(')', '').replace('.','').replace(',','') for i in df.columns]
    df['folder'] = official_store_name
    df['upload_timestamp'] = datetime.now()

    df = df.replace('nan', None)
    print(df)
    for i in schema:
        if i['type'] == 'FLOAT':
            df[i['name']] = df[i['name']].astype(float)
        elif i['type'] == 'INTEGER':
            df[i['name']] = df[i['name']].apply(lambda x: x.replace("Rp","").replace(".",""))
            df[i['name']] = df[i['name']].astype(int)
        elif i['type'] == 'STRING':
            df[i['name']] = df[i['name']].astype(str)
        elif i['type'] == 'TIMESTAMP':
            df[i['name']] = df[i['name']].replace(" GMT+7", "")
            df[i['name']] = pd.to_datetime(df[i['name']], format="%d/%m/%Y GMT+7")

    return df

def load_to_bq(bq_client, df, schema):

    job = bq_client.load_table_from_dataframe(
        df, 'sirclo-prod.temp_append.bronze_seller_center_commerce_shopee_non_fulfilled_changes', job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        autodetect=True,
                        schema = schema
                    )
    )
    print(job.result())

def merge(columns, bq_client):
    update_set = ', '.join(f"T.{col} = S.{col} " for col in columns)
    insert_set = ', '.join(col for col in columns)

    job = bq_client.query(f"""
        MERGE `sirclo-prod.bronze_seller_center_commerce.shopee_non_fulfilled` T
            USING `sirclo-prod.temp_append.bronze_seller_center_commerce_shopee_non_fulfilled_changes` S
            ON T.no_pesanan = S.no_pesanan AND T.folder  = S.folder
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_set}) VALUES({insert_set})
    """)

    print(job.result())

def main(os_key):
    config = get_config(os_key, "shopee")
    official_store_name = config['official_store_name']
    
    schema = open(os.getcwd() + '/transform/shopee/seller_center/non_fulfilled/schema.json')
    schema = json.load(schema)

    storage_client = storage.Client()
    storage_client = storage_client.bucket('sirclo-data-marketplace')
    bq_client = bigquery.Client()

    start_date = date.today()
    end_date   = start_date - timedelta(days=30)

    rows = load_from_gcs(storage_client, official_store_name, start_date, end_date)
    df   = transform(rows, official_store_name, schema)

    load_to_bq(bq_client, df, schema)
    merge(df.columns, bq_client)

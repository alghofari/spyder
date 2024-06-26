import os
import json
import gcsfs
import gspread

import pandas as pd
from datetime import date
from datetime import datetime
from google.cloud import bigquery

from helpers.creds_helper import get_config

def extract(fs, official_store_name, start_date, end_date):

    with fs.open(f'gs://sirclo-data-marketplace/assets/excel/shopee/raw_fulfillment/{official_store_name}/{date.today()}/Shopee_Raw_Fulfillment_{start_date}_{end_date}.xlsx') as f:
        df = pd.read_excel(f, thousands=".", decimal=",")
    return df


def transform(df, official_store_name, schema):

    df.columns = [i.lower().replace(' ','_').replace('.','').replace('(', '').replace(')', '').replace('/','').replace('-','_') for i in df.columns]
    df.reset_index(drop=True, inplace=True)
    
    for i in schema:
        if i['name'] in df.columns:
            if i['type'] == 'FLOAT':
                df[i['name']] = df[i['name']].apply(lambda x:str(x).lower().replace('.','').replace('rp','') if x != 0 else x)
                df[i['name']] = pd.to_numeric(df[i['name']], errors="coerce").fillna(0).astype(float)
            elif i['type'] == 'INTEGER':
                df[i['name']] = pd.to_numeric(df[i['name']], errors="coerce").fillna(0).astype(int)
            elif i['type'] == 'STRING':
                df[i['name']] = df[i['name']].astype(str)
            elif i['type'] == 'TIMESTAMP' or i['type'] == 'DATE':
                df[i['name']] = pd.to_datetime(df[i['name']], errors='coerce')
        else:
            df[i['name']] = None
    df['folder'] = official_store_name
    df['upload_timestamp'] = datetime.now()

    df = df.replace('nan', None)

    return df


def load_to_bq(df, bq_client, schema):

    job = bq_client.load_table_from_dataframe(
        df, 'sirclo-prod.bronze_seller_center_commerce.shopee_raw_fulfillment', job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        schema = schema,
                        autodetect=False
                    )
    )
    print(job.result())


def main(start_date, end_date, os_key):
    config      = get_config(os_key, "shopee")
    official_store_name = config['official_store_name']

    fs        = gcsfs.GCSFileSystem(project="sirclo-data-prod")
    bq_client = bigquery.Client()

    schema = open(os.getcwd() + '/transform/shopee/seller_center/raw_fulfillment/schema.json')
    schema = json.load(schema)
    
    df = extract(fs, official_store_name, start_date, end_date)
    df = transform(df, official_store_name, schema)
    load_to_bq(df, bq_client, schema)

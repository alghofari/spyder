import os
import json
import gcsfs

import pandas as pd
from datetime import date
from datetime import datetime
from datetime import timedelta
from google.cloud import bigquery

from helpers.creds_helper import get_config


def extract(fs, official_store_name, start_date, end_date):

    with fs.open(f'gs://sirclo-data-marketplace/assets/excel/shopee/live_stream_detail/{official_store_name}/{date.today()}/Shopee_Live_Stream_Detail_{start_date}_{end_date}.xlsx') as f:
        df = pd.read_excel(f, thousands=".", decimal=",")
    return df

def transform(df, official_store_name, schema):

    df.columns = [i.lower().replace(' ', '_').replace('-', '_').replace(
        '(', '').replace(')', '').replace('+_', '').replace('rp', '_rp') for i in df.columns]
    
    df.reset_index(drop=True, inplace=True)
    
    if 'unnamed:_0' in df.columns:
        df = df.drop(columns = ['unnamed:_0'])
        
    for i in schema:
        if i['name'] in df.columns:
            if i['type'] == 'FLOAT':
                df[i['name']] = df[i['name']].apply(lambda x: str(
                    x).lower().replace('.', '').replace('rp', '') if x != 0 else x)
                df[i['name']] = pd.to_numeric(
                    df[i['name']], errors="coerce").fillna(0).astype(float)
            elif i['type'] == 'INTEGER':
                df[i['name']] = pd.to_numeric(
                    df[i['name']], errors="coerce").fillna(0).astype(int)
            elif i['type'] == 'STRING':
                df[i['name']] = df[i['name']].astype(str)
            elif i['type'] == 'TIMESTAMP' or i['type'] == 'DATE':
                df[i['name']] = pd.to_datetime(df[i['name']], errors='coerce', format="%d-%m-%Y %H:%M")
        else:
            df[i['name']] = None
            
    df['folder'] = official_store_name
    df['load_timestamp'] = datetime.now()

    df = df.replace('nan', None)

    return df

def load_to_bq(df, bq_client, schema):

    job = bq_client.load_table_from_dataframe(
        df, 'sirclo-prod.temp_append.bronze_seller_center_commerce_shopee_livestreaming_detail_changes', job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema,
            autodetect=False
        )
    )
    print(job.result())


def merge(bq_client, schema):
    columns = [i['name'] for i in schema]

    update_set = ', '.join(f"T.{col} = S.{col} " for col in columns)
    insert_set = ', '.join(col for col in columns)

    query = f"""
        MERGE sirclo-prod.bronze_seller_center_commerce.shopee_livestreaming_detail T
            USING sirclo-prod.temp_append.bronze_seller_center_commerce_shopee_livestreaming_detail_changes S
            ON COALESCE(CAST(T.folder as string), "NULL") = COALESCE(CAST(S.folder as string), "NULL")
            AND COALESCE(CAST(T.waktu_dimulai as string), "NULL") = COALESCE(CAST(S.waktu_dimulai as string), "NULL")
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_set}) VALUES({insert_set})
    """

    print(query)
    job = bq_client.query(query)
    print(job.result())

def main(os_key):
    config = get_config(os_key, "shopee")
    official_store_name = config['official_store_name']

    fs = gcsfs.GCSFileSystem(project="sirclo-data-prod")
    bq_client = bigquery.Client()

    schema = open(os.getcwd() + '/transform/shopee/seller_center/live_stream_detail/schema.json')
    schema = json.load(schema)
    
    end_date = date.today()
    start_date   = end_date - timedelta(days=30)
    
    df = extract(fs, official_store_name, start_date, end_date)
    df = transform(df, official_store_name, schema)
    load_to_bq(df, bq_client, schema)
    merge(bq_client, schema)

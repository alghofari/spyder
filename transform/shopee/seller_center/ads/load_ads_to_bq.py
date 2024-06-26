import csv
import gcsfs
import pandas as pd
from datetime import date
from datetime import datetime
from google.cloud import bigquery

from helpers.creds_helper import get_config


def load_from_gcs(fs, official_store_name, start_date, end_date):
    with fs.open(f'sirclo-data-marketplace/assets/excel/shopee/ads/{official_store_name}/{date.today()}/Shopee_Ads_{start_date}_{end_date}.csv', 'r') as f:
        datas = csv.reader(f)
        rows = list(datas)
    return rows

def transform(rows, official_store_name):

    headers = [i.lower().replace(' ','_').replace('/','_atau_').replace('(', '').replace(')', '') for i in rows[6]]
    df = pd.DataFrame(rows[7:], columns = headers)

    df['folder'] = official_store_name
    df['upload_timestamp'] = datetime.now()

    df = df.replace('nan', None)

    return df

def load_to_bq(bq_client, df):

    job = bq_client.load_table_from_dataframe(
        df, 'sirclo-prod.bronze_seller_center_commerce.shopee_ads', job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        autodetect=True
                    )
    )
    print(job.result())

def main(start_date, end_date, os_key):
    config = get_config(os_key, "shopee")
    official_store_name = config['official_store_name']

    fs        = gcsfs.GCSFileSystem()
    bq_client = bigquery.Client()

    rows = load_from_gcs(fs, official_store_name, start_date, end_date)
    df   = transform(rows, official_store_name)

    load_to_bq(bq_client, df)
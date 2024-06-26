import os
import pathlib
import pandas as pd
import datetime
import json
from google.cloud import storage, bigquery
from helpers.cloud_storage_helper import load_gcs_json
from helpers.bigquery_helper import df_to_bq
client = bigquery.Client()

def schema_to_df(json_schema, data_dict):
    f = open(json_schema)
    schema = json.load(f)
    list_columns = [row["name"] for row in schema]
    df = pd.DataFrame(columns=list_columns)
    for col in list_columns:
        try:
            value = [data_dict[col]]
        except:
            value = None
        df[col] = value
    return df

def main(bucket_name: str, base_path: str, table_name: str, schema_path: str, timestamp: datetime, run_date: str):
    base_dir = os.getcwd() + f'/{base_path}/{run_date}'
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    google_client = storage.Client()
    blobs = google_client.list_blobs(bucket_name, prefix=f"{base_path}/{run_date}/")
    df_final = pd.DataFrame()
    for blob in blobs:
        data = load_gcs_json(bucket_name=bucket_name, blob_name=str(blob.name), client=google_client)
        print(f"Parsing json of: {str(blob.name)}")
        try:
            df = schema_to_df(schema_path, data["data"])
            df['load_timestamp'] = timestamp
            df_final = pd.concat([df_final, df], ignore_index=True)
        except Exception as e:
            print(f"{str(blob.name)} is not sufficient", e)
            continue
    df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
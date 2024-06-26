import pandas as pd
import datetime
import os
import re
import pathlib
from google.cloud import bigquery, storage
from helpers.cloud_storage_helper import list_blob_gcs, download_blob_to_local, load_gcs_json
from helpers.bigquery_helper import df_to_bq
import gspread

client = bigquery.Client()

def get_category(string, sep="/"):
    result = string.split(sep, 1)[1]
    return result


def download_from_gcs(category: str, base_dir: str, base_path: str, bucket_name: str, run_date: str):
    list_blob = list_blob_gcs(bucket_name, f"{base_path}/{run_date}/{category}")
    for index, file in enumerate(list_blob):
        print(f"list file in blob gcs: {file}")
        file_name = f'{base_dir}/'+re.sub('.*'+'/', '', file)
        download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)
        print(f'download finish for {category}{index}')


def parsing_json(data: dict):
    excluded_columns = []
    data = data["data"]["pdpGetLayout"]
    df = pd.json_normalize(data["basicInfo"])
    for row in data["components"]:
        if row["name"] == "product_content":
            content = row["data"][0]
            df_content = pd.json_normalize(content)
            df = pd.concat([df, df_content], axis=1)
    column_names = list(df.columns.values)
    column_names = [text.replace('__', '').replace('.', '__').lower() for text in column_names]
    df.columns = column_names
    return df

def get_categories():
    google_application_credentials = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    gc = gspread.service_account(filename=google_application_credentials)
    sh = gc.open_by_key('1jvrwFwpunb25RYeRxVtg_dZqrF8o-XVmByKwl-LFv5A')
    worksheet = sh.worksheet("Tokopedia Schedule")
    return worksheet.col_values(1)[1:]

def main(category: str, base_path: str, timestamp: datetime, bucket_name: str, table_name: str, schema_path:str):
    run_date = timestamp.strftime("%Y-%m-%d")
    #start= time.time()
    categories = get_categories()
    for category in categories:
        category = category.split('/',4)[4]    
        category = get_category(category)
        base_dir = os.getcwd() + f'/{base_path}/{run_date}'
        pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
        print(category)
        google_client = storage.Client()
        blobs = google_client.list_blobs(bucket_name, prefix=f"{base_path}/{run_date}/{category}")
        df_final = pd.DataFrame()
        for blob in blobs:
            data = load_gcs_json(bucket_name=bucket_name, blob_name=str(blob.name), client=google_client)
            print(f"Parsing json of: {str(blob.name)}")
            try:
                df = parsing_json(data)
                df.insert(0, 'category_name', category)
                df.insert(0, 'load_timestamp', timestamp)
                df = df.drop("typename", axis=1)
                df_final = pd.concat([df_final, df], ignore_index=True)
            except Exception as e:
                print(f"{str(blob.name)} is not sufficient", e)
                continue
        try:
            df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
        except:
            print("wrong schema for category ",category)
            continue 
        #end = time.time()
        #print("The time of execution of above program is :",(end - start) * 10 ** 3, "ms")
        print(f"Data has been inserted to {table_name}")
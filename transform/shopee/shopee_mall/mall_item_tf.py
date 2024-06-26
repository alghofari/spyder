import datetime
import logging
import os
import pathlib
import pandas as pd
import json

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import list_blob_gcs, download_blob_to_local, load_gcs_json
from google.cloud import storage


def parsing_json(data, schema_path): 
    payload = data["data"]["sections"][0]["data"]["item"]
    df = pd.DataFrame.from_dict(payload)

    list_col_bq = read_schema(schema_path)
    for col in df.columns : 
        try :  
            if col not in list_col_bq : 
                df = df.drop(col,axis=1)
        except :    
            print(f"col {col} not exist")
    return df

def handle_nonrecord_type(data, schema): 
    for field in schema:
        if field['type'] != 'RECORD' and field['name'] not in data.columns:  
            column = field['name']
            data[column] = None
    
    for col in data.columns: 
        if col not in schema: 
            drop_column(df=data, column_name=col)
    return data

def get_subcolumn(data,column, schema): 
    nested_field_name = []
    for field in schema:
        if field['name'] == column and field['type'] == 'RECORD': 
            for nested_field in field['fields']:
                nested_field_name.append(nested_field['name'])
    return nested_field_name

def handle_record_type(data, schema): 
    for field in schema: 
        if field['type'] == 'RECORD' and field['name'] not in data.columns: 
            subcolumn = get_subcolumn(data=data,column=field['name'], schema=schema)
            field_dict = {field: None for field in subcolumn}
            column = field['name']
            # data[column] = field_dict
    return data

def read_schema(jsonfile) : 
    # Open the JSON file in read mode
    with open(jsonfile, 'r') as file:
        # Load JSON data from the file
        data = json.load(file)

    # Extract "name" values into a list
    names = [item["name"] for item in data]

    return names

def set_column_to_none(df, column_name, function):
    try:
        df[column_name] = df.apply(function, axis=1)
    except:
        df[column_name] = None

def drop_column(df, column_name):
    try :
        df = df.drop(columns=[column_name])
    except : 
        print(f"column {column_name} not exist")


def main(shop: str, bucket_name: str, base_path: str, table_name: str, schema_path: str, run_date: str):
    
    base_dir = os.getcwd() + f"/{base_path}/{run_date}"
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)

    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=f"{base_path}/{run_date}/{shop}")
    print(f"inserting {shop} to bq")
    for blob in blobs :
        print(blob.name)
        data = load_gcs_json(bucket_name=bucket_name, blob_name=str(blob.name), client=client)
        logging.info(f"Parsing json of: {str(blob.name)}")
        try:
            df = parsing_json(data, schema_path)
            print("finish parsing_json")
            df.insert(0, "load_timestamp", datetime.datetime.now())

            with open(schema_path) as f:
                schema = json.load(f)
            #handling non record data
            df = handle_nonrecord_type(data=df, schema=schema)
            print("finish handle nonrecord type")
            df = handle_record_type(data=df, schema=schema)
            print("finish handle record type")

            drop_column(df,"flash_sale_infos")
            drop_column(df,"overlay_images")
            
            df_to_bq(df, table_name, schema_path, "load_timestamp", "mall_item")
            print(f"{str(blob.name)} has been inserted to {table_name}")
        except Exception as e:
            logging.error(f"Error detected at json level from {str(blob.name)} caused by:",e)
            continue
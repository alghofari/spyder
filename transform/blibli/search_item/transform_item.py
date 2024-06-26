import datetime
import logging
import os
import pathlib
import pandas as pd
import json
from pandas import json_normalize

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import list_blob_gcs, download_blob_to_local, load_gcs_json
from google.cloud import storage


def get_category(string, sep="/"):
    result = string.split(sep, 1)[0]
    return result


def download_from_gcs(category: str, base_dir: str, base_path: str, bucket_name: str, run_date: str):
    list_blob = list_blob_gcs(bucket_name, f"{base_path}/{run_date}/{category}")
    for index, file in enumerate(list_blob):
        file_name = f"{base_dir}/{category}_page_{str(index)}.json"
        download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)


def delete_dict_columns(df, column_names):
    for col_name in column_names:
        df = df.drop(col_name, axis=1)
    return df


def flatten_dict_columns(df, column_names):
    for col_name in column_names:
        df = pd.concat([df.drop([col_name], axis=1), json_normalize(df[col_name])], axis=1)
    return df


def array_str_columns(df, column_names):
    for col_name in column_names:
        df[col_name] = df[col_name].apply(lambda x: str(x))
    return df


def join_list_keys(dictionary, column_names):
    for col_name in column_names:
        value = dictionary[col_name]
        dictionary[col_name] = ",".join(value)
    return dictionary


def add_missing_columns(df, schema_path, timestamp):
    # Iterate over JSON schema files in the directory

    with open(schema_path, "r") as file:
        schema = json.load(file)

        # Convert the JSON schema to a list of column names

        columns_in_schema = [column["name"] for column in schema]

        # Get the list of columns in the DataFrame

        columns_in_df = list(df.columns)

        # Find columns in the DataFrame that are not in the schema

        extra_columns = set(columns_in_df) - set(columns_in_schema)

        # Remove extra columns from the DataFrame

        df.drop(extra_columns, axis=1, inplace=True)

        # Add missing columns to the DataFrame with None as the initial value

        missing_columns = set(columns_in_schema) - set(columns_in_df)
        for column in missing_columns:
            if column == "load_timestamp":
                df[column] = timestamp
            else:
                df[column] = None

    return df


def parsing_json_recommend_api(data: dict):
    # excluded_columns = ["label_ids", "images", "video_info_list", "transparent_background_image", "highlight_video", "global_sold_count"]
    # df = pd.DataFrame()
    product_details = []
    df = None
    columns_no_list = ['images', 'categoryIdHierarchy', 'categoryNameHierarchy']

    for product in data["data"]["products"]:
        product_detail = product
        if product_detail not in product_details:
            product_detail = join_list_keys(product_detail, columns_no_list)
            product_details.append(product_detail)
        df = pd.DataFrame(product_details)
    
    df.rename(columns={'id': 'productId'}, inplace=True)
    df.rename(columns={'name': 'productName'}, inplace=True)

    # List of columns to flatten
    columns_to_flatten = ['price', 'review', 'storeClosingInfo', 'badge', 'soldRangeCount']

    # Call the function to flatten specific columns
    df = flatten_dict_columns(df, columns_to_flatten)

    # Rename the 'OldName' column to 'NewName'
    df.rename(columns={'id': 'soldRangeCountId'}, inplace=True)
    df.rename(columns={'en': 'soldRangeCountEn'}, inplace=True)

    # df.rename(columns={'name': 'campaignName'}, inplace=True)
    # df.rename(columns={'code': 'campaignCode'}, inplace=True)

    return df


def read_schema(jsonfile):
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
    except Exception as e:
        df[column_name] = None
        print(e)


def null_to_record_transform(column_name, value):
    if column_name == "free_shipping_info":
        if value is None:
            value = {
              "type": None,
              "image_hash": None,
              "image_height": None,
              "image_width": None
            }
    elif column_name == "add_on_deal_info":
        if value is None:
            value = {
              "add_on_deal_id": None,
              "add_on_deal_label": None,
              "sub_type": None,
              "status": None
            }
    elif column_name == "bundle_deal_info":
        if value is None:
            value = {
              "bundle_deal_id": None,
              "bundle_deal_label": None
            }
    elif column_name == "voucher_info":
        if value is None:
            value = {
              "promotion_id": None,
              "voucher_code": None,
              "label": None
            }

    return value


def main(category: str, page: int, bucket_name: str, base_path: str, table_name: str, timestamp: datetime, schema_path: str):
    category = get_category(category)
    run_date = timestamp.strftime("%Y-%m-%d")
    base_dir = os.getcwd() + f"/{base_path}/{run_date}"
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)


    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=f"{base_path}/{run_date}/{category}_page_{page}")


    for blob in blobs:
        data = load_gcs_json(bucket_name=bucket_name, blob_name=str(blob.name), client=client)
        try:
            df = parsing_json_recommend_api(data)
            df.insert(0, "category_id", category)
            df.insert(0, "load_timestamp", timestamp)
            df_final = add_missing_columns(df, schema_path, timestamp)
            df_to_bq(df_final, table_name, schema_path, "load_timestamp", "search_item")
            logging.info(f"Category: {category} has been inserted")
        except Exception as e:
            logging.error(f"Error detected at json level from {str(blob.name)} caused by:", e)
            continue

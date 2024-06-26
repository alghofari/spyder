import datetime
import logging
import os
import pathlib
import pandas as pd
import json

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import list_blob_gcs, download_blob_to_local, load_gcs_json
from google.cloud import storage


def get_category(string, sep="_"):
    result = string.split(sep, 1)[0]
    return result


def download_from_gcs(category: str, base_dir: str, base_path: str, bucket_name: str, run_date: str):
    list_blob = list_blob_gcs(bucket_name, f"{base_path}/{run_date}/{category}")
    for index, file in enumerate(list_blob):
        logging.info(f"list file in blob gcs: {file}")
        file_name = f"{base_dir}/{category}_page_{str(index)}.json"
        download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)
        logging.info(f"download finish for {category}{index}")


def parsing_json_search_item_api(data: dict):
    excluded_columns = ["label_ids", "images", "video_info_list", "transparent_background_image", "highlight_video", "global_sold_count"]
    df = pd.DataFrame()
    for i in data["items"]:
        columns = []
        values = []
        try:
            for j in i["item_basic"]:
                column_name = str(j)
                if column_name in excluded_columns:
                    continue
                value = i["item_basic"][j]
                if column_name == "image":
                    value = "https://cf.shopee.co.id/file/" + value
                if column_name.startswith("price") and value > 0:
                    value = int(value / 100000)
                columns.append(column_name)
                values.append(value)
        except Exception as e:
            logging.error("Error detected at row level caused by:",e)
            continue

        df_temp = pd.DataFrame([values], columns=columns)
        df = pd.concat([df, df_temp], ignore_index=True)

    return df


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

def parsing_json_recommend_api(data: dict, schema_path: str):

    payload = data["data"]["sections"][0]["data"]["item"]
    df = pd.DataFrame.from_dict(payload)

    list_col_bq = read_schema(schema_path)
    for col in df.columns : 
        try :  
            if col not in list_col_bq : 
                df = df.drop(col,axis=1)
        except :    
            print(f"col {col} not exist")

    df["spl_repayment_label_text"] = None
    df["spl_repayment_label_repayment"] = None
    
    if "exclusive_price_info" in df.columns : 
        df = df.drop("exclusive_price_info",axis=1)
        df["exclusive_price_info"] = None
    else : 
        df["exclusive_price_info"] = None
    
    set_column_to_none(df, "image", lambda row: "https://cf.shopee.co.id/file/" + row["image"])
    set_column_to_none(df, "size_chart", lambda row: str(row["size_chart"]))
    set_column_to_none(df, "price", lambda row: int(row["price"] / 100000))
    set_column_to_none(df, "price_min", lambda row: int(row["price_min"] / 100000))
    set_column_to_none(df, "price_max", lambda row: int(row["price_max"] / 100000))
    set_column_to_none(df, "price_min_before_discount", lambda row: int(row["price_min_before_discount"] / 100000))
    set_column_to_none(df, "price_max_before_discount", lambda row: int(row["price_max_before_discount"] / 100000))
    set_column_to_none(df, "price_before_discount", lambda row: int(row["price_before_discount"] / 100000))
    set_column_to_none(df, "free_shipping_info", lambda row: null_to_record_transform("free_shipping_info", row["free_shipping_info"]))
    set_column_to_none(df, "add_on_deal_info", lambda row: null_to_record_transform("add_on_deal_info", row["add_on_deal_info"]))
    set_column_to_none(df, "bundle_deal_info", lambda row: null_to_record_transform("bundle_deal_info", row["bundle_deal_info"]))
    set_column_to_none(df, "voucher_info", lambda row: null_to_record_transform("voucher_info", row["voucher_info"]))

    list_column_check = ['coin_earn_label', 'view_count', 'itemid', 'show_official_shop_label_in_title', 'can_use_cod', 'show_official_shop_label', 'cb_option', 
                         'is_adult', 'is_group_buy_item', 'is_service_by_shopee', 'is_category_failed', 'cmt_count', 'reference_item_id', 'show_discount', 
                         'tier_variations', 'is_preferred_plus_seller', 'item_status', 'flag', 'is_cc_installment_payment_eligible', 'can_use_wholesale', 
                         'badge_icon_type', 'coin_info', 'show_free_shipping', 'is_official_shop', 'liked_count', 'currency', 'is_live_streaming_price', 
                         'brand', 'item_type', 'can_use_bundle_deal', 'sold', 'deep_discount_skin', 'show_shopee_verified_label', 
                         'has_model_with_available_shopee_stock', 'is_on_flash_sale', 'bundle_deal_id', 'preview_info', 'item_rating', 'shop_location', 
                         'stock', 'ctime', 'liked', 'discount', 'spl_installment_tenure', 'catid', 'name', 'pack_size', 'status', 
                         'group_buy_info', 'has_group_buy_stock', 'is_non_cc_installment_payment_eligible', 'raw_discount', 'welcome_package_info', 
                         'historical_sold', 'shopid', 'has_lowest_price_guarantee', 'hidden_price_display', 'shopee_verified', 'welcome_package_type', 'is_mart']
    
    for col in list_column_check : 
        if col not in df.columns : 
            df[col] = None

    return df


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


def main(category: str, page: int, bucket_name: str, base_path: str, table_name: str, timestamp: datetime, schema_path: str, sort_by: str):
    # category = get_category(category)
    run_date = timestamp.strftime("%Y-%m-%d")
    base_dir = os.getcwd() + f"/{base_path}/{run_date}"
    pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    logging.info(category)

    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=f"{base_path}/{run_date}/{category}_page_{page}")
    print(f"inserting {category} to bq")
    for blob in blobs :
        data = load_gcs_json(bucket_name=bucket_name, blob_name=str(blob.name), client=client)
        logging.info(f"Parsing json of: {str(blob.name)}")
        try:
            if sort_by == "popular":
                df = parsing_json_recommend_api(data, schema_path)
            else:
                df = parsing_json_search_item_api(data)

            df.insert(0, "category_name", category)
            df.insert(0, "load_timestamp", timestamp)
            df_to_bq(df, table_name, schema_path, "load_timestamp", "search_item")
            logging.info(f"{str(blob.name)} has been inserted to {table_name}")
        except Exception as e:
            logging.error(f"Error detected at json level from {str(blob.name)} caused by:",e)
            continue

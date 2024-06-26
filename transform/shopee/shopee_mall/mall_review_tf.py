import datetime
import logging
import os
import pathlib
import pandas as pd
import json

from helpers.bigquery_helper import df_to_bq
from helpers.cloud_storage_helper import list_blob_gcs, download_blob_to_local, load_gcs_json
from google.cloud import storage
from datetime import datetime,timedelta

# Need to note that if still running on local, there are some few lines that still not needed


def download_from_gcs(shop: str, base_path: str, bucket_name: str):
    print(f"check gcs for {shop}")
    list_blob = list_blob_gcs(bucket_name, f"{base_path}/2023-05-21/{shop}")
    print(list_blob)

    # COMMENT THIS because STILL RUNNING ON LOCAL
    # for index, file in enumerate(list_blob):
    #     logging.info(f"list file in blob gcs: {file}")
    #     file_name = f"{base_dir}/{category}_page_{str(index)}.json"
    #     download_blob_to_local(bucket_name=bucket_name, local_file_name=file_name, gcs_blob_name=file)
    #     logging.info(f"download finish for {shop}_{index}")

# create function to convert json file into dataframe
def json_cleansing(file) : 
    with open(file) as user_file:
        parsed_json = json.load(user_file)
        comment, rating_star, detailed_rating, item_id, orderid, nama_barang = [],[],[],[],[], []

    for item in range(len(parsed_json['data']['ratings'])) : 
       d = parsed_json['data']['ratings'][item]
       comment.append(d['comment'])
       rating_star.append(d['rating_star'])
       orderid.append(d['orderid'])
       detailed_rating.append(d['detailed_rating'])
       nama_barang.append(d['product_items'][0]['name'])
    
    zipped = list(zip(orderid, comment, rating_star, detailed_rating, nama_barang))
    df = pd.DataFrame(zipped, columns=['orderid', 'comment', 'rating_star', 'detailed_rating', 'nama_barang'])

    # Extract the nested fields into separate columns
    df['delivery_service'] = df['detailed_rating'].apply(lambda x: x['delivery_service'] if isinstance(x, dict) else None)
    df['product_quality'] = df['detailed_rating'].apply(lambda x: x['product_quality'] if isinstance(x, dict) else None)
    df['seller_service'] = df['detailed_rating'].apply(lambda x: x['seller_service'] if isinstance(x, dict) else None)
    df = df.drop('detailed_rating', axis=1)
    return df


def main(base_path: str, bucket_name: str, shop, table_name, schema_path):

    # COMMENT THIS because STILL RUNNING ON LOCAL
    # shops = ['unileverindonesia', 'mondelezofficial']
    # date_review = now - timedelta(days=1)
    # run_date = date_review.strftime("%Y-%m-%d")
    # print(run_date)
    # base_dir = os.getcwd() + f"/{base_path}/{run_date}"
    # pathlib.Path(base_dir).mkdir(parents=True, exist_ok=True)
    
    # download_from_gcs(shop, base_path, bucket_name)
    list_file = os.listdir(f'{base_path}/{shop}')
    list_file = [f'{base_path}/{shop}/{file}' for file in list_file]
    print(list_file)

    # make all dataframe into just one dataframe
    all_data_dict = {}
    count = 0
    for json_file in list_file : 
        try : 
            data = json_cleansing(json_file)
            all_data_dict[f'data_{str(count)}'] = data
        except : 
            print("data empty")
        count += 1
    
    
    all_data = pd.concat(all_data_dict.values(), axis=0, ignore_index=True)
    print(all_data.info())
    df_to_bq(all_data, table_name, schema_path, "load_timestamp", "shopee_mall_review")
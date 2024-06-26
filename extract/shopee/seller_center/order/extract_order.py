import os
from datetime import datetime
from datetime import timedelta
from google.cloud import bigquery

from extract.shopee.seller_center.order import request_order
from extract.shopee.seller_center.order import download_order
from transform.shopee.seller_center.order import load_order_to_bq

from helpers.creds_helper import get_creds


def get_null_data(official_store_name):
    client = bigquery.Client(project="sirclo-prod")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    with open(os.getcwd() + '/extract/shopee/seller_center/order/query_assets/query_missing.sql') as f:
        query = f.read()
        query = query.replace(f"{{start_date}}", f"{start_date.year}-{start_date.month}-{start_date.day}")
        query = query.replace(f"{{end_date}}", f"{end_date.year}-{end_date.month}-{end_date.day}")
        query = query.replace(f"{{official_store_name}}", f"{official_store_name}")
        print(query)

    results = client.query(query)
    order_date = [result['order_date'] for result in results][0]
    return order_date
    
def main(official_store_name, storage_client, os_key):
    null_date = get_null_data(official_store_name)
    for date in null_date:
        days_before = date - timedelta(days=1)
        print(f"Getting {official_store_name} order from {days_before} to {date}")
        request_order.main(days_before, date, official_store_name, storage_client)
        download_order.main(days_before, date, official_store_name, storage_client)
        load_order_to_bq.main(days_before, date, os_key)
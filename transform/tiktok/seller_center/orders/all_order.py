import json
import os
import sys
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from google.cloud import bigquery

from helpers.bigquery_helper import delete_table, check_table_exist


def read_data(report_file):
    # read csv to dataframe
    dataframe = pd.read_csv(report_file)

    # replace nan value to none
    dataframe = dataframe.replace({np.nan: None})

    # rename columns
    dataframe = dataframe.rename(columns=lambda x: x.strip())
    columns = {
        "Order ID": "order_id",
        "Order Status": "order_status",
        "Order Substatus": "order_substatus",
        "Cancelation/Return Type": "cancelation_return_type",
        "Normal or Pre-order": "normal_or_pre_order",
        "SKU ID": "sku_id",
        "Seller SKU": "seller_sku",
        "Product Name": "product_name",
        "Variation": "variation",
        "Quantity": "quantity",
        "Sku Quantity of return": "sku_quantity_of_return",
        "SKU Unit Original Price": "sku_unit_original_price",
        "SKU Subtotal Before Discount": "sku_subtotal_before_discount",
        "SKU Platform Discount": "sku_platform_discount",
        "SKU Seller Discount": "sku_seller_discount",
        "SKU Subtotal After Discount": "sku_subtotal_after_discount",
        "Shipping Fee After Discount": "shipping_fee_after_discount",
        "Original Shipping Fee": "original_shipping_fee",
        "Shipping Fee Seller Discount": "shipping_fee_seller_discount",
        "Shipping Fee Platform Discount": "shipping_fee_platform_discount",
        "Taxes": "taxes",
        "Order Amount": "order_amount",
        "Order Refund Amount": "order_refund_amount",
        "Created Time": "created_time",
        "Paid Time": "paid_time",
        "RTS Time": "rts_time",
        "Shipped Time": "shipped_time",
        "Delivered Time": "delivered_time",
        "Cancelled Time": "cancelled_time",
        "Cancel By": "cancel_by",
        "Cancel Reason": "cancel_reason",
        "Fulfillment Type": "fulfillment_type",
        "Warehouse Name": "warehouse_name",
        "Tracking ID": "tracking_id",
        "Delivery Option": "delivery_option",
        "Shipping Provider Name": "shipping_provider_name",
        "Buyer Message": "buyer_message",
        "Buyer Username": "buyer_username",
        "Recipient": "recipient",
        "Phone #": "phone",
        "Zipcode": "zipcode",
        "Country": "country",
        "Province": "province",
        "Regency and City": "regency_and_city",
        "Districts": "districts",
        "Villages": "villages",
        "Detail Address": "detail_address",
        "Additional address information": "additional_address_information",
        "Payment Method": "payment_method",
        "Weight(kg)": "weight_kg",
        "Product Category": "product_category",
        "Package ID": "package_id",
        "Seller Note": "seller_note",
        "Checked Status": "checked_status",
        "Checked Marked by": "checked_marked_by",
        "Store Name": "store_name"
    }
    dataframe = dataframe.rename(columns=columns)

    # replace currency and decimal character to empty string
    dataframe['sku_unit_original_price'] = dataframe['sku_unit_original_price'].replace("[IDR.]", "", regex=True)
    dataframe['sku_subtotal_before_discount'] = dataframe['sku_subtotal_before_discount'].replace("[IDR.]", "", regex=True)
    dataframe['sku_platform_discount'] = dataframe['sku_platform_discount'].replace("[IDR.]", "", regex=True)
    dataframe['sku_seller_discount'] = dataframe['sku_seller_discount'].replace("[IDR.]", "", regex=True)
    dataframe['sku_subtotal_after_discount'] = dataframe['sku_subtotal_after_discount'].replace("[IDR.]", "", regex=True)
    dataframe['shipping_fee_after_discount'] = dataframe['shipping_fee_after_discount'].replace("[IDR.]", "", regex=True)
    dataframe['original_shipping_fee'] = dataframe['original_shipping_fee'].replace("[IDR.]", "", regex=True)
    dataframe['shipping_fee_seller_discount'] = dataframe['shipping_fee_seller_discount'].replace("[IDR.]", "", regex=True)
    dataframe['shipping_fee_platform_discount'] = dataframe['shipping_fee_platform_discount'].replace("[IDR.]", "", regex=True)
    dataframe['taxes'] = dataframe['taxes'].replace("[IDR.]", "", regex=True)
    dataframe['order_amount'] = dataframe['order_amount'].replace("[IDR.]", "", regex=True)
    dataframe['order_refund_amount'] = dataframe['order_refund_amount'].replace("[IDR.]", "", regex=True)

    # replace tab value to empty string
    dataframe['created_time'] = dataframe['created_time'].replace("\t", "", regex=True)
    dataframe['paid_time'] = dataframe['paid_time'].replace("\t", "", regex=True)
    dataframe['rts_time'] = dataframe['rts_time'].replace("\t", "", regex=True)
    dataframe['shipped_time'] = dataframe['shipped_time'].replace("\t", "", regex=True)
    dataframe['delivered_time'] = dataframe['delivered_time'].replace("\t", "", regex=True)
    dataframe['cancelled_time'] = dataframe['cancelled_time'].replace("\t", "", regex=True)
    dataframe['zipcode'] = dataframe['zipcode'].replace("\t", "", regex=True).replace({"": None})
    dataframe['package_id'] = dataframe['package_id'].replace("\t", "", regex=True).replace({"": None})

    # casting data type
    dataframe['order_id'] = dataframe['order_id'].astype(str)
    dataframe['sku_id'] = dataframe['sku_id'].astype(str)
    dataframe['sku_unit_original_price'] = dataframe['sku_unit_original_price'].astype(float)
    dataframe['sku_subtotal_before_discount'] = dataframe['sku_subtotal_before_discount'].astype(float)
    dataframe['sku_platform_discount'] = dataframe['sku_platform_discount'].astype(float)
    dataframe['sku_seller_discount'] = dataframe['sku_seller_discount'].astype(float)
    dataframe['sku_subtotal_after_discount'] = dataframe['sku_subtotal_after_discount'].astype(float)
    dataframe['shipping_fee_after_discount'] = dataframe['shipping_fee_after_discount'].astype(float)
    dataframe['original_shipping_fee'] = dataframe['original_shipping_fee'].astype(float)
    dataframe['shipping_fee_seller_discount'] = dataframe['shipping_fee_seller_discount'].astype(float)
    dataframe['shipping_fee_platform_discount'] = dataframe['shipping_fee_platform_discount'].astype(float)
    dataframe['taxes'] = dataframe['taxes'].astype(float)
    dataframe['order_amount'] = dataframe['order_amount'].astype(float)
    dataframe['order_refund_amount'] = dataframe['order_refund_amount'].astype(float)
    dataframe['created_time'] = pd.to_datetime(dataframe['created_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    dataframe['paid_time'] = pd.to_datetime(dataframe['paid_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    dataframe['rts_time'] = pd.to_datetime(dataframe['rts_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    dataframe['shipped_time'] = pd.to_datetime(dataframe['shipped_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    dataframe['delivered_time'] = pd.to_datetime(dataframe['delivered_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    dataframe['cancelled_time'] = pd.to_datetime(dataframe['cancelled_time'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    dataframe['tracking_id'] = dataframe['tracking_id'].astype(str)
    dataframe['weight_kg'] = dataframe['weight_kg'].astype(float)
    dataframe['package_id'] = dataframe['package_id'].astype(str)
    dataframe['variation'] = dataframe['variation'].astype(str)

    # add new column
    dataframe['load_timestamp'] = datetime.now()

    return dataframe


def write_to_bigquery(client, table_schema, dataframe, table_id, load_method):
    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        write_disposition=load_method,
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def execute_query(client, query):
    client.query(query).result()  # Waits for job to complete.


def main(download_path):
    client = bigquery.Client()

    project = "sirclo-prod"
    dataset = "bronze_seller_center_commerce"
    temp_table_name = f"{project}.{dataset}.tiktok_order_temp"
    base_path = os.getcwd()

    delete_table(temp_table_name)

    try:
        report_files = glob(download_path + "/*.csv")
        for report_filename in report_files:
            df = read_data(report_filename)
            with open(f"{base_path}/schema.json", "r") as f:
                schema = json.loads(f.read())
            write_to_bigquery(client, schema, df, temp_table_name, "WRITE_APPEND")
    except Exception as e:
        print(e)

    if check_table_exist(temp_table_name):
        with open(f"{base_path}/query.sql", "r") as f:
            merge_query = f.read().format(temp_table_name=temp_table_name)

        execute_query(client, merge_query)
        delete_table(temp_table_name)


if __name__ == "__main__":
    asset_path = sys.argv[1]
    main(asset_path)

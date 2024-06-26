import os
import json
import gcsfs
import gspread
import pandas as pd
from datetime import date
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

from helpers.creds_helper import get_config


def transform(official_store_name, start_date, end_date, schema):
    fs = gcsfs.GCSFileSystem(project="sirclo-data-prod")

    with fs.open(f'gs://sirclo-data-marketplace/assets/excel/shopee/sales/{official_store_name}/{date.today()}/Shopee_Order_{start_date}_{end_date}.xlsx') as f:
        df = pd.read_excel(f, thousands=".", decimal=",")

    new_col = {
        'No. Pesanan': 'order_number', 'Status Pesanan': 'order_status', 'Alasan Pembatalan': 'cancel_reason', 
        'Status Pembatalan/ Pengembalian': 'cancel_return_status', 'No. Resi': 'receipt_number', 'Opsi Pengiriman': 'shipping_option', 
        'Antar ke counter/ pick-up': 'seller_shipping_option', 'Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)': 'due_date_shipping_product', 
        'Waktu Pengiriman Diatur': 'arrange_delivery_time', 'Waktu Pesanan Dibuat': 'create_delivery_time', 
        'Waktu Pembayaran Dilakukan': 'conducted_delivery_time', 'SKU Induk': 'sku', 'Nama Produk': 'product_name', 
        'Nomor Referensi SKU': 'sku_reference_number', 'Nama Variasi': 'variation_name', 'Harga Awal': 'initiate_price', 
        'Harga Setelah Diskon': 'discount_incl_price', 'Jumlah': 'total_goods', 'Total Harga Produk': 'total_product_price', 
        'Total Diskon': 'total_discount', 'Diskon Dari Penjual': 'discount_from_seller', 'Diskon Dari Shopee': 'discount_from_shopee', 
        'Berat Produk': 'weight_product', 'Jumlah Produk di Pesan': 'total_order_product', 'Total Berat': 'total_weight', 
        'Voucher Ditanggung Penjual': 'voucher_from_seller', 'Cashback Koin': 'coin_cashback', 'Voucher Ditanggung Shopee': 'voucher_from_shopee', 
        'Paket Diskon': 'discount_package', 'Paket Diskon (Diskon dari Shopee)': 'discount_shopee_package', 'Paket Diskon (Diskon dari Penjual)': 'discount_seller_package', 
        'Potongan Koin Shopee': 'coin_discount_shopee', 'Diskon Kartu Kredit': 'discount_credit_card', 'Ongkos Kirim Dibayar oleh Pembeli': 'buyer_shipping_price', 
        'Estimasi Potongan Biaya Pengiriman': 'estimate_discount_shipping_price', 'Ongkos Kirim Pengembalian Barang': 'return_shipping_price', 
        'Total Pembayaran': 'total_price', 'Perkiraan Ongkos Kirim': 'estimate_shipping_price', 'Catatan dari Pembeli': 'note_from_seller', 
        'Catatan': 'note', 'Username (Pembeli)': 'buyer_username', 'Nama Penerima': 'buyer_name', 'No. Telepon': 'buyer_phone_number', 
        'Alamat Pengiriman': 'buyer_address', 'Kota/Kabupaten': 'buyer_city', 'Provinsi': 'buyer_province', 'Waktu Pesanan Selesai': 'complete_delivery_time'}
    
    df = df.reindex(columns = new_col.keys())
    df.rename(columns = new_col, inplace=True)

    df['folder'] = official_store_name
    df['upload_timestamp'] = datetime.now()

    for i in schema:
        if i['name'] in df.columns:
            if i['type'] == 'FLOAT':
                df[i['name']] = df[i['name']].astype(float)
            elif i['type'] == 'INTEGER':
                df[i['name']] = df[i['name']].astype(int)
            elif i['type'] == 'STRING':
                df[i['name']] = df[i['name']].astype(str)
            elif i['type'] == 'TIMESTAMP':
                df[i['name']] = pd.to_datetime(df[i['name']], errors='coerce')
        else:
            df[i['name']] = None

    df = df.replace('nan', None)
    return df

def load_to_bq(bq_client, df, schema):
    # bq_client = bigquery.Client('sirclo-prod')

    # config              = read_config(os_key)
    # official_store_name = config['official_store_name']

    # schema = open(os.getcwd() + '/transform/shopee/seller_center/order/schema.json')
    # schema = json.load(schema)

    # df = transform(official_store_name,start_date, end_date, schema)
    # official_store_name = official_store_name.replace(' ','_').lower()

    table = 'sirclo-prod.bronze_seller_center_commerce.shopee_order'

    job = bq_client.load_table_from_dataframe(
                df,
                table,
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                    schema = schema,
                    autodetect=False
                )
            )

    try:
        print(job.result())
    except BadRequest as e:
        for e in job.errors:
            print('ERROR: {}'.format(e['message']))

def main(start_date, end_date, os_key):
    config = get_config(os_key, "shopee")
    official_store_name = config['official_store_name']
    
    schema = open(os.getcwd() + '/transform/shopee/seller_center/order/schema.json')
    schema = json.load(schema)

    storage_client = storage.Client()
    storage_client = storage_client.bucket('sirclo-data-marketplace')
    bq_client = bigquery.Client()
    
    df = transform(official_store_name, start_date, end_date, schema)
    load_to_bq(bq_client, df, schema)
    


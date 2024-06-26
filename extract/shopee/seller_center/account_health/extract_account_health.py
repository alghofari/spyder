import sys
import pandas as pd

from datetime import datetime
from google.cloud import bigquery

from selenium.webdriver.common.by import By

from helpers.time_helper import sleep_condition
from extract.shopee.seller_center.helper.helper import click_action


def get_health_data(driver):
    """
    Function to get text from accounthealth page
    Args:
        driver (any): chromedriver.Driver used
    Returns:
        df (list): text character from accounthealth portal 
    """
    driver.get("https://seller.shopee.co.id/portal/accounthealth/home")
    sleep_condition(10,20)

    click_action(driver, xpath='//*[@id="listing"]/section/section[2]/section[1]/div/div').click()
    sleep_condition(20,30)

    click_action(driver, xpath='//*[@id="listing"]/section/section[3]/section[1]/div/div').click()
    sleep_condition(20,30)

    click_action(driver, xpath='//*[@id="fulfillment"]/section/section[2]/section[1]/div/div').click()
    sleep_condition(20,30)
    
    text = driver.find_element(By.XPATH, '//*[@id="metrics"]/div[2]').text

    df = [
        i for i in text.split("\n") 
        if i not in 
        [
            '','Kepuasan Pembeli','Pelayanan Pembeli','Pesanan Terselesaikan','Produk Yang Dilarang',
            'Pelanggaran Produk Berat','Produk Spam','Produk Imitasi','Produk yang Dilarang', 'Produk Pre-order',
            'Jumlah Hari dari Pelanggaran Produk Pre-order', 'Pelanggaran Produk Ringan', 
            'Tingkat Pesanan Tidak Terselesaikan','Tingkat Pembatalan','Tingkat Pengembalian', 'Tingkat Keterlambatan Pengiriman Pesanan',
            'Masa Pengemasan', 'Persentase Chat Dibalas', 'Kecepatan Chat Dibalas','Keseluruhan Penilaian']
        ]
    
    return df

def clean_df(df, official_store_name):
    """
    Function to clean text from accounthealth portal
    Args:
        df (list): text character from accounthealth portal 
        official_store_name (str): official store name for GCS
    Returns:
        None
    """
    produk_toko_saya_metric = {
        'pelanggaran_produk_berat' : df[6],
        'produk_spam': df[11],
        'produk_imitasi': df[16],
        'produk_yang_dilarang': df[21],
        'produk_pre_order': df[26],
        'jumlah_hari_dari_pelanggan_produk_pre_order': df[31],
        'pelanggaran_produk_ringan': df[36]
    }

    produk_target_metric = {
        'pelanggaran_produk_berat' : df[7],
        'produk_spam': df[12],
        'produk_imitasi': df[17],
        'produk_yang_dilarang': df[22],
        'produk_pre_order': df[27],
        'jumlah_hari_dari_pelanggan_produk_pre_order': df[32],
        'pelanggaran_produk_ringan': df[37]
    }

    produk_periode_sebelumnya_metric = {
        'pelanggaran_produk_berat' : df[8],
        'produk_spam': df[13],
        'produk_imitasi': df[18],
        'produk_yang_dilarang': df[23],
        'produk_pre_order': df[28],
        'jumlah_hari_dari_pelanggan_produk_pre_order': df[33],
        'pelanggaran_produk_ringan': df[38]
    }

    produk_poin_penalti_metric = {
        'pelanggaran_produk_berat' : df[9],
        'produk_spam': df[14],
        'produk_imitasi': df[19],
        'produk_yang_dilarang': df[24],
        'produk_pre_order': df[29],
        'jumlah_hari_dari_pelanggan_produk_pre_order': df[34],
        'pelanggaran_produk_ringan': df[39]
    }

    produk_aksi_metric = {
        'pelanggaran_produk_berat' : df[10],
        'produk_spam': df[15],
        'produk_imitasi': df[20],
        'produk_yang_dilarang': df[25],
        'produk_pre_order': df[30],
        'jumlah_hari_dari_pelanggan_produk_pre_order': df[35],
        'pelanggaran_produk_ringan': df[40]
    }

    pesanan_toko_saya_metric = {
        'tingkat_pesanan_tidak_terselesaikan': df[47],
        'tingkat_pembatalan': df[52],
        'tingkat_pengembalian': df[57],
        'tingkat_keterlambatan_pengiriman_pesanan': df[62],
        'masa_pengemasan': df[67]
    }

    pesanan_target_metric = {
        'tingkat_pesanan_tidak_terselesaikan': df[48],
        'tingkat_pembatalan': df[53],
        'tingkat_pengembalian': df[58],
        'tingkat_keterlambatan_pengiriman_pesanan': df[63],
        'masa_pengemasan': df[68]
    }

    pesanan_periode_sebelumnya_metric = {
        'tingkat_pesanan_tidak_terselesaikan': df[49],
        'tingkat_pembatalan': df[54],
        'tingkat_pengembalian': df[59],
        'tingkat_keterlambatan_pengiriman_pesanan': df[64],
        'masa_pengemasan': df[69]
    }

    pesanan_poin_penalti_metric = {
        'tingkat_pesanan_tidak_terselesaikan': df[50],
        'tingkat_pembatalan': df[55],
        'tingkat_pengembalian': df[60],
        'tingkat_keterlambatan_pengiriman_pesanan': df[65],
        'masa_pengemasan': df[70]
    }

    pesanan_aksi_metric = {
        'tingkat_pesanan_tidak_terselesaikan': df[51],
        'tingkat_pembatalan': df[56],
        'tingkat_pengembalian': df[61],
        'tingkat_keterlambatan_pengiriman_pesanan': df[66],
        'masa_pengemasan': df[71]
    }

    pelayanan_toko_saya_metric = {
        'persentase_chat_dibalas': df[78],
        'kecepatan_chat_dibalas': df[83]
    }

    pelayanan_target_metric = {
        'persentase_chat_dibalas': df[79],
        'kecepatan_chat_dibalas': df[84]
    }

    pelayanan_periode_sebelumnya_metric = {
        'persentase_chat_dibalas': df[80],
        'kecepatan_chat_dibalas': df[85]
    }

    pelayanan_poin_penalti_metric = {
        'persentase_chat_dibalas': df[81],
        'kecepatan_chat_dibalas': df[86]
    }

    pelayanan_aksi_metric = {
        'persentase_chat_dibalas': df[82],
        'kecepatan_chat_dibalas': df[87]
    }

    kepuasan_toko_saya_metric = {
        'keseluruhan_penilaian': df[94]
    }

    kepuasan_target_metric = {
        'keseluruhan_penilaian': df[95]
    }

    kepuasan_periode_sebelumnya_metric = {
        'keseluruhan_penilaian': df[96]
    }

    kepuasan_poin_penalti_metric = {
        'keseluruhan_penilaian': df[97]
    }

    kepuasan_aksi_metric = {
        'keseluruhan_penilaian': df[98]
    }


    df_metric = pd.DataFrame(
        list(zip(
            [produk_toko_saya_metric, pesanan_toko_saya_metric, pelayanan_toko_saya_metric, kepuasan_toko_saya_metric], 
            [produk_target_metric, pesanan_target_metric, pelayanan_target_metric, kepuasan_target_metric], 
            [produk_periode_sebelumnya_metric, pesanan_periode_sebelumnya_metric, pelayanan_periode_sebelumnya_metric, kepuasan_periode_sebelumnya_metric], 
            [produk_poin_penalti_metric, pesanan_poin_penalti_metric, pelayanan_poin_penalti_metric, kepuasan_poin_penalti_metric], 
            [produk_aksi_metric, pesanan_aksi_metric, pelayanan_aksi_metric, kepuasan_aksi_metric]
            )), 
            columns=['toko_saya', 'target', 'periode_sebelumnya', 'poin_penalti', 'aksi'])

    df_metric['official_store'] = official_store_name
    df_metric['upload_tstamp'] = datetime.now()

    table = 'sirclo-prod.bronze_seller_center_commerce.shopee_account_health'

    client = bigquery.Client()

    job = client.load_table_from_dataframe(df_metric, table,
                        job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        autodetect=True
                    )
    )

    job.result()

def main(driver, official_store_name):
    sys.stdout.write("Start Account Health")
    df = get_health_data(driver)
    clean_df(df, official_store_name)
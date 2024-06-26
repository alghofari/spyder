# --- Import all the packages --- 
import time
import traceback
import pandas as pd
import numpy as np

from helpers.bigquery_helper import df_to_bq, delete_table
from helpers.cloud_storage_helper import enhanced_metadata_blob_gcs
from transform.tokopedia.seller_center.load_query_processing import merge_query

# --- Function for extract data from downloaded file ---
def extract_data_overview_into_dataframe(prefix_name, official_store, sheet, main_table, table_id, key_column, order_by_column, bq_project, dataset, dataset_append, gcs_logger):
    result_metadata_file = enhanced_metadata_blob_gcs(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    for full_filename, metadata in result_metadata_file.items():
        excel_filename  = metadata['excel_filename']
        upload_tstamp   = metadata['upload_tstamp']
        target_filepath = metadata['target_filepath']
        print(f"The Upload tstamp that will be process is : {upload_tstamp}")

        if excel_filename != "":
            try:
                # Read dataframe
                df_detail_overview = pd.read_excel(target_filepath, sheet_name=sheet)

                # Clean dataframe
                df_detail_overview['Waktu'] = df_detail_overview['Waktu'].astype("str").apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True, format="%d/%m/%Y")).dt.date.replace({pd.NaT: None})
                df_detail_overview['Potensi penjualan'] = df_detail_overview['Potensi penjualan'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Potensi penjualan dari Bebas Ongkir'] = df_detail_overview['Potensi penjualan dari Bebas Ongkir'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Potensi penjualan dari Bebas Ongkir Dilayani Tokopedia'] = df_detail_overview['Potensi penjualan dari Bebas Ongkir Dilayani Tokopedia'].astype("str").replace("", np.nan, regex=True).astype("float")

                df_detail_overview['Detail pengeluaran operasional: Biaya Layanan'] = df_detail_overview['Detail pengeluaran operasional: Biaya Layanan'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Detail pengeluaran operasional: Biaya Jasa Transaksi'] = df_detail_overview['Detail pengeluaran operasional: Biaya Jasa Transaksi'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Detail pengeluaran promosi: Bebas Ongkir'] = df_detail_overview['Detail pengeluaran promosi: Bebas Ongkir'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Detail pengeluaran promosi: Bebas Ongkir Dilayani Tokopedia'] = df_detail_overview['Detail pengeluaran promosi: Bebas Ongkir Dilayani Tokopedia'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Detail pengeluaran promosi: TopAds'] = df_detail_overview['Detail pengeluaran promosi: TopAds'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Detail pengeluaran promosi: Broadcast Chat'] = df_detail_overview['Detail pengeluaran promosi: Broadcast Chat'].astype("str").replace("", np.nan, regex=True).astype("float")
                df_detail_overview['Detail pengeluaran promosi: Kupon Toko'] = df_detail_overview['Detail pengeluaran promosi: Kupon Toko'].astype("str").replace("", np.nan, regex=True).astype("float")

                df_detail_overview['Produk dilihat'] = df_detail_overview['Produk dilihat'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")
                df_detail_overview['Pesanan'] = df_detail_overview['Pesanan'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")
                df_detail_overview['Pesanan baru dari Bebas Ongkir'] = df_detail_overview['Pesanan baru dari Bebas Ongkir'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")
                df_detail_overview['Pesanan baru dari Bebas Ongkir Dilayani Tokopedia'] = df_detail_overview['Pesanan baru dari Bebas Ongkir Dilayani Tokopedia'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")

                df_detail_overview['Pembatalan: Dibatalkan otomatis'] = df_detail_overview['Pembatalan: Dibatalkan otomatis'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")
                df_detail_overview['Pembatalan: Ditolak seller'] = df_detail_overview['Pembatalan: Ditolak seller'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")
                df_detail_overview['Pembatalan: Permintaan pembeli'] = df_detail_overview['Pembatalan: Permintaan pembeli'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")

                # Adjust data based on query condition
                df_detail_overview['Potensi penjualan bukan dari Bebas Ongkir'] = df_detail_overview['Potensi penjualan'] - df_detail_overview['Potensi penjualan dari Bebas Ongkir'] - df_detail_overview['Potensi penjualan dari Bebas Ongkir Dilayani Tokopedia']
                df_detail_overview['Potensi penjualan bukan dari Bebas Ongkir'] = df_detail_overview['Potensi penjualan bukan dari Bebas Ongkir'].astype("str").replace("", np.nan, regex=True).astype("float")

                df_detail_overview['Pesanan baru bukan dari Bebas Ongkir'] = df_detail_overview['Pesanan'] - df_detail_overview['Pesanan baru dari Bebas Ongkir'] - df_detail_overview['Pesanan baru dari Bebas Ongkir Dilayani Tokopedia']
                df_detail_overview['Pesanan baru bukan dari Bebas Ongkir'] = df_detail_overview['Pesanan baru bukan dari Bebas Ongkir'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")

                df_detail_overview['Pesanan batal'] = df_detail_overview['Pembatalan: Permintaan pembeli'] + df_detail_overview['Pembatalan: Ditolak seller'] + df_detail_overview['Pembatalan: Dibatalkan otomatis']
                df_detail_overview['Pesanan batal'] = df_detail_overview['Pesanan batal'].astype("str").replace("", np.nan, regex=True).replace(r"(\.0)", "", regex=True).astype("Int64")

                df_detail_overview['Estimasi pengeluaran'] = df_detail_overview['Detail pengeluaran promosi: Bebas Ongkir'] + df_detail_overview['Detail pengeluaran operasional: Biaya Layanan'] + df_detail_overview['Detail pengeluaran promosi: TopAds'] + df_detail_overview['Detail pengeluaran promosi: Broadcast Chat'] + df_detail_overview['Detail pengeluaran promosi: Kupon Toko']
                df_detail_overview['Estimasi pengeluaran'] = df_detail_overview['Estimasi pengeluaran'].astype("str").replace("", np.nan, regex=True).astype("float")

                # Add column identifier
                df_detail_overview['official_store'] = official_store
                df_detail_overview['upload_tstamp'] = upload_tstamp

                base_config_path = "./transform/tokopedia/seller_center/statistic/overview/resources/"
                schema_path_detail_overview = base_config_path + f"schema_detail_overview.json"

                # Change column name
                schema_df_detail_overview  = pd.read_json(schema_path_detail_overview)
                column_name_detail_trafic  = schema_df_detail_overview['name'].to_list()
                df_detail_overview.columns = column_name_detail_trafic

                print("Dataframe detail_overview has been created")
                time.sleep(5)

                # Load dataframe into bigquery temp table
                temp_table_path_detail = f"{bq_project}.{dataset_append}.{table_id}"

                df_to_bq(
                    df                = df_detail_overview,
                    project_table_bq  = temp_table_path_detail,
                    file_path         = schema_path_detail_overview,
                    job_configuration = "statistic"
                )
                time.sleep(15)
                print(f"Dataframe detail_overview successfully created on temp table")

                # Merge temp table into main table
                merge_query(
                    schema_path     = schema_path_detail_overview,
                    key_column      = key_column,
                    order_by_column = order_by_column,
                    bq_project      = bq_project,
                    dataset         = dataset,
                    dataset_append  = dataset_append,
                    main_table      = main_table,
                    table_temp      = table_id,
                    gcs_logger      = gcs_logger
                )
                time.sleep(5)

                # Delete temp table
                print("Process to delete temp table")
                delete_table(temp_table_path_detail)
                time.sleep(5)
                gcs_logger.log(f"End of the pipeline for transform statistic wawasan toko data")
            
            except:
                gcs_logger.log(f"The data {excel_filename} is corrupted, please retry extract the data")
                traceback_str = traceback.format_exc()
                gcs_logger.error(traceback_str)
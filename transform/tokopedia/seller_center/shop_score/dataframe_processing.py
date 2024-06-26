import json
import pytz
import pandas as pd
from datetime import datetime

from helpers.bigquery_helper import delete_table, df_to_bq
from helpers.cloud_storage_helper import enhanced_gcs_json
from transform.tokopedia.seller_center.load_query_processing import append_query

def replace_month_abbreviation(date_string):
    return date_string.replace('Mei', 'May').replace('Agu', 'Aug').replace('Okt', 'Oct').replace('Des', 'Dec')

def extract_data_shop_score_into_dataframe(prefix_name, official_store, main_table, table_id, key_column, order_by_column, bq_project, dataset, dataset_append, gcs_logger):
    # Define base schema path
    base_config_path = "./transform/tokopedia/seller_center/shop_score/resources/"
    schema_path = base_config_path + 'schema.json'

    # Load the schema JSON data
    with open(schema_path, 'r') as f:
        schema_data = json.load(f)

    result_json_gcs = enhanced_gcs_json(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    # Extracting the relevant data from the JSON
    shop_score_details = result_json_gcs['data']['shopScoreLevel']['result']['shopScoreDetail']

    # Getting header from schema
    headers = [item['name'] for item in schema_data]

    # Creating a dictionary to store the shopScoreDetail data
    score_details_dict = {}
    for item in shop_score_details:
        if item['identifier'] in headers:
            identifier = item['identifier']
            raw_value  = item['rawValue'] * 100 if identifier in ['finish_rate',
                                                                 'percent_good_rating',
                                                                 'chat_and_discussion_reply_rate'] else item['rawValue']
            
            if item['identifier'] in ['chat_and_discussion_reply_speed', 'penalty']:
                raw_value = int(item['rawValue'])

            score_details_dict[identifier] = raw_value

        elif item['identifier'] == 'cnt_unique_buyer':
            identifier = item['identifier']
            raw_value  = item['rawValue']

            score_details_dict['count_unique_buyer'] = int(raw_value)

        elif item['identifier'] == 'ct_active_days':
            identifier = item['identifier']
            raw_value  = item['rawValue']

            score_details_dict['count_active_days'] = int(raw_value)

    score_details_dict['shop_score']     = int(result_json_gcs['data']['shopScoreLevel']['result']['shopScore'])
    score_details_dict['shop_level']     = int(result_json_gcs['data']['shopLevel']['result']['shopLevel'])
    score_details_dict['official_store'] = official_store
    score_details_dict['upload_tstamp']  = datetime.now().astimezone(pytz.timezone('Asia/Jakarta'))

    # Get shop_score period and next update
    shop_score_next_update = replace_month_abbreviation(result_json_gcs['data']['shopScoreLevel']['result']['nextUpdate'])
    shop_score_period      = replace_month_abbreviation(result_json_gcs['data']['shopScoreLevel']['result']['period'])
    shop_score_start_period, shop_score_end_period = shop_score_period.split(" - ")

    score_details_dict['shop_score_start_date']  = datetime.strptime(shop_score_start_period, "%d %b %Y").strftime("%Y-%m-%d")
    score_details_dict['shop_score_end_date']    = datetime.strptime(shop_score_end_period, "%d %b %Y").strftime("%Y-%m-%d")
    score_details_dict['shop_score_next_update'] = datetime.strptime(shop_score_next_update, "%d %b %Y").strftime("%Y-%m-%d")

    # Get shop_level period and next update
    shop_level_next_update = replace_month_abbreviation(result_json_gcs['data']['shopLevel']['result']['nextUpdate'])
    shop_level_period      = replace_month_abbreviation(result_json_gcs['data']['shopLevel']['result']['period'])
    shop_level_start_period, shop_level_end_period = shop_level_period.split(" - ")

    score_details_dict['shop_level_start_date']  = datetime.strptime(shop_level_start_period, "%d %b %Y").strftime("%Y-%m-%d")
    score_details_dict['shop_level_end_date']    = datetime.strptime(shop_level_end_period, "%d %b %Y").strftime("%Y-%m-%d")
    score_details_dict['shop_level_next_update'] = datetime.strptime(shop_level_next_update, "%d %b %Y").strftime("%Y-%m-%d")

    # Creating the DataFrame from the extracted data
    df = pd.DataFrame([score_details_dict])

    # Convert datetime type in df
    columns_to_convert = ['shop_score_start_date', 'shop_score_end_date', 'shop_score_next_update',
                          'shop_level_start_date', 'shop_level_end_date', 'shop_level_next_update']
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_datetime)

    # Load dataframe into bigquery temp table
    temp_table_path_data = f"{bq_project}.{dataset_append}.{table_id}"
    df_to_bq(df=df, project_table_bq=temp_table_path_data,
             file_path=schema_path, job_configuration="shop_score")

    # Merge temp table into main table
    append_query(schema_path, key_column, order_by_column,
                bq_project, dataset, dataset_append,
                main_table, table_id, gcs_logger)

    # Delete temp table
    print("Process to delete temp table")
    delete_table(temp_table_path_data)
    gcs_logger.log(f"End of the pipeline for transform shop_score data")

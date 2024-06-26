import pytz

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from helpers.bigquery_helper import delete_table, df_to_bq
from helpers.cloud_storage_helper import enhanced_gcs_json

from transform.tokopedia.seller_center.load_query_processing import merge_query

def remove_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()

def get_values_by_data_key(data, data_key):
    results = []
    for item in data:
        if "data" in item and "fetchSearchTableWidgetData" in item["data"]:
            widget_data = item["data"]["fetchSearchTableWidgetData"]["data"]
            for widget_item in widget_data:
                if "dataKey" in widget_item and widget_item["dataKey"] == data_key:
                    rows = widget_item["data"]["rows"]
                    for row in rows:
                        columns = row.get("columns", [])
                        values = [remove_html_tags(
                            column["value"]) for column in columns]
                        results.append(values)
    return results

def extract_data_operational_into_dataframe(prefix_name, official_store, main_table, table_id, key_column, order_by_column, bq_project, dataset, dataset_append, gcs_logger):
    # Define base schema path
    base_config_path = "./transform/tokopedia/seller_center/operational/resources/"
    schema_path = base_config_path + 'schema.json'

    result_json_gcs = enhanced_gcs_json(
        bucket_name = "sirclo-data-marketplace",
        prefix_name = prefix_name
    )

    # Get the current date
    current_date = datetime.today().astimezone(pytz.timezone('Asia/Jakarta'))

    # Calculate the start_date and end_date for the previous seven days
    start_date = (current_date - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # Get values for "communicationRate"
    communication_rate_values = get_values_by_data_key(
        result_json_gcs, "communicationRate")

    # Get values for "communicationSpeed"
    communication_speed_values = get_values_by_data_key(
        result_json_gcs, "communicationSpeed")

    operational_dict = {}
    operational_dict['chat_replied_rate'] = float(communication_rate_values[0][1].replace('%', '').replace(',', '.')) if communication_rate_values[0][1] else np.nan
    operational_dict['chat_replied_rate_target'] = communication_rate_values[0][2]
    operational_dict['chat_replied_rate_description'] = communication_rate_values[0][3]
    operational_dict['discussion_replied_rate'] = float(communication_rate_values[1][1].replace('%', '').replace(',', '.')) if communication_rate_values[1][1] else np.nan
    operational_dict['discussion_replied_rate_target'] = communication_rate_values[1][2]
    operational_dict['discussion_replied_rate_description'] = communication_rate_values[1][3]
    operational_dict['chat_reply_speed'] = int(communication_speed_values[0][1].replace(' menit', '')) if ' menit' in communication_speed_values[0][1] else np.nan
    operational_dict['chat_reply_speed_target'] = communication_speed_values[0][2]
    operational_dict['chat_reply_speed_description'] = communication_speed_values[0][3]
    operational_dict['discussion_reply_speed'] = int(communication_speed_values[1][1].replace(' menit', '')) if ' menit' in communication_speed_values[1][1] else np.nan
    operational_dict['discussion_reply_speed_target'] = communication_speed_values[1][2]
    operational_dict['discussion_reply_speed_description'] = communication_speed_values[1][3]

    operational_dict['official_store'] = official_store
    operational_dict['upload_tstamp'] = datetime.now().astimezone(pytz.timezone('Asia/Jakarta'))
    operational_dict['start_date'] = start_date
    operational_dict['end_date'] = end_date

    # Creating the DataFrame from the extracted data
    df = pd.DataFrame([operational_dict])
    print("Dataframe operational has been created")
    
    columns_to_convert = ['start_date', 'end_date']
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_datetime)

    # Load dataframe into bigquery temp table
    temp_table_path_data = f"{bq_project}.{dataset_append}.{table_id}"
    df_to_bq(df=df, project_table_bq=temp_table_path_data,
            file_path=schema_path, job_configuration="operational")

    # Merge temp table into main table
    merge_query(schema_path, key_column, order_by_column,
                bq_project, dataset, dataset_append,
                main_table, table_id, gcs_logger)

    # Delete temp table
    print("Process to delete temp table")
    delete_table(temp_table_path_data)
    gcs_logger.log(f"End of the pipeline for transform operational data")
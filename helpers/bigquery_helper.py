import json

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

client = bigquery.Client()

def df_to_bq(df, project_table_bq, file_path, partition_field="run_date",job_configuration=""):
    # Construct a BigQuery client object.
    table_id = project_table_bq  # "your-project.your_dataset.your_table_name"

    # Define the job configuration
    if job_configuration in ["shop_info", "statistic", "shop_score", "operational"]:
        schema_file = open(file_path)
        schema_json = json.load(schema_file)

        job_config = bigquery.LoadJobConfig(
            schema=schema_json,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            time_partitioning=None
        )
        
    elif job_configuration == "shopee_mall_review":
        job_config = bigquery.LoadJobConfig(
                autodetect=True,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                time_partitioning=None
                )
        
    elif job_configuration == "seller_center" or job_configuration == "ads":
        schema_file = open(file_path)
        schema_json = json.load(schema_file)

        job_config = bigquery.LoadJobConfig(
            schema=schema_json,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            time_partitioning=None
        )

    else:
        if job_configuration == "search_item" or job_configuration == "mall_item":
            schema_config = client.schema_from_json(file_path)
        else:
            f = open(file_path, "r")
            schema_json = json.loads(f.read())
            schema_config = []
            for item in schema_json:
                schema_config.append(bigquery.SchemaField(item, schema_json[item]))

        job_config = bigquery.LoadJobConfig(
            schema=schema_config,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        )
	

    # load job
    load_job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    return


def bq_to_df(sql: str):
    df = client.query(sql).to_dataframe()
    return df


def get_table_list(bucket, dataset):
    dataset_id = f"{bucket}.{dataset}"
    dataset = client.get_dataset(dataset_id)
    return list(client.list_tables(dataset))


def check_table_exist(table_id):
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False

def delete_table(table_id):
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"Table {table_id} has been deleted")
    except NotFound:
        print(f"Table {table_id} not exist")
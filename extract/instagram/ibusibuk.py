import os
import time

import pandas as pd

from extract.instagram.utils.instagram_scraper import InstagramScraper
from utils.file_reader import FileReader
from utils.google.bigquery_reader import BigQueryReader
from utils.google.bigquery_writer import BigQueryWriter
from utils.logger import Logger

logger = Logger().get_logger()
bigquery_reader = BigQueryReader(project='sirclo-prod')
file_reader = FileReader()


def profile_data(ig_profile: InstagramScraper, instagram_user: str):
    try:
        profiles_data = []
        try:
            profile_info = ig_profile.get_profile_info(instagram_user)
            profiles_data.append(profile_info)
        except Exception as e:
            logger.error(f"An error occurred while processing user {instagram_user}: {e}")

        # Create a DataFrame from the collected profile data
        df = pd.DataFrame(profiles_data)

        # Schema file path and BigQuery table ID
        schema_path = os.getcwd() + '/extract/instagram/schema/ibusibuk/profile.json'
        table_id = 'sirclo-prod.bronze_instagram.profile_ibusibuk'

        # Initialize writer with custom dispositions and partitioning
        writer = BigQueryWriter(
            schema_path, table_id,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            time_partitioning_type="DAY"
        )
        writer.write_to_table(df)

    except Exception as e:
        logger.error(f"An error occurred during the main process: {e}")


def post_data(ig_profile: InstagramScraper, instagram_user: str):
    try:
        posts_data = []
        try:
            post_info = ig_profile.get_post_info(instagram_user)
            posts_data.append(post_info)
        except Exception as e:
            logger.error(f"An error occurred while processing user {instagram_user}: {e}")

        # Flatten the list of lists
        posts_data = [item for sublist in posts_data for item in sublist]

        # Create a DataFrame from the collected post data
        df = pd.DataFrame(posts_data)

        # Schema file path and BigQuery table ID
        schema_path = os.getcwd() + '/extract/instagram/schema/ibusibuk/post.json'
        table_id = 'sirclo-prod.bronze_instagram.post_ibusibuk'

        # Initialize writer with custom dispositions and partitioning
        writer = BigQueryWriter(
            schema_path, table_id,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            time_partitioning_type="DAY"
        )
        writer.write_to_table(df)

    except Exception as e:
        logger.error(f"An error occurred during the main process: {e}")


def main(instagram_username: str, instagram_password: str):
    ig_profile = InstagramScraper(instagram_username, instagram_password)

    # login is required for private profile
    # ig_profile.login()

    ibusibuk_instagram_status_query = file_reader.read_file(os.getcwd() + '/extract/instagram/query/ibusibuk_instagram_status.sql')
    instagram_users = bigquery_reader.execute_query(ibusibuk_instagram_status_query)
    for row in instagram_users:
        profile_data(ig_profile, row['username'])
        time.sleep(1)
        post_data(ig_profile, row['username'])
        time.sleep(1)

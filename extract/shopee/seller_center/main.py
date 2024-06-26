import os
import traceback

from google.cloud import storage
from pyvirtualdisplay import Display

from extract.shopee.seller_center import login
from extract.shopee.seller_center.account_health import extract_account_health
from extract.shopee.seller_center.ads import extract_ads
from extract.shopee.seller_center.cancel_order import extract_cancel_order
from extract.shopee.seller_center.live_stream import extract_stream
from extract.shopee.seller_center.non_fulfilled import extract_non_fulfilled
from extract.shopee.seller_center.order import extract_order
from extract.shopee.seller_center.raw_fulfillment import extract_raw_fulfillment
from extract.shopee.seller_center.return_order import extract_return_order
from extract.shopee.seller_center.store_performance import extract_store_performance
from extract.shopee.seller_center.traffic import extract_traffic
from extract.shopee.seller_center.web_seller_affiliate import extract_web_seller_affiliate
from helpers.creds_helper import get_config
from helpers.creds_helper import get_creds
from helpers.webdriver_helper import set_webdriver
from transform.shopee.seller_center.ads import load_ads_to_bq
from transform.shopee.seller_center.cancel_order import load_cancel_order_to_bq
from transform.shopee.seller_center.live_stream import load_live_stream_to_bq
from transform.shopee.seller_center.live_stream_detail import load_live_stream_detail_to_bq
from transform.shopee.seller_center.non_fulfilled import load_non_fulfilled_to_bq
from transform.shopee.seller_center.raw_fulfillment import load_raw_fulfillment_to_bq
from transform.shopee.seller_center.return_order import load_return_order_to_bq
from transform.shopee.seller_center.store_performance import load_store_performance_to_bq
from transform.shopee.seller_center.traffic import load_traffic_to_bq
from transform.shopee.seller_center.web_seller_affiliate import load_web_seller_affiliate_to_bq


def shopee_config(os_key: str) -> tuple:
    """
    Extract config from google sheet and vaultwarden
    to get config and credentials for Shopee login
    Args:
        os_key (str): os_key as in this Shopee sheet https://docs.google.com/spreadsheets/d/1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg.
    Returns:
        official_store_name, vaultwarden_email, seller_center_pass, official_store_id (tuple): Credential and config for shopee login.
    """

    # Get config from sheet
    config = get_config(os_key, 'shopee')
    seller_center_email = config['seller_center_email']
    official_store_name = config['official_store_name']

    # Get email and password from bronze_vaultwarden
    creds = get_creds(seller_center_email, 'shopee')
    vaultwarden_email = creds[0]
    seller_center_pass = creds[1]
    official_store_id = creds[2]
    ned_email = creds[3]
    return official_store_name, vaultwarden_email, seller_center_pass, official_store_id, ned_email


def webdriver():
    """
    Configuration for webdriver
    Args:
        'None'
    Returns:
        driver (undetected_chromedriver.Chrome): Driver for login.
    """
    os.mkdir(f"{os.getcwd()}/download_folder") 
    preference = {
        "download.default_directory": "/app/download_folder",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }
    # additional_opts = ["--start-maximized", "--window-size=1920,1080"]
    additional_opts = ["--start-maximized", "--user-data-dir=/app/profile"]
    driver = set_webdriver(driver_type='webdriver', exp_opt_pref=preference, additional_opts=additional_opts)
    return driver


def execute(start_date: str, end_date: str, os_key: str) -> None:
    """
    Main function to execute all Shopee Seller Center
    Args:
        start_date (str): start date for data to be extracted.
        end_date (str): end date for data to be extracted.
        os_key (str): os_key as in this Shopee sheet https://docs.google.com/spreadsheets/d/1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg.
    Returns:
        None
    """
    display = Display(visible=False, size=(2560, 1440))
    display.start()

    driver = webdriver()
    config = shopee_config(os_key)
    storage_client = storage.Client(project='sirclo-data-prod')

    # Code below need to login(can't use cookies)
    try:
        login.main(driver, 'seller_center', config, storage_client)
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Extract and download traffic
    try:
        extract_traffic.main(driver, storage_client, config)
        load_traffic_to_bq.main(os_key)
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Extract and download store performance
    try:
        extract_store_performance.main(driver, storage_client, config[0])
        load_store_performance_to_bq.main(os_key)
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Extract and download account health
    try:
        extract_account_health.main(driver, config[0])
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Extract and download non fulfilled
    try:
        extract_non_fulfilled.main(driver, storage_client, config[0])
        load_non_fulfilled_to_bq.main(os_key)
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Extract and Download web seller affiliate
    try:
        extract_web_seller_affiliate.main(driver)
        load_web_seller_affiliate_to_bq.main(os_key)
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Extract and download live streaming
    try:
        extract_stream.main(driver, storage_client, config)
        load_live_stream_to_bq.main(os_key)
        load_live_stream_detail_to_bq.main(os_key)
    except Exception as e:
        print(e)
        traceback.print_exc()
        pass

    # Code below use cookies stored in GCS so it can be run without login

    extract_order.main(config[0], storage_client, os_key)

    extract_ads.main(start_date, end_date, config[0], storage_client)
    load_ads_to_bq.main(start_date, end_date, os_key)

    extract_return_order.main(start_date, end_date, config[0], storage_client)
    load_return_order_to_bq.main(start_date, end_date, os_key)

    extract_raw_fulfillment.main(start_date, end_date, config[0], storage_client)
    load_raw_fulfillment_to_bq.main(start_date, end_date, os_key)

    extract_cancel_order.main(start_date, end_date, config[0], storage_client)
    load_cancel_order_to_bq.main(start_date, end_date, os_key)

    display.stop()
    print(f'Done loading {os_key}')

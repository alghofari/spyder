import time
import re
import traceback

from pyvirtualdisplay import Display

from helpers.logger_helper import GCSLogger
from extract.tokopedia.seller_center.miscellaneous import driver_init
from extract.tokopedia.seller_center.login_process import login
from extract.tokopedia.seller_center.config import generate_tkpd_config

from extract.tokopedia.seller_center.ads import ads_by_api
from extract.tokopedia.seller_center.operational import operational_by_api
from extract.tokopedia.seller_center.sales import sales_by_api
from extract.tokopedia.seller_center.shop_score import shop_score_by_api
from extract.tokopedia.seller_center.statistic import statistic_by_api
from extract.tokopedia.seller_center.statistic import video_statistic

from transform.tokopedia.seller_center.ads import transform_ads_page
from transform.tokopedia.seller_center.operational import transform_operational_page
from transform.tokopedia.seller_center.sales import transform_sales_page
from transform.tokopedia.seller_center.shop_score import transform_shop_score_page
from transform.tokopedia.seller_center.statistic import transform_statistic_page

seller_center_url = "https://seller.tokopedia.com/landing"


def execute(os_key, page_part_stats, start_date, end_date):
    # define config
    config = generate_tkpd_config(os_key)
    vaultwarden_email = config[0]
    seller_center_pass = config[1]
    official_store_id = config[2]
    official_store_name = config[3]

    display = Display(size=(1920, 1080))
    display.start()
    webdriver = driver_init()

    # Initialize logging
    gcs_logger = GCSLogger(bucket_name="sirclo-data-marketplace",
                           log_prefix=f"assets/logs/Tokopedia/seller_center_page",
                           official_name=re.sub(r"[^A-Za-z]", "_", official_store_name.lower()))

    try:
        login(
            driver=webdriver,
            url=seller_center_url,
            mail_creds=vaultwarden_email,
            pass_creds=seller_center_pass,
            official_store_id=official_store_id,
            marketplace="tokopedia",
            gcs_logger=gcs_logger
        )

        try:
            sales_by_api.generate_order_data(webdriver, config, start_date, end_date, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            ads_by_api.generate_ads_data(webdriver, config, start_date, end_date, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            operational_by_api.generate_operational_data(webdriver, config, start_date, end_date, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            shop_score_by_api.generate_shop_score(webdriver, config, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            statistic_by_api.generate_statistic_data(webdriver, config, page_part_stats, start_date, end_date, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            video_statistic.main(webdriver, config, start_date, end_date, gcs_logger)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            transform_sales_page.main(config, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            transform_ads_page.main(config, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            transform_shop_score_page.main(config, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            transform_operational_page.main(config, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)

        try:
            transform_statistic_page.main(config, page_part_stats, gcs_logger)
            time.sleep(10)
        except Exception as e:
            traceback_str = traceback.format_exc()
            gcs_logger.error(traceback_str)
            gcs_logger.error(e)
    except Exception as e:
        traceback_str = traceback.format_exc()
        gcs_logger.error(traceback_str)
        gcs_logger.error(e)

    time.sleep(10)

    webdriver.quit()
    display.stop()

    gcs_logger.log(f"Finish all pipeline {official_store_name}")

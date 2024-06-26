import os

from extract.tiktok.seller_center import login
from extract.tiktok.seller_center.data_compass import live_analysis as extract_live_analysis
from extract.tiktok.seller_center.data_compass import video_analysis as extract_video_analysis
from transform.tiktok.seller_center.data_compass.live_analysis import live_analysis as transform_live_analysis
from transform.tiktok.seller_center.data_compass.video_analysis import video_analysis as transform_video_analysis
from utils.virtual_display_manager import VirtualDisplayManager
from utils.web_scraper import WebScraper


def execute(store_name, start_date, end_date):
    scraper = WebScraper()
    display_manager = VirtualDisplayManager()

    assets_path = os.getcwd() + "/assets"
    browser_path = f"{assets_path}/browser/geckodriver"
    download_path = f"{assets_path}/data/tiktok/{store_name}"
    os.makedirs(download_path, exist_ok=True)

    # initialize webdriver and display
    with display_manager.managed_display():
        scraper.start_browser(browser_path, download_path)
        login.main(scraper, store_name)
        extract_live_analysis.main(scraper, store_name)
        extract_video_analysis.main(scraper, store_name)
        scraper.stop_browser()

    transform_live_analysis.main(download_path, store_name)
    transform_video_analysis.main(download_path, store_name)

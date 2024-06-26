import time

from utils.logger import Logger


def download_data(scraper):
    # go to video detail page
    scraper.navigate_to_url("https://seller-id.tiktok.com/compass/video-analytics/video-details")
    time.sleep(15)

    # click date picker button
    scraper.find_element_by_locator("css selector", ".theme-arco-picker").click()
    scraper.find_element_by_locator("xpath", "/html/body/div[1]/section/section/div/main/div[2]/div[1]/div[1]/div[1]/div/div/div[2]").click()
    time.sleep(5)

    # click date filter button
    scraper.find_element_by_keyword(['28 hari', 'Last 28 days']).click()
    time.sleep(5)

    # click export button
    scraper.find_element_by_keyword(['Unduh', 'Export']).click()
    time.sleep(20)

    # click afiliasi tab
    scraper.find_element_by_keyword(['afiliasi', 'Affiliate']).click()
    time.sleep(5)

    # click export button
    scraper.find_element_by_keyword(['Unduh', 'Export']).click()
    time.sleep(45)


def main(scraper, store_name):
    logger = Logger().get_logger()

    try:
        download_data(scraper)
    except Exception as e:
        logger.error(f"Error downloading video analysis data from {store_name}: {e}")
        pass

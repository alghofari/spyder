from datetime import datetime, timedelta

from executor.click_executor import passing_click_element as pce
from helpers.data_extractor_helper import check_downloaded_data as cdd
from helpers.date_helper import date_selector
from helpers.time_helper import sleep_condition as sc


def date_selector_download(driver, wait, button_identifier, tags_identifier, days_interval, start_sleep, end_sleep):
    """
    This function is used to select specific date in download page in seller center menu

    Params:
    driver            (driver)      | Required  : WebDriver command that will drive the process of scrapping specific define tags
    wait              (condition)   | Required  : Combination of wait time on specific range time, to give spare time for perform the specific define tags
    button_identifier (str)         | Required  : Name of tags on specific button to be click on the pop up (Ex: "button")
    tags_identifier   (str)         | Required  : Name of the tags on specific button to be identify specifically (Ex: "." -> for search all pages, "text()" -> for specific)
    days_interval     (str)         | Required  : Days interval to get specific date in download data
    start_sleep       (int)         | Required  : Number to start the range interval the randomize wait time
    end_sleep         (int)         | Required  : Number to end the range interval the randomize wait time

    Example:
    date_selector_condition(driver, wait, button_identifier = 'p', tags_identifier = 'text()', days_interval = '1', start_sleep = 3, end_sleep = 7)
    """
    # Get the date selector
    text_identifier, start_date, end_date, interval_download_data = date_selector(days_interval)
    print("interval_download_data=" + interval_download_data)

    # Click the date selector based on specific date first decision
    # click_date_selector
    pce(driver, wait, button_identifier, tags_identifier, text_identifier, start_sleep, end_sleep)
    sc(2, 4)

    # Click download data
    # click_ask_report_to_be_downloaded
    pce(driver, wait, button_identifier="button", tags_identifier=".",
        text_identifier="Minta Laporan", start_sleep=4, end_sleep=9)
    print("Minta Laporan already been clicked")
    print("wait notif will be appear")
    sc(5, 10)

    check_download_appearence = driver.page_source
    if "Download Laporan Penjualan" in check_download_appearence:
        print(f"Download Laporan Penjualan still appear on the page")

        # get class for download specific laporan
        new_data_element = driver.find_element("xpath", f'//p[contains(text(), "{interval_download_data}")]')
        par_class_data_element = new_data_element.find_element("xpath", "./parent::*").get_attribute("class")
        par_class_data_element_txt = driver.find_element("class name", par_class_data_element).text
        print(par_class_data_element + "=" + par_class_data_element_txt)

        par_data_element = driver.find_element("xpath", f"//div[contains(@class, '{par_class_data_element}')]")
        gpar_class_data_element = par_data_element.find_element("xpath", "./parent::*").get_attribute("class")
        print(gpar_class_data_element)

        download_element = driver.find_element("xpath",
                                            f"//div[contains(@class, '{gpar_class_data_element}')]//span[contains(text(), 'Download')]")
        download_element.click()
        # close_menu
        pce(driver, wait, "button", "@aria-label", "Tutup tampilan modal", 2, 8)
        print(f"Next to Detection the Folder that has been downloaded")

    else:
        print(f"Next to Detection the Folder that has been downloaded")

    sc(10, 15)

    # Check download data condition
    print(f"Data that has to be check is: Tokopedia_Order_{start_date}_{end_date}.xslx")
    check_result = cdd()

    # Check if downloaded data already existed in the local
    if check_result:
        print(f"Data : Tokopedia_Order_{start_date}_{end_date}.xslx has been uploaded into local environment")
    else:
        print("Try the second trial")
        print("Start Download Process")
        pce(driver, wait, 'div', '@id', 'download', 3, 5)

        print("Go to 30 hari terakhir, because laporan gagal")
        pce(driver, wait, 'p', 'text()', '30 hari terakhir', 5, 9)

        sc(4, 6)

        pce(driver, wait, button_identifier="button", tags_identifier=".",
        text_identifier="Minta Laporan", start_sleep=4, end_sleep=9)
        print("Minta Laporan already been clicked")

        check_download_appearence = driver.page_source
        if "Download Laporan Penjualan" in check_download_appearence:
            # Check download data condition
            last_week = datetime.now() - timedelta(days=29)
            start_date_2nd_trial = datetime.strftime(last_week, '%Y%m%d')
            end_date_2nd_trial = datetime.strftime(datetime.now(), '%Y%m%d')

            # get class for download specific laporan
            new_data_element = driver.find_element("xpath", f'//p[contains(text(), "{interval_download_data}")]')
            par_class_data_element = new_data_element.find_element("xpath", "./parent::*").get_attribute("class")
            print(par_class_data_element)

            par_data_element = driver.find_element("xpath", f"//div[contains(@class, '{par_class_data_element}')]")
            gpar_class_data_element = par_data_element.find_element("xpath", "./parent::*").get_attribute("class")
            print(gpar_class_data_element)

            download_element = driver.find_element("xpath",
                                                f"//div[contains(@class, '{gpar_class_data_element}')]//span[contains(text(), 'Download')]")
            download_element.click()
            # close_menu
            pce(driver, wait, "button", "@aria-label", "Tutup tampilan modal", 2, 8)
            print(f"Next to Detection the Folder that has been downloaded")
        else:
            print(f"Next to Detection the Folder that has been downloaded")

        sc(10, 15)

        # Check download data condition
        print(f"Data that has to be check is: Tokopedia_Order_{start_date}_{end_date}.xslx")
        check_result = cdd()

        # Check if downloaded data already existed in the local
        if check_result:
            print(f"Data : Tokopedia_Order_{start_date}_{end_date}.xslx has been uploaded into local environment")
        else:
            print("You already on third trial, please check the script")
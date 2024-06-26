import os

from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

from executor.click_executor import passing_click_element as pce
from executor.click_executor import passing_pop_up as ppu
from executor.download_executor import date_selector_download as dsd
from executor.upload_executor import gcs_upload_bucket
from helpers.otp_helper import otp_code
from helpers.time_helper import sleep_condition
from helpers.webdriver_helper import set_webdriver


def seller_center_crawl(url: str, mail_creds: str, pass_creds: str, interval_scrapping: str,
                        official_store: str, official_store_id:str):
    disp = Display(size=(1920, 1080))
    disp.start()

    print("-------------------")
    print("Start Pipeline")
    
    # Set the current directory
    current_directory = os.getcwd()

    marketplace = "tokopedia"
    base_path = f"assets/excel/{marketplace}/sales"

    result_directory = f"{current_directory}/{base_path}"
    print(f"Directory Path for Download Data is in : {result_directory}")

    if official_store != '':
        if not os.path.exists(result_directory):
            os.makedirs(result_directory)

    preference = {
        "credentials_enable_service": False,
        "download.default_directory": result_directory,
        "download.prompt_for_download": False,
        "profile.password_manager_enabled": False
    }

    # Set the webdriver
    driver = set_webdriver(additional_opts=None, exp_opt_pref=preference, driver_type='webdriver')

    print("-------------------")
    print(f"Start Scrapping")
    # Get into the seller url
    driver.get(url=url)

    # Set the wait condition
    wait = WebDriverWait(driver, 10)

    print("-------------------")
    print("Start in Login Page")

    # Wait until username section clickable
    # wait_visibility_username
    wait.until(ec.visibility_of_element_located((By.XPATH, '//*[@id="email-phone"]')))

    # wait_username
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="email-phone"]')))
    sleep_condition(2, 9)

    # click_username
    driver.find_element("xpath", '//*[@id="email-phone"]').click()

    # Send email in username section
    # username
    driver.find_element("xpath", '//*[@id="email-phone"]').send_keys(mail_creds)
    print("Username filled")

    # Wait until selanjutnya appear green and clickable to perform selanjutnya and get into fill password section
    # wait_next
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="email-phone-submit"]')))
    sleep_condition(2, 10)

    # click_next
    driver.find_element("xpath", '//*[@id="email-phone-submit"]').click()

    # Wait untill password section appear and clickable
    # wait_visibility_password
    wait.until(ec.visibility_of_element_located((By.XPATH, '//*[@id="password-input"]')))
    sleep_condition(3, 9)

    # wait_password
    wait.until(ec.element_to_be_clickable((By.XPATH, '//*[@id="password-input"]')))

    # click_password
    driver.find_element("xpath", '//*[@id="password-input"]').click()
    sleep_condition(4, 11)

    # password
    driver.find_element("xpath", '//*[@id="password-input"]').send_keys(pass_creds)
    print("Password filled")

    # Wait untill login clickable to get into main menu
    sleep_condition(2, 6)
    # wait_log_in
    wait.until(
        ec.element_to_be_clickable((By.XPATH, '//*[@id="zeus-root"]/div/div[2]/section/div[2]/form/button'))).click()
    # click_log_in
    driver.find_element("xpath", '//*[@id="zeus-root"]/div/div[2]/section/div[2]/form/button').click()

    sleep_condition(15, 30)
    sms_ke_page = driver.page_source
    condition = ""

    if "SMS ke" in sms_ke_page:
        print("SMS ke is in the page")
        try:
            # Wait to send message
            # wait_sms
            pce(driver, wait, 'b', 'text()', 'SMS ke', 5, 6)

            # Wait untill get the otp viewer
            # sleep_condition(3, 16)
            print("-------------------")
            print("Start Pipeline for get the OTP Number")

            get_otp = otp_code(official_store_id, marketplace.capitalize())
            get_otp = str(get_otp)

            if get_otp != "Not Found":

                print(f"The OTP Number that will be filled is : {get_otp}")
                
                # sleep_condition(10, 30)
                print("send the otp code into tokopedia page file")

                # wait_visibility_otp
                wait.until(
                    ec.visibility_of_element_located((By.XPATH, '//*[@id="zeus-root"]/div/div[2]/section/div/div/input')))

                # otp_input
                driver.find_element("xpath", '//*[@id="zeus-root"]/div/div[2]/section/div/div/input').send_keys(
                    get_otp)

                print("End Pipeline for OTP Number, OTP has been filled")
                print("-------------------")
                # sleep_condition(15, 30)
                print("End in Login Page, Login Successfull")
            
            else:
                condition = "Otp Error"

        except Exception as e:
            print(e)
            print("End in Login Page, Login Successfull")

        sleep_condition(6, 20)
    else:
        print("SMS ke is not in the page")
        print("End in Login Page, Login Successfull")
        
    if condition != "Otp Error":
        # Click checkbox
        sleep_condition(30, 45)

        # admin_checkbox
        print("-------------------")
        print("Start in Home Menu, only remove pop up")
        pce(driver, wait, "label", "@name", "chk-agree-tnc", 2, 8)
        sleep_condition(3, 10)

        # Click Masuk Toko
        sleep_condition(5, 10)
        # get_in_toko
        pce(driver, wait, "span", "text()", "Masuk Toko", 5, 10)

        # Move url to all order seller center
        driver.get("https://seller.tokopedia.com/myshop_order?status=all_order")
        sleep_condition(30, 45)

        # Detect PIN Tokopedia pop up
        detect_pop_up = driver.page_source

        if "Tutup tampilan modal" in detect_pop_up:
            pce(driver, wait, "button", "@aria-label", "Tutup tampilan modal", 0, 3)
            sleep_condition(4, 7)
        else:
            print("There is not Pin Tokopedia Pop Up")
            sleep_condition(4, 7)

        # Detect Lewati Pop Up
        ppu(driver, wait, "Lewati", "button", 2, 7)
        sleep_condition(5, 10)

        # Detect Nanti saja Pop Up
        pce(driver, wait, "span", "text()", "Nanti Saja", 2, 3)
        sleep_condition(5, 10)

        # Select download button
        sleep_condition(10, 15)
        # click_download_button
        print("-------------------")
        print("Start Download Process")
        pce(driver, wait, 'div', '@id', 'download', 3, 5)

        # Check if the menu download already appear in the webpage
        calendar_menu_title = driver.page_source

        if "Download Laporan Penjualan" in calendar_menu_title:

            # Get the data from specific date
            sleep_condition(2, 7)

            # get_data
            dsd(driver, wait, button_identifier='p', tags_identifier='text()', days_interval=interval_scrapping,
                start_sleep=3, end_sleep=7)

            print("Data already downloaded, next to upload into GCS Bucket")

            # Upload data into GCS
            sleep_condition(20, 30)
            print("upload process will be process")

            # upload
            gcs_upload_bucket(official_store)
            print('Upload in scrapping prosess has been done, in 5 seconds, process will be cleared up')
            print("End Download Process")

            # Close the webdriver
            print("End in All Order Page, pipeline done.")
            print("-------------------")
            print("Close the webdriver")
            driver.close()

            # Quit the driver
            driver.quit()

        else:
            print("Download menu still not appear in the web page, check the condition")

            try:
                print("Start second trial for download data")
                # Passing to Lewati Notification Pop Up
                sleep_condition(5, 7)
                # perform_passed_pop_up
                ppu(driver, wait, "Lewati", "button", 2, 7)
                
                # Passing to Nanti Saja Notification Pop Up
                sleep_condition(5, 7)
                # perform_passed_pop_up
                pce(driver, wait, "span", "text()", "Nanti Saja", 2, 3)

                # click_download_button
                sleep_condition(15, 30)
                print("-------------------")
                print("Start Download Process")

                # Perform click download
                pce(driver, wait, 'div', '@id', 'download', 3, 5)

                if "Download Laporan Penjualan" in calendar_menu_title:

                    # Get the data from specific date
                    sleep_condition(2, 7)

                    # get_data
                    dsd(driver, wait, button_identifier='p', tags_identifier='text()', days_interval=interval_scrapping,
                        start_sleep=3, end_sleep=7)

                    print("Data already downloaded, next to upload into GCS Bucket")

                    # Upload data into GCS
                    sleep_condition(10, 30)
                    print("upload process will be process")

                    # upload
                    gcs_upload_bucket(official_store)
                    print('Upload in scrapping prosess has been done, in 5 seconds, process will be cleared up')
                    print("End Download Process")

                    # Close the webdriver
                    print("End in All Order Page, pipeline done.")
                    print("-------------------")
                    print("Close the webdriver")
                    driver.close()

                    # Quit the driver
                    driver.quit()

            except Exception as e:
                print(e)
                print("Still on the other section, check the problem")

                # Close the webdriver
                driver.close()

                # Quit the driver
                driver.quit()

    else:
        print("Otp Error, can't login, all process has been terminated")
        # Close the webdriver
        driver.close()

        # Quit the driver
        driver.quit()

    disp.stop()
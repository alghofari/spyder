import os
import time
from datetime import datetime

from PIL import Image

from extract.tiktok.seller_center.utils.captcha_solver import execute
from extract.tiktok.seller_center.utils.email_fetcher import EmailFetcher
from helpers.cloud_storage_helper import upload_blob_to_gcs
from helpers.otp_helper import otp_code as get_verification_code_from_otp_viewer
from utils.google.sheets_reader import GoogleSheetsReader
from utils.logger import Logger
from utils.vaultwarden_client import VaultwardenClient

logger = Logger().get_logger()


def download_captcha_image(scraper, url, filename):
    captcha_img = scraper.find_element_by_locator("css selector", url)
    captcha_img.screenshot(filename)

    image = Image.open(filename)
    image.thumbnail((340, 212))
    image.save(filename)


def click_at_coordinate(scraper, element, coordinate):
    element_size = element.size
    element_width = -(element_size['width'] / 2)
    element_height = -(element_size['height'] / 2)
    x_axis = element_width + coordinate[0]
    y_axis = element_height + coordinate[1]
    logger.info(f"Click coordinate on x, y axis: {x_axis}, {y_axis}")

    action_chains = scraper.get_action_chains()
    action_chains.move_to_element_with_offset(element, x_axis, y_axis).click().perform()
    action_chains.reset_actions()


def solve_captcha(scraper, captcha_filename):
    coordinate1, coordinate2 = execute(captcha_filename)
    logger.info(f"Coordinate 1, 2: {coordinate1}, {coordinate2}")

    today = datetime.now()
    current_timestamp = today.strftime("%Y%m%d%H%M%S")
    current_date = today.strftime("%Y-%m-%d")
    gcs_bucket_name = "sirclo-data-marketplace"
    gcs_base_path = "assets/captcha/tiktok"

    if coordinate1 != "Refresh the CAPTCHA!":
        # upload success solved captcha image
        gcs_captcha_filename = f"{gcs_base_path}/success/{current_date}/captcha_{current_timestamp}.png"
        upload_blob_to_gcs(gcs_bucket_name, captcha_filename, gcs_captcha_filename)

        captcha_box = scraper.find_element_by_locator("css selector", "#captcha-verify-image")
        time.sleep(5)
        click_at_coordinate(scraper, captcha_box, coordinate1)
        logger.info("Click object 1")
        time.sleep(5)
        click_at_coordinate(scraper, captcha_box, coordinate2)
        logger.info("Click object 2")
        return True
    else:
        # upload fail solved captcha image
        gcs_captcha_filename = f"{gcs_base_path}/fail/{current_date}/captcha_{current_timestamp}.png"
        upload_blob_to_gcs(gcs_bucket_name, captcha_filename, gcs_captcha_filename)

        scraper.find_element_by_locator("css selector", ".secsdk_captcha_refresh--text").click()
        return False


def captcha_loop(scraper):
    captcha_filename = "captcha.png"

    while True:
        time.sleep(5)
        download_captcha_image(scraper, "#captcha-verify-image", captcha_filename)
        captcha_solved = solve_captcha(scraper, captcha_filename)
        if captcha_solved:
            break


def login(scraper, username, password, execution_date, otp_account_name):
    # go to login page
    scraper.navigate_to_url("https://seller-id.tiktok.com/account/login")
    time.sleep(5)

    # click login with email
    scraper.find_element_by_locator("css selector", "#TikTok_Ads_SSO_Login_Email_Panel_Button").click()
    time.sleep(5)

    # input email address
    scraper.find_element_by_locator("css selector", "#TikTok_Ads_SSO_Login_Email_Input").send_keys(username)
    time.sleep(5)

    # input password
    scraper.find_element_by_locator("css selector", value="#TikTok_Ads_SSO_Login_Pwd_Input").send_keys(password)
    time.sleep(5)

    # click login button
    scraper.find_element_by_locator("css selector", "#TikTok_Ads_SSO_Login_Btn").click()
    time.sleep(5)

    # solve captcha
    captcha_loop(scraper)
    time.sleep(5)

    # click confirm button
    scraper.find_element_by_locator("css selector", ".verify-captcha-submit-button").click()
    time.sleep(10)

    # check form input otp code weather exist or not
    login_code_input = scraper.find_element_by_locator("css selector", "#TikTok_Ads_SSO_Login_Code_Input")
    if login_code_input.is_displayed():
        logger.info("Input text OTP displayed, continue to get OTP from email")
        time.sleep(120)

        # get otp verification code from email
        with EmailFetcher() as fetcher:
            verification_code = fetcher.fetch_verification_code(execution_date, username)

        if verification_code is None:
            verification_code = get_verification_code_from_otp_viewer(otp_account_name, "Tiktok")

        # input otp verification code
        if verification_code is not None:
            scraper.find_element_by_locator("css selector", "#TikTok_Ads_SSO_Login_Code_Input").send_keys(verification_code)
            time.sleep(5)
            # click login button
            scraper.find_element_by_locator("css selector", "#TikTok_Ads_SSO_Login_Code_Btn").click()
            time.sleep(10)
        else:
            scraper.stop_browser()
    else:
        logger.info("Input text OTP not visible")
        scraper.stop_browser()


def main(scraper, store_name):
    execution_date = datetime.today()
    sheets_reader = GoogleSheetsReader(spreadsheet_key='1SizN344HBT-QK2Fg9rUVQEMATvlWVndOWdVv0fWtedg',
                                       worksheet_name='tiktok')
    configs = sheets_reader.read_worksheet().get_all_records()
    filtered_config = [d for d in configs if d['store_name'] == store_name]

    for config in filtered_config:
        otp_account_name = config['otp_account_name']
        vault_id = config['vault_id']

        vault_client = VaultwardenClient(server=os.getenv('VAULT_SERVER'),
                                         email=os.getenv('VAULT_EMAIL'),
                                         password=os.getenv('VAULT_PASSWORD'),
                                         client_id=os.getenv('VAULT_CLIENT_ID'),
                                         client_secret=os.getenv('VAULT_CLIENT_SECRET'))
        store_credentials = vault_client.get_credentials_by_id(vault_id)
        username = store_credentials['username']
        password = store_credentials['password']

        try:
            login(scraper, username, password, execution_date, otp_account_name)
        except Exception as e:
            logger.error(f"Not able login from {store_name}: {e}")
            scraper.stop_browser()
            continue

from selenium.webdriver.chrome.webdriver import WebDriver

from extract.shopee.sub_account import login
from extract.shopee.sub_account.member import password_rotation
from helpers.creds_helper import get_config, get_creds
from helpers.webdriver_helper import set_webdriver


def shopee_config(os_key: str) -> tuple:
    # Get config from sheet
    config = get_config(os_key, 'shopee')
    seller_center_email = config['seller_center_email']
    main_account_email = config['main_account_email']
    main_account_password = config['main_account_password']
    main_account_otp_id = config['main_account_otp_id']
    official_store_name = config['official_store_name']

    # Get email and password from bronze_vaultwarden
    creds = get_creds(seller_center_email, 'shopee')
    vaultwarden_email = creds[0]
    seller_center_pass = creds[1]
    official_store_id = creds[2]
    ned_email = creds[3]
    return official_store_name, vaultwarden_email, seller_center_pass, official_store_id, ned_email, main_account_email, main_account_password, main_account_otp_id


def webdriver(user_data_path: str) -> WebDriver:
    additional_opts = ["--start-maximized", f"--user-data-dir={user_data_path}"]
    driver = set_webdriver(driver_type='webdriver', additional_opts=additional_opts)
    return driver


def execute(os_key: str, profile_path: str):
    driver = webdriver(profile_path)
    config = shopee_config(os_key)

    login.main(driver, config)
    password_rotation.main(driver, config)

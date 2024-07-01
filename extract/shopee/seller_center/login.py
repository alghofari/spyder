import os
import re
import sys
import json
import email
import imaplib
from datetime import datetime
from datetime import timedelta
from selenium.webdriver.common.by import By

from helpers.otp_helper import otp_code
from helpers.time_helper import sleep_condition

from extract.shopee.seller_center.helper.helper import click_action


def login_process(driver: any, mail_creds: str, pass_creds: str) -> None:
    """
    Login process for shopee seller center
    Args:
        driver (any): chromedriver.Driver used
        mail_creds (str): username for subaccount login
        pass_creds (str): password for subaccount login
    Returns:
        None
    """
    driver.get('https://seller.shopee.co.id/account/signin?next=%2F')
    sleep_condition(30, 45)
    # Click sub account option (Alternative for XPATH: "/html/body/div[1]/div/div[2]/div/div/div/div[2]/div/button")
    click_action(driver = driver, css_selector = "#main > div > div.uqT7Nz > div > div > div > div:nth-child(2) > div._tsLgN.YN9KVd.Rlj6l5 > button").click()
    sleep_condition(30, 45)
    
    # Fill email subaccount (Alternative for XPATH: "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[1]/div/div/div/input")
    click_action(
        driver = driver, 
        css_selector = ".username .shopee-input .shopee-input__inner.shopee-input__inner--large .shopee-input__input"
        ).send_keys(mail_creds)
    
    # Fill password subaccount (Alternative for XPATH: "/html/body/div/main/div/div[1]/div/div/div/div/div/div/div[2]/div[3]/div/div/input")
    click_action(
        driver = driver, 
        css_selector = ".password.form-item .shopee-input .shopee-input__inner.shopee-input__inner--large .shopee-input__input"
        ).send_keys(pass_creds)
    
    # Submit click (Alternative for XPATH: "/html/body/div/main/div/div[1]/div/div/div/div/div/div/button[2]")
    click_action(
        driver = driver,
        css_selector = ".shopee-button.login-btn.shopee-button--primary.shopee-button--large.shopee-button--block" 
        ).click()
    sleep_condition(30, 60)
    driver.save_screenshot(f'{os.getcwd()}/login_process.png')

def get_verification_code(execution_date: datetime, from_email: str, to_email: str) -> str:
    # IMAP settings
    imap_server = 'imap.gmail.com'
    email_address = 'xxx@wxxx.co'
    password = 'xxx'

    # Connect to the IMAP server
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(email_address, password)

    # Select the mailbox (inbox)
    mailbox = 'INBOX'
    mail.select(mailbox)

    # Define search criteria
    sender = from_email
    subject = "Your Email OTP Verification Code"
    since_date = execution_date.strftime("%d-%b-%Y")

    # Construct the search query
    search_query = f'(FROM "{sender}" TEXT "{subject}" SINCE {since_date} TO "{to_email}")'

    # Search for emails based on the criteria
    status, email_ids = mail.search(None, search_query)

    # Get the list of email IDs
    email_id_list = email_ids[0].split()

    # Fetch and print the emails
    email_id = email_id_list[-1]
    status, msg_data = mail.fetch(email_id, '(RFC822)')
    raw_email = msg_data[0][1].decode("utf-8")
    msg = email.message_from_string(raw_email)
    
    for part in msg.walk():
        if part.get_content_type() == 'text/html':
            html = re.sub('<[^>]+>', '', part.get_payload())
            otp_code = re.findall("\d+", html)[-21]

    mail.logout()

    return otp_code

def send_otp(driver: any, official_store_id: str, ned_email: str) -> None:
    """
    Send OTP for current official store (Currently VIA SMS)
    Args:
        driver (any): chromedriver.Driver used
        official_store_id (str): official store id for OTP code
        ned_email (str): used to find specific email
    Returns:
        None
    """
    try:
        otp = otp_code(official_store_id, 'Shopee')
    except Exception as e:
        print(e)
        print("Get OTP from email")
        
        if driver.find_element(By.CSS_SELECTOR, ".shopee-button.shopee-button--link.shopee-button--normal").text == "Kirim ke Email":
            click_action(driver = driver, css_selector = ".shopee-button.shopee-button--link.shopee-button--normal").click()
        else:
            pass
        
        sleep_condition(10,15)
        otp = get_verification_code(datetime.today() - timedelta(hours= 7), "Shopee", ned_email)
            

    # Fill OTP (Alternative for XPATH: "//input[@placeholder='Kode verifikasi (OTP)']")
    click_action(driver = driver, css_selector = ".shopee-input__input").send_keys(otp)
    sleep_condition(30, 45)

    # Submit OTP (Alternative for XPATH: "//button[@class='shopee-button login-btn shopee-button--primary shopee-button--large shopee-button--block']")
    click_action(driver = driver, css_selector = ".shopee-button.login-btn.shopee-button--primary.shopee-button--large.shopee-button--block").click()
    sleep_condition(30, 45)


def get_cookies(driver: any, storage_client: any, official_store_name: any) -> None:
    """
    Get cookies for current OS that will be needed in seller_center and ads
    Args:
        driver (any): chromedriver.Driver used
        storage_client (any): storage.Client for sirclo-data-prod
        official_store_name (str): official store name for GCS identify
    Returns:
        None
    """
    cookies = driver.get_cookies()
    
    # Upload cookies only and only if SPC_CDS in cookies  
    for cookie in cookies:
        if 'name' in cookie and cookie['name'] == "SPC_CDS":
            creds   = {
                "cookies" : cookies
            }
            json_object = json.dumps(creds, indent=4)

            client      = storage_client.get_bucket('sirclo-data-marketplace')
            blob        = client.blob(f'assets/excel/shopee/sales/{official_store_name}/credentials/cookies.json')
            with blob.open(mode='w') as f:
                f.write(json_object)
        else:
            continue


## TODO: Investigate why profile can't be used on selenium

# def upload_profile(directory_path: str, dest_bucket_name: str, dest_blob_name: str, storage_client: any):
#     rel_paths = glob.glob(directory_path + '/**', recursive=True)
#     bucket = storage_client.get_bucket(dest_bucket_name)
#     for local_file in rel_paths:
#         remote_path = f'{dest_blob_name}/{"/".join(local_file.split(os.sep)[1:])}'
#         if os.path.isfile(local_file):
#             blob = bucket.blob(remote_path)
#             blob.upload_from_filename(local_file)


def main(driver, type, config, storage_client):
    """
    Main function to access all shopee page
    Args:
        driver (any): chromedriver.Driver used
        config (tuple): official store credential
        storage_client (any): storage.Client for sirclo-data-prod
    Returns:
        None
    """
    official_store_name, vaultwarden_email, seller_center_pass, official_store_id, ned_email = config
    
    login_process(driver, vaultwarden_email, seller_center_pass)
    send_otp(driver, official_store_id, ned_email)
    get_cookies(driver, storage_client, official_store_name)
    # upload_profile("profile/", "sirclo-data-marketplace", f"assets/excel/shopee/profile/{official_store_name}/profile", storage_client)

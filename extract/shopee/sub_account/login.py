import time

from selenium.webdriver.chrome.webdriver import WebDriver

from extract.shopee.seller_center.helper.helper import click_action
from helpers.otp_helper import otp_code


def login_process(driver: WebDriver, main_account_email: str, main_account_password: str):
    # open login page
    driver.get("https://subaccount.shopee.com/login/")
    time.sleep(5)

    # input email
    click_action(
        driver,
        css_selector="body > div.app > div > div.page-login > div > div.login-panel.col-8 > div > div.login-card__form.login-form > form > div:nth-child(1) > div > div > div > div > input"
    ).send_keys(main_account_email)
    time.sleep(5)

    # input password
    click_action(
        driver,
        css_selector="body > div.app > div > div.page-login > div > div.login-panel.col-8 > div > div.login-card__form.login-form > form > div:nth-child(2) > div > div > div > div > input"
    ).send_keys(main_account_password)
    time.sleep(5)

    # slick sign in button
    click_action(
        driver,
        css_selector="body > div.app > div > div.page-login > div > div.login-panel.col-8 > div > div.login-card__form.login-form > div.login-form__button > button"
    ).click()
    time.sleep(120)


def otp_process(driver: WebDriver, main_account_otp_id: str):
    try:
        # get otp code from otp viewer
        # otp send through phone number
        otp = otp_code(main_account_otp_id, 'Shopee')

        # input otp
        click_action(
            driver,
            css_selector="body > div.app > div > div.page-login > div > div.login-panel.col-8 > div > div > div > div > div.simple-otp__input > div > span:nth-child(1) > div > div > input"
        ).send_keys(otp)
        time.sleep(5)
    except ValueError:
        # click kirim email button
        click_action(
            driver,
            css_selector="body > div.app > div > div.page-login > div > div.login-panel.col-8 > div > div > div > div > div.simple-otp__addition.justify-center > div.simple-otp__addition__change-type > button"
        ).click()
        time.sleep(120)

        # get otp code from otp viewer
        # otp send through email
        otp = otp_code(main_account_otp_id, 'Shopee')

        # input otp
        click_action(
            driver,
            css_selector="body > div.app > div > div.page-login > div > div.login-panel.col-8 > div > div > div > div > div.simple-otp__input > div > span:nth-child(1) > div > div > input"
        ).send_keys(otp)
        time.sleep(5)

        # click verifikasi button
        click_action(
            driver,
            css_selector="body > div > div > div.page-login > div > div.login-panel.col-8 > div > div > div > div > div.simple-otp__button > button"
        ).click()
        time.sleep(5)


def main(driver: WebDriver, config: tuple):
    official_store_name, vaultwarden_email, seller_center_pass, official_store_id, ned_email, main_account_email, main_account_password, main_account_otp_id = config

    login_process(driver, main_account_email, main_account_password)
    otp_process(driver, main_account_otp_id)
